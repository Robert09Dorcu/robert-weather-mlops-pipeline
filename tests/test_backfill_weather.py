import json
import sys
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest
import requests
from botocore.exceptions import ClientError, NoCredentialsError

from ingestion import backfill_weather as bf
from ingestion.ingest_weather import ARCHIVE_API_URL, HOURLY_VARIABLES

CONFIG = {
    "aws_access_key_id": "AKIAFAKE",
    "aws_secret_access_key": "secret",
    "aws_region": "eu-central-1",
    "s3_bucket_name": "my-bucket",
    "latitude": "44.43",
    "longitude": "26.10",
}


def client_error(code, op="HeadObject"):
    return ClientError({"Error": {"Code": code, "Message": "msg"}}, op)


class TestDaterange:
    def test_inclusive_range(self):
        days = list(bf.daterange(date(2024, 1, 30), date(2024, 2, 2)))
        assert days == [date(2024, 1, 30), date(2024, 1, 31), date(2024, 2, 1), date(2024, 2, 2)]

    def test_single_day(self):
        assert list(bf.daterange(date(2024, 1, 1), date(2024, 1, 1))) == [date(2024, 1, 1)]

    def test_start_after_end_is_empty(self):
        assert list(bf.daterange(date(2024, 1, 2), date(2024, 1, 1))) == []

    def test_leap_day_included(self):
        assert date(2024, 2, 29) in list(bf.daterange(date(2024, 2, 28), date(2024, 3, 1)))


class TestBuildS3Key:
    def test_zero_padded_partitions(self):
        assert bf.build_s3_key(date(2024, 3, 5)) == (
            "bronze/weather/year=2024/month=03/day=05/weather_2024-03-05.json"
        )

    def test_matches_single_day_ingest_key_layout(self):
        # Backfill and daily ingestion must land in the same S3 layout.
        from ingestion import ingest_weather as iw

        client = MagicMock()
        with patch.object(iw.boto3.session, "Session") as session_cls:
            session_cls.return_value.client.return_value = client
            iw.upload_to_s3({}, date(2024, 12, 31), CONFIG)
        assert client.put_object.call_args.kwargs["Key"] == bf.build_s3_key(date(2024, 12, 31))


class TestFetchWeatherRange:
    def test_valid_response(self, sample_api_response):
        response = MagicMock()
        response.json.return_value = sample_api_response
        with patch.object(bf.requests, "get", return_value=response) as mock_get:
            result = bf.fetch_weather_range(date(2024, 1, 1), date(2024, 1, 2), "44.43", "26.10")

        assert result == sample_api_response
        args, kwargs = mock_get.call_args
        assert args[0] == ARCHIVE_API_URL
        assert kwargs["params"]["start_date"] == "2024-01-01"
        assert kwargs["params"]["end_date"] == "2024-01-02"
        assert kwargs["params"]["hourly"] == ",".join(HOURLY_VARIABLES)
        assert kwargs["timeout"] == 120

    @pytest.mark.parametrize(
        "error", [requests.ConnectionError(), requests.Timeout(), requests.HTTPError()]
    )
    def test_request_errors_exit(self, error):
        with patch.object(bf.requests, "get", side_effect=error):
            with pytest.raises(SystemExit) as exc:
                bf.fetch_weather_range(date(2024, 1, 1), date(2024, 1, 2), "1", "2")
        assert exc.value.code == 1

    def test_http_status_error_exits(self):
        response = MagicMock()
        response.raise_for_status.side_effect = requests.HTTPError("500")
        with patch.object(bf.requests, "get", return_value=response):
            with pytest.raises(SystemExit):
                bf.fetch_weather_range(date(2024, 1, 1), date(2024, 1, 2), "1", "2")


    def test_invalid_json_body_exits(self):
        response = MagicMock()
        response.json.side_effect = ValueError("not json")
        with patch.object(bf.requests, "get", return_value=response):
            with pytest.raises(SystemExit) as exc:
                bf.fetch_weather_range(date(2024, 1, 1), date(2024, 1, 2), "1", "2")
        assert exc.value.code == 1


class TestGroupHourlyByDay:
    def test_groups_indices_by_date_prefix(self, sample_api_response):
        assert bf.group_hourly_by_day(sample_api_response) == {
            "2024-01-01": [0, 1],
            "2024-01-02": [2],
        }

    @pytest.mark.parametrize("data", [{}, {"hourly": {}}, {"hourly": {"time": []}}])
    def test_empty_or_missing_data_gives_empty_dict(self, data):
        assert bf.group_hourly_by_day(data) == {}


class TestBuildDayPayload:
    def test_slices_every_hourly_field_and_keeps_metadata(self, sample_api_response):
        payload = bf.build_day_payload(sample_api_response, [0, 1])

        assert payload["hourly"] == {
            "time": ["2024-01-01T22:00", "2024-01-01T23:00"],
            "temperature_2m": [1.0, 0.5],
            "precipitation": [0.0, 0.1],
        }
        assert payload["latitude"] == 44.43
        assert payload["timezone"] == "GMT"
        assert payload["hourly_units"] == {"temperature_2m": "°C"}

    def test_does_not_mutate_input(self, sample_api_response):
        original = json.loads(json.dumps(sample_api_response))
        bf.build_day_payload(sample_api_response, [2])
        assert sample_api_response == original

    def test_missing_hourly_gives_empty_hourly(self):
        assert bf.build_day_payload({"latitude": 1}, []) == {"latitude": 1, "hourly": {}}

    def test_out_of_range_index_raises_index_error(self, sample_api_response):
        with pytest.raises(IndexError):
            bf.build_day_payload(sample_api_response, [99])


class TestObjectExists:
    def test_true_when_head_succeeds(self):
        client = MagicMock()
        assert bf.object_exists(client, "b", "k") is True
        client.head_object.assert_called_once_with(Bucket="b", Key="k")

    @pytest.mark.parametrize("code", ["404", "NoSuchKey", "NotFound"])
    def test_false_for_not_found_codes(self, code):
        client = MagicMock()
        client.head_object.side_effect = client_error(code)
        assert bf.object_exists(client, "b", "k") is False

    def test_other_errors_are_reraised(self):
        client = MagicMock()
        client.head_object.side_effect = client_error("403")
        with pytest.raises(ClientError):
            bf.object_exists(client, "b", "k")


class TestUploadDay:
    def test_put_object_arguments(self):
        client = MagicMock()
        bf.upload_day(client, "b", "k", {"a": 1})
        kwargs = client.put_object.call_args.kwargs
        assert kwargs["Bucket"] == "b"
        assert kwargs["Key"] == "k"
        assert kwargs["ContentType"] == "application/json"
        assert json.loads(kwargs["Body"]) == {"a": 1}


class TestParseArgs:
    def test_defaults(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["x"])
        args = bf.parse_args()
        assert args.start_date is None and args.end_date is None

    def test_explicit(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["x", "--start-date", "2024-01-01", "--end-date", "2024-01-05"])
        args = bf.parse_args()
        assert (args.start_date, args.end_date) == ("2024-01-01", "2024-01-05")


class TestMain:
    @pytest.fixture
    def env(self, monkeypatch, sample_api_response):
        """Patch every external edge of main(); returns the fake S3 client and fetch mock."""
        client = MagicMock()
        client.head_object.side_effect = client_error("404")
        with patch.object(bf, "load_config", return_value=CONFIG), patch.object(
            bf, "fetch_weather_range", return_value=sample_api_response
        ) as fetch, patch.object(bf.boto3.session, "Session") as session_cls:
            session_cls.return_value.client.return_value = client
            self.client, self.fetch = client, fetch
            yield

    def run(self, monkeypatch, *argv):
        monkeypatch.setattr(sys, "argv", ["x", *argv])
        bf.main()

    def uploaded_keys(self):
        return [c.kwargs["Key"] for c in self.client.put_object.call_args_list]

    def test_uploads_each_day_in_range(self, monkeypatch, env):
        self.run(monkeypatch, "--start-date", "2024-01-01", "--end-date", "2024-01-02")
        self.fetch.assert_called_once_with(date(2024, 1, 1), date(2024, 1, 2), "44.43", "26.10")
        assert self.uploaded_keys() == [
            bf.build_s3_key(date(2024, 1, 1)),
            bf.build_s3_key(date(2024, 1, 2)),
        ]
        # Each day's body contains only that day's hours.
        first = json.loads(self.client.put_object.call_args_list[0].kwargs["Body"])
        assert first["hourly"]["time"] == ["2024-01-01T22:00", "2024-01-01T23:00"]

    def test_existing_days_are_skipped(self, monkeypatch, env):
        self.client.head_object.side_effect = [None, client_error("404")]
        self.run(monkeypatch, "--start-date", "2024-01-01", "--end-date", "2024-01-02")
        assert self.uploaded_keys() == [bf.build_s3_key(date(2024, 1, 2))]

    def test_day_without_hourly_data_is_failed_but_run_continues(self, monkeypatch, env):
        # 2024-01-03 is in range but absent from the API response.
        self.run(monkeypatch, "--start-date", "2024-01-02", "--end-date", "2024-01-03")
        assert self.uploaded_keys() == [bf.build_s3_key(date(2024, 1, 2))]

    def test_empty_api_response_uploads_nothing(self, monkeypatch, env):
        self.fetch.return_value = {}
        self.run(monkeypatch, "--start-date", "2024-01-01", "--end-date", "2024-01-02")
        self.client.put_object.assert_not_called()

    def test_s3_client_error_on_one_day_does_not_stop_others(self, monkeypatch, env):
        self.client.put_object.side_effect = [client_error("InternalError", "PutObject"), None]
        self.run(monkeypatch, "--start-date", "2024-01-01", "--end-date", "2024-01-02")
        assert self.client.put_object.call_count == 2

    def test_unexpected_head_error_is_counted_as_failure_not_crash(self, monkeypatch, env):
        self.client.head_object.side_effect = client_error("403")
        self.run(monkeypatch, "--start-date", "2024-01-01", "--end-date", "2024-01-01")
        self.client.put_object.assert_not_called()

    def test_no_credentials_aborts(self, monkeypatch, env):
        self.client.head_object.side_effect = NoCredentialsError()
        with pytest.raises(SystemExit) as exc:
            self.run(monkeypatch, "--start-date", "2024-01-01", "--end-date", "2024-01-01")
        assert exc.value.code == 1

    def test_defaults_use_default_start_and_yesterday(self, monkeypatch, env):
        self.fetch.return_value = {}
        self.run(monkeypatch)
        start, end = self.fetch.call_args.args[:2]
        assert start == bf.DEFAULT_START_DATE
        assert end == date.today() - timedelta(days=1)

    @pytest.mark.parametrize(
        "argv",
        [
            ["--start-date", "nope"],
            ["--end-date", "2024-13-01"],
            ["--start-date", "2024-02-01", "--end-date", "2024-01-01"],
        ],
    )
    def test_invalid_arguments_exit_before_fetch(self, monkeypatch, env, argv):
        with pytest.raises(SystemExit) as exc:
            self.run(monkeypatch, *argv)
        assert exc.value.code == 1
        self.fetch.assert_not_called()
