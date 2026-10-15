import json
import logging
import sys
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest
import requests
from botocore.exceptions import ClientError, NoCredentialsError

from ingestion import ingest_weather as iw

CONFIG = {
    "aws_access_key_id": "AKIAFAKE",
    "aws_secret_access_key": "secret",
    "aws_region": "eu-central-1",
    "s3_bucket_name": "my-bucket",
    "latitude": "44.43",
    "longitude": "26.10",
}


class TestLoadConfig:
    def test_returns_all_values(self, valid_env):
        config = iw.load_config()
        assert config["aws_access_key_id"] == "AKIAFAKE"
        assert config["aws_region"] == "eu-west-1"
        assert config["s3_bucket_name"] == "test-bucket"
        assert config["latitude"] == "44.43"
        assert config["longitude"] == "26.10"

    def test_region_defaults_to_eu_central_1(self, valid_env, monkeypatch):
        monkeypatch.delenv("AWS_REGION")
        assert iw.load_config()["aws_region"] == "eu-central-1"

    @pytest.mark.parametrize(
        "missing", ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "S3_BUCKET_NAME"]
    )
    def test_missing_required_aws_setting_exits(self, valid_env, monkeypatch, missing):
        monkeypatch.delenv(missing)
        with pytest.raises(SystemExit) as exc:
            iw.load_config()
        assert exc.value.code == 1

    @pytest.mark.parametrize("missing", ["OPEN_METEO_LATITUDE", "OPEN_METEO_LONGITUDE"])
    def test_missing_coordinates_exits(self, valid_env, monkeypatch, missing):
        monkeypatch.delenv(missing)
        with pytest.raises(SystemExit) as exc:
            iw.load_config()
        assert exc.value.code == 1

    def test_empty_string_counts_as_missing(self, valid_env, monkeypatch):
        monkeypatch.setenv("S3_BUCKET_NAME", "")
        with pytest.raises(SystemExit):
            iw.load_config()


class TestFetchWeatherData:
    def test_valid_response_is_returned_and_request_built_correctly(
        self, sample_api_response
    ):
        response = MagicMock()
        response.json.return_value = sample_api_response
        with patch.object(iw.requests, "get", return_value=response) as mock_get:
            result = iw.fetch_weather_data(date(2024, 1, 1), "44.43", "26.10")

        assert result == sample_api_response
        args, kwargs = mock_get.call_args
        assert args[0] == iw.ARCHIVE_API_URL
        params = kwargs["params"]
        assert params["latitude"] == "44.43"
        assert params["longitude"] == "26.10"
        assert params["start_date"] == params["end_date"] == "2024-01-01"
        assert params["hourly"] == ",".join(iw.HOURLY_VARIABLES)
        assert kwargs["timeout"] == 30
        response.raise_for_status.assert_called_once()

    @pytest.mark.parametrize(
        "error",
        [
            requests.ConnectionError("boom"),
            requests.Timeout("slow"),
            requests.HTTPError("500 Server Error"),
        ],
    )
    def test_request_errors_exit_cleanly(self, error):
        with patch.object(iw.requests, "get", side_effect=error):
            with pytest.raises(SystemExit) as exc:
                iw.fetch_weather_data(date(2024, 1, 1), "1", "2")
        assert exc.value.code == 1

    def test_http_error_status_exits(self):
        response = MagicMock()
        response.raise_for_status.side_effect = requests.HTTPError("429")
        with patch.object(iw.requests, "get", return_value=response):
            with pytest.raises(SystemExit):
                iw.fetch_weather_data(date(2024, 1, 1), "1", "2")
        response.json.assert_not_called()

    def test_invalid_json_body_exits(self):
        response = MagicMock()
        response.json.side_effect = ValueError("not json")
        with patch.object(iw.requests, "get", return_value=response):
            with pytest.raises(SystemExit) as exc:
                iw.fetch_weather_data(date(2024, 1, 1), "1", "2")
        assert exc.value.code == 1

    def test_empty_json_is_returned_as_is(self):
        response = MagicMock()
        response.json.return_value = {}
        with patch.object(iw.requests, "get", return_value=response):
            assert iw.fetch_weather_data(date(2024, 1, 1), "1", "2") == {}


class TestUploadToS3:
    @pytest.fixture
    def s3_client(self):
        client = MagicMock()
        with patch.object(iw.boto3.session, "Session") as session_cls:
            session_cls.return_value.client.return_value = client
            self.session_cls = session_cls
            yield client

    def test_puts_object_with_partitioned_key_and_json_body(
        self, s3_client, sample_api_response
    ):
        iw.upload_to_s3(sample_api_response, date(2024, 3, 5), CONFIG)

        kwargs = s3_client.put_object.call_args.kwargs
        assert kwargs["Bucket"] == "my-bucket"
        assert kwargs["Key"] == (
            "bronze/weather/year=2024/month=03/day=05/weather_2024-03-05.json"
        )
        assert kwargs["ContentType"] == "application/json"
        assert json.loads(kwargs["Body"].decode("utf-8")) == sample_api_response

    def test_session_built_from_config(self, s3_client):
        iw.upload_to_s3({}, date(2024, 1, 1), CONFIG)
        self.session_cls.assert_called_once_with(
            aws_access_key_id="AKIAFAKE",
            aws_secret_access_key="secret",
            region_name="eu-central-1",
        )

    def test_no_credentials_exits(self, s3_client):
        s3_client.put_object.side_effect = NoCredentialsError()
        with pytest.raises(SystemExit) as exc:
            iw.upload_to_s3({}, date(2024, 1, 1), CONFIG)
        assert exc.value.code == 1

    def test_client_error_exits(self, s3_client):
        s3_client.put_object.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "nope"}}, "PutObject"
        )
        with pytest.raises(SystemExit) as exc:
            iw.upload_to_s3({}, date(2024, 1, 1), CONFIG)
        assert exc.value.code == 1


class TestParseArgs:
    def test_default_date_is_none(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["ingest_weather.py"])
        assert iw.parse_args().date is None

    def test_date_argument(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["ingest_weather.py", "--date", "2024-02-01"])
        assert iw.parse_args().date == "2024-02-01"


class TestMain:
    @pytest.fixture
    def mocks(self):
        with patch.object(iw, "load_config", return_value=CONFIG) as cfg, patch.object(
            iw, "fetch_weather_data", return_value={"hourly": {}}
        ) as fetch, patch.object(iw, "upload_to_s3") as upload:
            yield cfg, fetch, upload

    def test_explicit_date_is_used(self, monkeypatch, mocks):
        _, fetch, upload = mocks
        monkeypatch.setattr(sys, "argv", ["x", "--date", "2024-02-01"])
        iw.main()
        fetch.assert_called_once_with(date(2024, 2, 1), "44.43", "26.10")
        upload.assert_called_once_with({"hourly": {}}, date(2024, 2, 1), CONFIG)

    def test_defaults_to_yesterday(self, monkeypatch, mocks):
        _, fetch, _ = mocks
        monkeypatch.setattr(sys, "argv", ["x"])
        iw.main()
        assert fetch.call_args.args[0] == date.today() - timedelta(days=1)

    def test_invalid_date_exits_before_any_io(self, monkeypatch, mocks):
        cfg, fetch, upload = mocks
        monkeypatch.setattr(sys, "argv", ["x", "--date", "01-02-2024"])
        with pytest.raises(SystemExit) as exc:
            iw.main()
        assert exc.value.code == 1
        cfg.assert_not_called()
        fetch.assert_not_called()
        upload.assert_not_called()
