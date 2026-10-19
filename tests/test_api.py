import importlib
import io
import os
from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from fastapi.testclient import TestClient


@pytest.fixture(scope="module")
def api_main():
    # api.main reads AWS_REGION / S3_BUCKET_NAME and builds the boto3 client at
    # import time, so the fake environment must exist before the first import.
    with patch.dict(
        os.environ, {"AWS_REGION": "eu-central-1", "S3_BUCKET_NAME": "test-bucket"}
    ):
        return importlib.import_module("api.main")


@pytest.fixture
def gold_df():
    """Four days of Gold data, deliberately unsorted.

    The oldest day has no lag/rolling value and the newest day has no target
    (the next day's temperature is not known yet), so both Optional columns
    contain NaN.
    """
    return pd.DataFrame(
        {
            "date": [date(2024, 1, 2), date(2024, 1, 4), date(2024, 1, 1), date(2024, 1, 3)],
            "temp_avg": [2.0, 4.0, 1.0, 3.0],
            "temp_min": [0.5, 2.5, -0.5, 1.5],
            "temp_max": [3.5, 5.5, 2.5, 4.5],
            "humidity_avg": [80.0, 70.0, 85.0, 75.0],
            "precipitation_total": [0.0, 1.2, 0.4, 0.0],
            "wind_speed_avg": [10.0, 12.0, 9.0, 11.0],
            "pressure_avg": [1010.0, 1008.0, 1012.0, 1009.0],
            "temp_avg_lag1": [1.0, 3.0, float("nan"), 2.0],
            "temp_avg_rolling_7d": [1.5, 2.5, float("nan"), 2.0],
            "target_next_day_temp": [3.0, float("nan"), 2.0, 4.0],
        }
    )


@pytest.fixture
def client(api_main, gold_df):
    """TestClient whose S3 get_object returns the Gold parquet built in memory."""
    buffer = io.BytesIO()
    gold_df.to_parquet(buffer, index=False)
    parquet_bytes = buffer.getvalue()

    def fake_get_object(**kwargs):
        return {"Body": io.BytesIO(parquet_bytes)}

    with patch.object(api_main.s3, "get_object", side_effect=fake_get_object) as mock_get:
        test_client = TestClient(api_main.app)
        test_client.mock_get_object = mock_get
        yield test_client


class TestHealth:
    def test_returns_ok(self, client):
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}

    def test_does_not_touch_s3(self, client):
        client.get("/health")
        client.mock_get_object.assert_not_called()


class TestLatest:
    def test_returns_most_recent_day_with_expected_fields(self, client):
        response = client.get("/weather/latest")
        assert response.status_code == 200
        body = response.json()
        assert body["date"] == "2024-01-04"
        assert body["temp_avg"] == 4.0
        assert body["temp_min"] == 2.5
        assert body["temp_max"] == 5.5
        assert body["humidity_avg"] == 70.0
        assert body["precipitation_total"] == 1.2
        assert body["wind_speed_avg"] == 12.0
        assert body["pressure_avg"] == 1008.0
        assert body["temp_avg_lag1"] == 3.0
        assert body["temp_avg_rolling_7d"] == 2.5

    def test_reads_gold_key_from_configured_bucket(self, client, api_main):
        client.get("/weather/latest")
        client.mock_get_object.assert_called_once_with(
            Bucket=api_main.S3_BUCKET, Key="gold/weather_gold.parquet"
        )

    def test_nan_in_optional_field_becomes_null(self, client):
        # The newest day has no target_next_day_temp (NaN in the parquet).
        response = client.get("/weather/latest")
        assert response.status_code == 200
        body = response.json()
        assert "target_next_day_temp" in body
        assert body["target_next_day_temp"] is None


class TestHistory:
    def test_days_limits_result_and_sorts_descending(self, client):
        response = client.get("/weather/history", params={"days": 2})
        assert response.status_code == 200
        body = response.json()
        assert len(body) == 2
        assert [r["date"] for r in body] == ["2024-01-04", "2024-01-03"]

    def test_default_is_seven_days_capped_by_available_rows(self, client):
        response = client.get("/weather/history")
        assert response.status_code == 200
        dates = [r["date"] for r in response.json()]
        assert dates == ["2024-01-04", "2024-01-03", "2024-01-02", "2024-01-01"]

    def test_nan_values_become_null_in_every_record(self, client):
        response = client.get("/weather/history", params={"days": 4})
        assert response.status_code == 200
        by_date = {r["date"]: r for r in response.json()}
        assert by_date["2024-01-04"]["target_next_day_temp"] is None
        assert by_date["2024-01-01"]["temp_avg_lag1"] is None
        assert by_date["2024-01-01"]["temp_avg_rolling_7d"] is None
        # Rows with real values are untouched.
        assert by_date["2024-01-03"]["target_next_day_temp"] == 4.0

    @pytest.mark.parametrize("days", [0, -1, -5])
    def test_days_below_one_is_rejected_by_validation(self, client, days):
        response = client.get("/weather/history", params={"days": days})
        assert response.status_code == 422
        client.mock_get_object.assert_not_called()

    def test_non_integer_days_is_rejected_by_validation(self, client):
        response = client.get("/weather/history", params={"days": "abc"})
        assert response.status_code == 422
        client.mock_get_object.assert_not_called()
