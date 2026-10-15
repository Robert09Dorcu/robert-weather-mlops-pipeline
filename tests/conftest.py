import pytest


@pytest.fixture
def sample_api_response():
    """Minimal valid Open-Meteo archive response covering two days (3 hourly points)."""
    return {
        "latitude": 44.43,
        "longitude": 26.1,
        "timezone": "GMT",
        "hourly_units": {"temperature_2m": "°C"},
        "hourly": {
            "time": ["2024-01-01T22:00", "2024-01-01T23:00", "2024-01-02T00:00"],
            "temperature_2m": [1.0, 0.5, 0.0],
            "precipitation": [0.0, 0.1, 0.2],
        },
    }


@pytest.fixture
def valid_env(monkeypatch):
    """Set a complete, fake environment and stop load_dotenv from reading the real .env."""
    monkeypatch.setattr("ingestion.ingest_weather.load_dotenv", lambda *a, **k: None)
    values = {
        "AWS_ACCESS_KEY_ID": "AKIAFAKE",
        "AWS_SECRET_ACCESS_KEY": "fake-secret",
        "AWS_REGION": "eu-west-1",
        "S3_BUCKET_NAME": "test-bucket",
        "OPEN_METEO_LATITUDE": "44.43",
        "OPEN_METEO_LONGITUDE": "26.10",
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)
    return values
