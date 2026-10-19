import io
import math
import os
from datetime import date as date_type
from typing import List, Optional

import boto3
import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI
from pydantic import BaseModel

load_dotenv()

AWS_REGION = os.environ["AWS_REGION"]
S3_BUCKET = os.environ["S3_BUCKET_NAME"]
GOLD_KEY = "gold/weather_gold.parquet"

s3 = boto3.client("s3", region_name=AWS_REGION)


class WeatherRecord(BaseModel):
    date: date_type
    temp_avg: float
    temp_min: float
    temp_max: float
    humidity_avg: float
    precipitation_total: float
    wind_speed_avg: float
    pressure_avg: float
    temp_avg_lag1: Optional[float] = None
    temp_avg_rolling_7d: Optional[float] = None
    target_next_day_temp: Optional[float] = None


def load_gold_data() -> pd.DataFrame:
    obj = s3.get_object(Bucket=S3_BUCKET, Key=GOLD_KEY)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


def _clean_value(value):
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def _clean_record(record: dict) -> dict:
    return {key: _clean_value(value) for key, value in record.items()}


app = FastAPI(title="Weather Pipeline API")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/weather/latest", response_model=WeatherRecord)
def get_latest_weather():
    df = load_gold_data()
    latest_row = df.sort_values("date", ascending=False).iloc[0]
    return _clean_record(latest_row.to_dict())


@app.get("/weather/history", response_model=List[WeatherRecord])
def get_weather_history(days: int = 7):
    df = load_gold_data()
    df_sorted = df.sort_values("date", ascending=False)
    records = df_sorted.head(days).to_dict(orient="records")
    return [_clean_record(r) for r in records]