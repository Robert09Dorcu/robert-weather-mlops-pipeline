"""Bronze-layer ingestion: fetch raw daily weather data from Open-Meteo and land it in S3."""
import argparse
import json
import logging
import os
import sys
from datetime import date, timedelta

import boto3
import requests
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv

ARCHIVE_API_URL = "https://archive-api.open-meteo.com/v1/archive"
HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "surface_pressure",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("ingest_weather")


def load_config() -> dict:
    load_dotenv()

    config = {
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "aws_region": os.getenv("AWS_REGION", "eu-central-1"),
        "s3_bucket_name": os.getenv("S3_BUCKET_NAME"),
        "latitude": os.getenv("OPEN_METEO_LATITUDE"),
        "longitude": os.getenv("OPEN_METEO_LONGITUDE"),
    }

    missing = [
        key
        for key in ("aws_access_key_id", "aws_secret_access_key", "s3_bucket_name")
        if not config[key]
    ]
    if missing:
        logger.error(
            "Missing required configuration in .env: %s. "
            "Copy .env.example to .env and fill in real values.",
            ", ".join(missing),
        )
        sys.exit(1)

    if not config["latitude"] or not config["longitude"]:
        logger.error("Missing OPEN_METEO_LATITUDE or OPEN_METEO_LONGITUDE in .env")
        sys.exit(1)

    return config


def fetch_weather_data(target_date: date, latitude: str, longitude: str) -> dict:
    date_str = target_date.isoformat()
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": date_str,
        "end_date": date_str,
        "hourly": ",".join(HOURLY_VARIABLES),
    }

    logger.info("Fetching weather data for %s from Open-Meteo archive API", date_str)
    try:
        response = requests.get(ARCHIVE_API_URL, params=params, timeout=30)
        response.raise_for_status()
    except requests.RequestException as exc:
        logger.error("Failed to fetch weather data from Open-Meteo: %s", exc)
        sys.exit(1)

    try:
        return response.json()
    except ValueError as exc:
        logger.error("Open-Meteo returned an invalid JSON body: %s", exc)
        sys.exit(1)


def upload_to_s3(data: dict, target_date: date, config: dict) -> None:
    s3_key = (
        f"bronze/weather/year={target_date.year:04d}/"
        f"month={target_date.month:02d}/day={target_date.day:02d}/"
        f"weather_{target_date.isoformat()}.json"
    )

    session = boto3.session.Session(
        aws_access_key_id=config["aws_access_key_id"],
        aws_secret_access_key=config["aws_secret_access_key"],
        region_name=config["aws_region"],
    )
    s3_client = session.client("s3")

    logger.info(
        "Uploading raw weather data to s3://%s/%s", config["s3_bucket_name"], s3_key
    )
    try:
        s3_client.put_object(
            Bucket=config["s3_bucket_name"],
            Key=s3_key,
            Body=json.dumps(data).encode("utf-8"),
            ContentType="application/json",
        )
    except NoCredentialsError:
        logger.error("AWS credentials are invalid or not found.")
        sys.exit(1)
    except ClientError as exc:
        logger.error("Failed to upload to S3: %s", exc)
        sys.exit(1)

    logger.info("Successfully uploaded weather data for %s", target_date.isoformat())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest raw daily weather data into the Bronze S3 layer."
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Target date in YYYY-MM-DD format. Defaults to yesterday "
        "(Open-Meteo archive data requires at least a 1-day lag).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.date:
        try:
            target_date = date.fromisoformat(args.date)
        except ValueError:
            logger.error("Invalid --date value: %s (expected YYYY-MM-DD)", args.date)
            sys.exit(1)
    else:
        target_date = date.today() - timedelta(days=1)

    config = load_config()
    data = fetch_weather_data(target_date, config["latitude"], config["longitude"])
    upload_to_s3(data, target_date, config)


if __name__ == "__main__":
    main()
