"""Bronze-layer backfill: fetch a historical range of daily weather data from
Open-Meteo in a single API call and land each day in S3, skipping days that
already exist so the script can be safely resumed.
"""
import argparse
import json
import logging
import sys
from datetime import date, timedelta

import boto3
import requests
from botocore.exceptions import ClientError, NoCredentialsError

from ingestion.ingest_weather import ARCHIVE_API_URL, HOURLY_VARIABLES, load_config

logger = logging.getLogger("backfill_weather")

DEFAULT_START_DATE = date(2024, 1, 1)


def daterange(start_date: date, end_date: date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def build_s3_key(target_date: date) -> str:
    return (
        f"bronze/weather/year={target_date.year:04d}/"
        f"month={target_date.month:02d}/day={target_date.day:02d}/"
        f"weather_{target_date.isoformat()}.json"
    )


def fetch_weather_range(
    start_date: date, end_date: date, latitude: str, longitude: str
) -> dict:
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "hourly": ",".join(HOURLY_VARIABLES),
    }

    logger.info(
        "Fetching weather data from %s to %s from Open-Meteo archive API "
        "(single request)",
        start_date.isoformat(),
        end_date.isoformat(),
    )
    try:
        response = requests.get(ARCHIVE_API_URL, params=params, timeout=120)
        response.raise_for_status()
    except requests.RequestException as exc:
        logger.error("Failed to fetch weather data from Open-Meteo: %s", exc)
        sys.exit(1)

    try:
        return response.json()
    except ValueError as exc:
        logger.error("Open-Meteo returned an invalid JSON body: %s", exc)
        sys.exit(1)


def group_hourly_by_day(data: dict) -> dict:
    hourly = data.get("hourly", {})
    times = hourly.get("time", [])

    indices_by_day: dict[str, list[int]] = {}
    for idx, timestamp in enumerate(times):
        day_str = timestamp[:10]
        indices_by_day.setdefault(day_str, []).append(idx)

    return indices_by_day


def build_day_payload(data: dict, indices: list) -> dict:
    hourly = data.get("hourly", {})

    day_payload = {
        key: value for key, value in data.items() if key != "hourly"
    }
    day_payload["hourly"] = {
        field: [values[i] for i in indices] for field, values in hourly.items()
    }
    return day_payload


def object_exists(s3_client, bucket: str, key: str) -> bool:
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def upload_day(s3_client, bucket: str, key: str, payload: dict) -> None:
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload).encode("utf-8"),
        ContentType="application/json",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill historical daily weather data into the Bronze S3 layer."
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help=f"Start date in YYYY-MM-DD format. Defaults to {DEFAULT_START_DATE.isoformat()}.",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="End date in YYYY-MM-DD format. Defaults to yesterday.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    try:
        start_date = (
            date.fromisoformat(args.start_date) if args.start_date else DEFAULT_START_DATE
        )
        end_date = (
            date.fromisoformat(args.end_date)
            if args.end_date
            else date.today() - timedelta(days=1)
        )
    except ValueError as exc:
        logger.error("Invalid date argument: %s", exc)
        sys.exit(1)

    if start_date > end_date:
        logger.error(
            "--start-date (%s) must not be after --end-date (%s)",
            start_date.isoformat(),
            end_date.isoformat(),
        )
        sys.exit(1)

    config = load_config()
    data = fetch_weather_range(start_date, end_date, config["latitude"], config["longitude"])
    indices_by_day = group_hourly_by_day(data)

    session = boto3.session.Session(
        aws_access_key_id=config["aws_access_key_id"],
        aws_secret_access_key=config["aws_secret_access_key"],
        region_name=config["aws_region"],
    )
    s3_client = session.client("s3")
    bucket = config["s3_bucket_name"]

    all_days = list(daterange(start_date, end_date))
    total_days = len(all_days)
    logger.info("Backfilling %d days (%s to %s)", total_days, start_date, end_date)

    uploaded = 0
    skipped = 0
    failed_days = []

    for processed, target_date in enumerate(all_days, start=1):
        day_str = target_date.isoformat()
        key = build_s3_key(target_date)

        try:
            if object_exists(s3_client, bucket, key):
                logger.info("Skipping %s (already exists in S3)", day_str)
                skipped += 1
                continue

            indices = indices_by_day.get(day_str)
            if not indices:
                raise ValueError(
                    f"No hourly data returned by Open-Meteo for {day_str}"
                )

            payload = build_day_payload(data, indices)
            upload_day(s3_client, bucket, key, payload)
            uploaded += 1
            logger.info("Uploaded %d/%d days (%s)", processed, total_days, day_str)
        except NoCredentialsError:
            logger.error("AWS credentials are invalid or not found.")
            sys.exit(1)
        except (ClientError, ValueError, KeyError) as exc:
            logger.error("Failed to process %s: %s", day_str, exc)
            failed_days.append(day_str)
            continue

    logger.info(
        "Backfill complete. Total: %d, uploaded: %d, skipped (already existed): %d, failed: %d",
        total_days,
        uploaded,
        skipped,
        len(failed_days),
    )
    if failed_days:
        logger.warning("Failed days: %s", ", ".join(failed_days))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    main()
