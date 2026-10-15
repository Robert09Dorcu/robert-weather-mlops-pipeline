import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime, timedelta

DATABRICKS_JOB_ID = int(os.environ["DATABRICKS_JOB_ID"])

default_args = {
    "owner": "robert",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="weather_ingestion",
    default_args=default_args,
    description="Ingestie zilnica + procesare Silver/Gold pentru datele meteo",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=["weather", "bronze", "ingestion"],
) as dag:

    ingest_task = BashOperator(
        task_id="ingest_weather_data",
        bash_command="cd /opt/airflow/project && python -m ingestion.ingest_weather",
    )

    process_task = DatabricksRunNowOperator(
        task_id="process_silver_gold",
        databricks_conn_id="databricks_default",
        job_id=DATABRICKS_JOB_ID,
    )

    ingest_task >> process_task