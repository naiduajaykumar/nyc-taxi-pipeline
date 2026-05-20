from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from datetime import datetime, timedelta
import logging

default_args = {
    "owner": "ajay",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}
with DAG(
    dag_id="nyc_taxi_pipeline",
    default_args=default_args,
    description="Monthly NYC Taxi ELT pipeline",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["taxi", "etl", "portfolio"],
) as dag:
    check_url = HttpSensor(
        task_id="check_source_url",
        http_conn_id="http_default",
        endpoint="trip-data/yellow_tripdata_2024-01.parquet",
        method="HEAD",
        response_check=lambda r: r.status_code == 200,
        poke_interval=30,
        timeout=300,
    )
ingest_data = BashOperator(
    task_id="ingest_taxi_data",
    bash_command="cd /opt/airflow/ingestion && python extract_taxi.py",
    env={
        "DB_HOST": "postgres",
        "DB_PORT": "5432",
        "DB_NAME": "taxi_warehouse",
        "DB_USER": "airflow",
        "DB_PASSWORD": "airflow",
    },
)
validate_data = BashOperator(
    task_id="validate_taxi_data",
    bash_command="cd /opt/airflow/ingestion && python test_extract.py",
    env={
        "DB_HOST": "postgres",
        "DB_PORT": "5432",
        "DB_NAME": "taxi_warehouse",
        "DB_USER": "airflow",
        "DB_PASSWORD": "airflow",
    },
)


def log_completion():
    import psycopg2

    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="taxi_warehouse",
        user="airflow",
        password="airflow",
    )
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM raw.yellow_taxi_trips")
    count = cur.fetchone()[0]
    conn.close()
    logging.info(f"Pipeline complete! {count:,} rows in raw.yellow_taxi_trips")
    log_success = PythonOperator(
        task_id="log_success",
        python_callable=log_completion,
    )
    check_url >> ingest_data >> validate_data >> log_success
