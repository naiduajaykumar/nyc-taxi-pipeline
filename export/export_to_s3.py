"""
Day 8 — Export dbt Mart Tables to AWS S3

Location in project:  nyc-taxi-pipeline/export/export_to_s3.py

How to run:
  cd C:/Users/Ajayk/de/nyc-taxi-pipeline
  python export/export_to_s3.py

Prerequisites:
  - Set AWS credentials as environment variables:
      set AWS_ACCESS_KEY_ID=your_key_id
      set AWS_SECRET_ACCESS_KEY=your_secret_key
      set AWS_DEFAULT_REGION=us-east-1
  - dbt run must have completed (mart tables must exist)
  - Docker containers running (docker-compose up -d)
"""

from sqlalchemy import create_engine
import boto3
import pandas as pd
import psycopg2
import os
from datetime import datetime


# ── CHANGE THIS to your actual S3 bucket name ───────────────────────────────
BUCKET_NAME = "nyc-taxi-pipeline-ajaynaidu"  # <-- edit this
AWS_REGION = "us-east-1"

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "taxi_warehouse",
    "user": "airflow",
    "password": "airflow",
}


def get_df(table_name):
    engine = create_engine(
        "postgresql+psycopg2://airflow:airflow@localhost:5432/taxi_warehouse"
    )
    df = pd.read_sql(f"SELECT * FROM staging.{table_name}", engine)
    engine.dispose()
    return df


def upload_mart(s3_client, table_name, run_date):
    """Export one mart table to CSV and upload to S3."""
    print(f"  Reading {table_name} from PostgreSQL...")
    df = get_df(table_name)
    print(f"  Rows: {len(df)}")

    # Save locally as CSV
    local_csv = f"{table_name}.csv"
    df.to_csv(local_csv, index=False)

    # S3 key = folder path inside the bucket
    s3_key = f"marts/{table_name}/{run_date}/{table_name}.csv"

    # Upload to S3
    s3_client.upload_file(local_csv, BUCKET_NAME, s3_key)
    os.remove(local_csv)  # clean up local temp file

    print(f"  Uploaded: s3://{BUCKET_NAME}/{s3_key}")
    return s3_key


if __name__ == "__main__":
    run_date = datetime.today().strftime("%Y-%m-%d")

    print("=== NYC TAXI PIPELINE — S3 EXPORT ===")
    print(f"Bucket:   s3://{BUCKET_NAME}/")
    print(f"Run date: {run_date}")
    print()

    # boto3 reads AWS credentials from environment variables automatically
    s3 = boto3.client("s3", region_name=AWS_REGION)

    marts = ["mart_daily_trips", "mart_zone_performance", "mart_hourly_patterns"]

    uploaded_keys = []
    for mart in marts:
        key = upload_mart(s3, mart, run_date)
        uploaded_keys.append(key)

    print()
    print("=== UPLOAD COMPLETE ===")
    for key in uploaded_keys:
        print(f"  s3://{BUCKET_NAME}/{key}")
    print()
    print("Go to AWS Console -> S3 -> your bucket to verify files.")
