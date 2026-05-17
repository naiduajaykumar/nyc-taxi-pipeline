import logging
import os
import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from config import (
    DB_CONFIG,
    RAW_SCHEMA,
    RAW_TABLE,
    DATA_YEAR,
    DATA_MONTH,
    TAXI_BASE_URL,
)

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/ingestion.log"),
    ],
)
log = logging.getLogger(__name__)


# ── Step 1: Download data ─────────────────────────────────────────────────────
def download_taxi_data(year: int, month: int) -> pd.DataFrame:
    url = f"{TAXI_BASE_URL}/yellow_tripdata_{year}-{month:02d}.parquet"
    local_path = f"data/yellow_tripdata_{year}-{month:02d}.parquet"

    os.makedirs("data", exist_ok=True)

    if os.path.exists(local_path):
        log.info(f"File already exists locally — skipping download: {local_path}")
    else:
        log.info(f"Downloading: {url}")
        response = requests.get(url, stream=True, timeout=120)
        response.raise_for_status()

        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        log.info(f"Download complete: {local_path}")

    log.info("Reading parquet file into DataFrame...")
    df = pd.read_parquet(local_path)
    log.info(f"Loaded {len(df):,} rows, {len(df.columns)} columns")
    return df


# ── Step 2: Clean and standardize ────────────────────────────────────────────
def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Cleaning DataFrame...")

    # Standardize column names to lowercase with underscores
    df.columns = [c.lower().strip().replace(" ", "_") for c in df.columns]

    # Drop rows with missing pickup or dropoff times (critical fields)
    before = len(df)
    df = df.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime"])
    log.info(f"Dropped {before - len(df):,} rows with null datetime values")

    # Remove rows with negative fares or distances (data quality)
    df = df[df["fare_amount"] >= 0]
    df = df[df["trip_distance"] >= 0]

    # Add ingestion metadata columns
    df["_ingested_at"] = datetime.utcnow()
    df["_source_file"] = f"yellow_tripdata_{DATA_YEAR}-{DATA_MONTH:02d}.parquet"

    # Convert datetimes to string for PostgreSQL compatibility
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"]).astype(str)
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"]).astype(
        str
    )

    log.info(f"Clean DataFrame: {len(df):,} rows ready for load")
    return df


# ── Step 3: Create raw table ──────────────────────────────────────────────────
def create_raw_table(conn, df: pd.DataFrame):
    log.info(f"Creating table {RAW_SCHEMA}.{RAW_TABLE} if not exists...")

    # Map pandas dtypes to PostgreSQL types
    type_map = {
        "int64": "BIGINT",
        "int32": "INTEGER",
        "float64": "DOUBLE PRECISION",
        "float32": "REAL",
        "object": "TEXT",
        "bool": "BOOLEAN",
        "datetime64[ns]": "TIMESTAMP",
    }

    col_defs = []
    for col, dtype in df.dtypes.items():
        pg_type = type_map.get(str(dtype), "TEXT")
        col_defs.append(f'"{col}" {pg_type}')

    create_sql = f"""
        CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA};
        DROP TABLE IF EXISTS {RAW_SCHEMA}.{RAW_TABLE};
        CREATE TABLE {RAW_SCHEMA}.{RAW_TABLE} (
            _row_id SERIAL PRIMARY KEY,
            {", ".join(col_defs)}
        );
    """

    with conn.cursor() as cur:
        cur.execute(create_sql)
    conn.commit()
    log.info("Table created.")


# ── Step 4: Load data in batches ──────────────────────────────────────────────
def load_to_postgres(conn, df: pd.DataFrame, batch_size: int = 10_000):
    log.info(f"Loading {len(df):,} rows into {RAW_SCHEMA}.{RAW_TABLE}...")

    columns = [f'"{c}"' for c in df.columns]
    insert_sql = f"""
        INSERT INTO {RAW_SCHEMA}.{RAW_TABLE} ({", ".join(columns)})
        VALUES %s
    """

    total_loaded = 0
    records = [tuple(row) for row in df.itertuples(index=False, name=None)]

    with conn.cursor() as cur:
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            execute_values(cur, insert_sql, batch)
            total_loaded += len(batch)
            log.info(f"  Loaded {total_loaded:,} / {len(records):,} rows...")

    conn.commit()
    log.info(f"Load complete — {total_loaded:,} rows inserted.")


# ── Step 5: Validate load ─────────────────────────────────────────────────────
def validate_load(conn, expected_rows: int):
    log.info("Running post-load validation...")

    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {RAW_SCHEMA}.{RAW_TABLE}")
        actual_rows = cur.fetchone()[0]

        cur.execute(f"""
            SELECT
                MIN(tpep_pickup_datetime) AS earliest_trip,
                MAX(tpep_pickup_datetime) AS latest_trip,
                ROUND(AVG(fare_amount::NUMERIC), 2) AS avg_fare,
                ROUND(AVG(trip_distance::NUMERIC), 2) AS avg_distance
            FROM {RAW_SCHEMA}.{RAW_TABLE}
        """)
        stats = cur.fetchone()

    log.info(f"Row count check: expected ~{expected_rows:,}, got {actual_rows:,}")
    log.info(f"Date range:  {stats[0]} → {stats[1]}")
    log.info(f"Avg fare:    ${stats[2]}")
    log.info(f"Avg distance: {stats[3]} miles")

    if actual_rows == 0:
        raise ValueError("Validation FAILED — 0 rows loaded!")

    log.info("Validation PASSED ✅")
    return actual_rows


# ── Main orchestrator ─────────────────────────────────────────────────────────
def run_ingestion(year: int = DATA_YEAR, month: int = DATA_MONTH):
    log.info(f"=== Starting ingestion for {year}-{month:02d} ===")
    start_time = datetime.utcnow()

    # Download
    df = download_taxi_data(year, month)
    expected_rows = len(df)

    # Clean
    df = clean_dataframe(df)

    # Connect to DB
    log.info("Connecting to PostgreSQL...")
    conn = psycopg2.connect(**DB_CONFIG)
    log.info("Connected.")

    try:
        # Create table
        create_raw_table(conn, df)

        # Load
        load_to_postgres(conn, df)

        # Validate
        validate_load(conn, expected_rows)

    finally:
        conn.close()

    elapsed = (datetime.utcnow() - start_time).seconds
    log.info(f"=== Ingestion complete in {elapsed}s ===")


if __name__ == "__main__":
    run_ingestion()
