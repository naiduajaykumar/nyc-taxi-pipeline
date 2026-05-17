import psycopg2
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from config import DB_CONFIG, RAW_SCHEMA, RAW_TABLE


def test_table_exists():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = '{RAW_SCHEMA}'
            AND table_name = '{RAW_TABLE}'
        )
    """)
    exists = cur.fetchone()[0]
    conn.close()
    assert exists, f"Table {RAW_SCHEMA}.{RAW_TABLE} does not exist!"
    print("✅ test_table_exists passed")


def test_row_count():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {RAW_SCHEMA}.{RAW_TABLE}")
    count = cur.fetchone()[0]
    conn.close()
    assert count > 1_000_000, f"Expected >1M rows, got {count}"
    print(f"✅ test_row_count passed — {count:,} rows")


def test_no_null_datetimes():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(f"""
        SELECT COUNT(*) FROM {RAW_SCHEMA}.{RAW_TABLE}
        WHERE tpep_pickup_datetime IS NULL
           OR tpep_dropoff_datetime IS NULL
    """)
    nulls = cur.fetchone()[0]
    conn.close()
    assert nulls == 0, f"Found {nulls} rows with null datetimes!"
    print("✅ test_no_null_datetimes passed")


def test_no_negative_fares():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(f"""
        SELECT COUNT(*) FROM {RAW_SCHEMA}.{RAW_TABLE}
        WHERE fare_amount < 0
    """)
    negatives = cur.fetchone()[0]
    conn.close()
    assert negatives == 0, f"Found {negatives} rows with negative fares!"
    print("✅ test_no_negative_fares passed")


if __name__ == "__main__":
    test_table_exists()
    test_row_count()
    test_no_null_datetimes()
    test_no_negative_fares()
    print("\n🎉 All tests passed!")
