import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "dbname": os.getenv("DB_NAME", "taxi_warehouse"),
    "user": os.getenv("DB_USER", "airflow"),
    "password": os.getenv("DB_PASSWORD", "airflow"),
}

RAW_SCHEMA = os.getenv("RAW_SCHEMA", "raw")
RAW_TABLE = os.getenv("RAW_TABLE", "yellow_taxi_trips")
DATA_YEAR = int(os.getenv("DATA_YEAR", 2024))
DATA_MONTH = int(os.getenv("DATA_MONTH", 1))

TAXI_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
