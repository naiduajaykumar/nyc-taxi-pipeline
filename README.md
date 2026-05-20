# NYC Taxi Data Pipeline

A production-grade end-to-end data engineering pipeline processing
NYC Yellow Taxi trip data using Python, PostgreSQL, dbt, Great Expectations, and AWS S3.

## Architecture
<img width="1837" height="724" alt="project1-dbt-dag" src="https://github.com/user-attachments/assets/59174006-0baa-40c8-b5f9-9ccbf5923274" />

```
NYC Taxi Parquet Files (Public Dataset)
         |
         v
Python Ingestion --> PostgreSQL (Docker) --> dbt Transforms
ingestion/ingest.py  raw.yellow_taxi_trips   staging -> intermediate -> marts
                                                          |
                             +----------------------------+
                             |                  |
                             v                  v
                    Great Expectations      AWS S3
                    Data Quality Suite      Cloud Storage
                    (HTML Data Docs)        (3 mart CSVs)
```

## Tech Stack

| Tool               | Version | Purpose                     |
|--------------------|---------|------------------------------|
| Python             | 3.11    | Ingestion & orchestration    |
| PostgreSQL         | 15      | Local data warehouse         |
| dbt                | 1.7     | SQL transformations          |
| Great Expectations | 0.18.x  | Data quality validation      |
| AWS S3             | —       | Cloud storage                |
| Docker             | —       | Local environment            |

## Key Results

<!-- EDIT THESE with your actual numbers after running the pipeline -->
- **2.9 million** NYC taxi trips ingested and processed
- **35 days** of trip data transformed across **3 mart tables**
- **258 NYC zones** analyzed for pickup/dropoff performance
- **168 hourly patterns** captured (24 hours × 7 days of week)
- **9 data quality expectations** validated via Great Expectations
- **3 mart tables** exported to AWS S3 cloud storage


## Project Structure

```
nyc-taxi-pipeline/
├── docker-compose.yml          # PostgreSQL + pgAdmin containers
├── ingestion/
│   └── ingest.py               # Downloads + loads Parquet data
├── dbt/
│   └── taxi_transform/
│       ├── models/
│       │   ├── staging/        # stg_taxi_trips, stg_taxi_zones
│       │   ├── intermediate/   # int_taxi_enriched
│       │   └── marts/          # mart_daily, mart_zone, mart_hourly
│       └── seeds/
│           └── taxi_zones.csv
├── gx/                         # Great Expectations config
│   ├── day7_ge_setup.py
│   └── day7_run_checkpoint.py
├── export/
│   └── export_to_s3.py         # Upload marts to AWS S3
└── README.md
```

## How to Run

### 1. Prerequisites

```bash
pip install dbt-postgres great-expectations boto3 pandas psycopg2-binary
```

### 2. Start Docker

```bash
docker-compose up -d
```

### 3. Ingest Data

```bash
python ingestion/ingest.py
```

### 4. Run dbt Transformations

```bash
cd dbt/taxi_transform
dbt run
dbt test
dbt docs generate && dbt docs serve
```

### 5. Run Data Quality Validation

```bash
python gx/day7_ge_setup.py        # first time only
python gx/day7_run_checkpoint.py
```

### 6. Export to AWS S3

```bash
# Set credentials first:
set AWS_ACCESS_KEY_ID=your_key
set AWS_SECRET_ACCESS_KEY=your_secret
set AWS_DEFAULT_REGION=us-east-1

python export/export_to_s3.py
```

## Author
**Ajay Kumar Naidu**
