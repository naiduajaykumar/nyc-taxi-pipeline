"""
Day 7 — Great Expectations (GE 1.x, final version)
Safe to run multiple times — cleans up previous runs automatically.

How to run:
  cd C:/Users/Ajayk/de/nyc-taxi-pipeline
  python gx\ge_setup.py
"""

import os, pathlib
import great_expectations as gx

print(f"GE version: {gx.__version__}")

# ── Fresh context ─────────────────────────────────────────────────────────────
context = gx.get_context(mode="file")
print("GE context ready")

# ── Wipe previous run (so script is safe to re-run) ──────────────────────────
for name, store in [
    ("taxi_checkpoint", context.checkpoints),
    ("taxi_validation_def", context.validation_definitions),
    ("taxi_raw_quality_suite", context.suites),
]:
    try:
        store.delete(name)
        print(f"  Cleared old: {name}")
    except Exception:
        pass

try:
    context.data_sources.delete("postgres_taxi")
    print("  Cleared old datasource")
except Exception:
    pass

# ── Connect to PostgreSQL ─────────────────────────────────────────────────────
datasource = context.data_sources.add_postgres(
    name="postgres_taxi",
    connection_string="postgresql+psycopg2://airflow:airflow@localhost:5432/taxi_warehouse",
)
print("PostgreSQL connected")

# ── Point to raw taxi trips table ─────────────────────────────────────────────
asset = datasource.add_table_asset(
    name="yellow_taxi_trips", table_name="yellow_taxi_trips", schema_name="raw"
)

batch_def = asset.add_batch_definition_whole_table("whole_table")

# ── Expectation suite ─────────────────────────────────────────────────────────
suite = context.suites.add(gx.ExpectationSuite(name="taxi_raw_quality_suite"))

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="tpep_pickup_datetime")
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="tpep_dropoff_datetime")
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="fare_amount")
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="passenger_count", min_value=0, max_value=9
    )
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="fare_amount", min_value=0, max_value=1000
    )
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="trip_distance", min_value=0, max_value=200
    )
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="payment_type", value_set=[0, 1, 2, 3, 4, 5, 6]
    )
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="total_amount", min_value=-500, max_value=5000
    )
)
suite.add_expectation(
    gx.expectations.ExpectTableRowCountToBeBetween(min_value=1000, max_value=50_000_000)
)

print(f"Added {len(suite.expectations)} expectations")

# ── Validation definition + checkpoint ───────────────────────────────────────
val_def = context.validation_definitions.add(
    gx.ValidationDefinition(name="taxi_validation_def", data=batch_def, suite=suite)
)

checkpoint = context.checkpoints.add(
    gx.Checkpoint(name="taxi_checkpoint", validation_definitions=[val_def])
)

# ── Run ───────────────────────────────────────────────────────────────────────
print("\nRunning validation ...")
results = checkpoint.run()

# ── Print results ─────────────────────────────────────────────────────────────
print("\n=== VALIDATION RESULTS ===")
print(f"Overall success: {results.success}")

for run_id, run_result in results.run_results.items():
    vr = run_result.get("validation_result", {})
    stats = vr.get("statistics", {})
    print(f"  Evaluated : {stats.get('evaluated_expectations', '?')}")
    print(f"  Passed    : {stats.get('successful_expectations', '?')}")
    print(f"  Failed    : {stats.get('unsuccessful_expectations', '?')}")
    print()
    for r in vr.get("results", []):
        status = "PASS" if r["success"] else "FAIL"
        col = r["expectation_config"].get("kwargs", {}).get("column", "table-level")
        etype = r["expectation_config"]["type"]
        print(f"  [{status}]  {col}  —  {etype}")

# ── Build Data Docs (HTML report) ─────────────────────────────────────────────
print("\nBuilding Data Docs ...")
context.build_data_docs()

docs = (
    pathlib.Path(os.getcwd())
    / "gx"
    / "uncommitted"
    / "data_docs"
    / "local_site"
    / "index.html"
)
if docs.exists():
    print("\n*** DATA DOCS READY ***")
    print(f"Open this path in your browser:")
    print(f"  {docs}")
    print("\nScreenshot the page for your portfolio!")
else:
    print("Data Docs built — check gx/uncommitted/data_docs/local_site/")
