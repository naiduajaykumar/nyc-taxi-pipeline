"""
Day 7 — Run Great Expectations Checkpoint
Validates raw taxi data and opens HTML Data Docs in browser.

Location in project:  nyc-taxi-pipeline/gx/day7_run_checkpoint.py

How to run:
  cd C:/Users/Ajayk/de/nyc-taxi-pipeline
  python gx/day7_run_checkpoint.py

Prerequisites:
  - Run day7_ge_setup.py at least once first
  - Docker containers must be running (docker-compose up -d)
"""

import great_expectations as gx

# Get context
context = gx.get_context()

# Get existing datasource and asset
pg_datasource = context.get_datasource("postgres_taxi")
trips_asset = pg_datasource.get_asset("yellow_taxi_trips")
batch_request = trips_asset.build_batch_request()

# Create (or update) checkpoint
checkpoint = context.add_or_update_checkpoint(
    name="taxi_quality_checkpoint",
    validations=[
        {
            "batch_request": batch_request,
            "expectation_suite_name": "taxi_raw_quality_suite",
        }
    ],
)

# Run validation
results = checkpoint.run()

# Print summary
print("\n=== VALIDATION RESULTS ===")
print(f"Overall success: {results.success}")

for run_id, result in results.run_results.items():
    stats = result["validation_result"]["statistics"]
    print(f"Evaluated:  {stats['evaluated_expectations']}")
    print(f"Successful: {stats['successful_expectations']}")
    print(f"Failed:     {stats['unsuccessful_expectations']}")

# Open HTML Data Docs in browser
context.open_data_docs()
print("\nData Docs opened in your browser!")
print("Screenshot the report for your portfolio.")
