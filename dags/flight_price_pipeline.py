
from datetime import timedelta
from pathlib import Path

import pendulum

from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook

from src.ingestion.mysql_loader import load_csv_to_mysql
from src.validation.data_quality import (
    run_data_quality_checks,
    print_quality_report,
)


CSV_PATH = (
    Path("/opt/airflow/data")
    / "Flight_Price_Dataset_of_Bangladesh.csv"
)

MYSQL_CONN_ID = "mysql_flight_staging"


@dag(
    dag_id="flight_price_pipeline",
    description="Ingest and validate flight price data",
    start_date=pendulum.datetime(2026, 11, 11, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["flight-price-analysis", "data-engineering"],
)
def flight_price_pipeline():

    @task
    def ingest_flight_data():

        print("Starting flight data ingestion...")
        print(f"Source file: {CSV_PATH}")

        if not CSV_PATH.exists():
            raise FileNotFoundError(
                f"Dataset not found: {CSV_PATH}"
            )

        hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        connection = hook.get_conn()

        try:
            print("Connected to MySQL successfully.")

            loaded_records = load_csv_to_mysql(
                csv_path=CSV_PATH,
                connection=connection,
                batch_size=1000,
            )

            print(
                f"Successfully ingested "
                f"{loaded_records} records."
            )

            return loaded_records

        finally:
            connection.close()
            print("MySQL connection closed.")

    @task
    def validate_flight_data(loaded_records: int):

        print("Starting data-quality validation...")

        hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        connection = hook.get_conn()

        try:
            results = run_data_quality_checks(connection)

            print_quality_report(results)

            if results["total_records"] != loaded_records:
                raise RuntimeError(
                    "Record count mismatch: "
                    f"ingested={loaded_records}, "
                    f"staged={results['total_records']}"
                )

            critical_checks = [
                "missing_values",
                "invalid_fares",
                "invalid_durations",
            ]

            failed_checks = [
                check
                for check in critical_checks
                if results[check] > 0
            ]

            if failed_checks:
                raise RuntimeError(
                    f"Critical quality checks failed: "
                    f"{failed_checks}"
                )

            if results["fare_discrepancies"] > 0:
                print(
                    "WARNING: Fare discrepancies detected. "
                    "Original fares have been preserved."
                )

            print("DATA QUALITY VALIDATION PASSED.")

            return {
                "total_records": int(results["total_records"]),
                "missing_values": int(results["missing_values"]),
                "invalid_fares": int(results["invalid_fares"]),
                "invalid_durations": int(
                    results["invalid_durations"]
                ),
                "fare_discrepancies": int(
                    results["fare_discrepancies"]
                ),
                "fare_discrepancy_percentage": float(
                    results["fare_discrepancy_percentage"]
                ),
            }

        finally:
            connection.close()
            print("MySQL connection closed.")

    # Define task dependencies.
    loaded_records = ingest_flight_data()
    validate_flight_data(loaded_records)


flight_price_pipeline()
