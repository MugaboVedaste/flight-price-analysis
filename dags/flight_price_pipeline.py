
from datetime import timedelta
from pathlib import Path
import os

import pendulum

from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.smtp.notifications.smtp import SmtpNotifier

from src.ingestion.mysql_loader import load_csv_to_mysql
from src.validation.data_quality import (
    run_data_quality_checks,
    print_quality_report,
)
from src.transformation.run_transformation import run_transformation
from src.loading.postgres_loader import load_transformed_csv


CSV_PATH = (
    Path("/opt/airflow/data")
    / "Flight_Price_Dataset_of_Bangladesh.csv"
)

TRANSFORMED_CSV_PATH = (
    Path("/opt/airflow/data")
    / "processed"
    / "transformed_flight_prices.csv"
)

MYSQL_CONN_ID = "mysql_flight_staging"
POSTGRES_CONN_ID = "postgres_flight_analytics"

ALERT_EMAIL = os.getenv("GMAIL_ADDRESS")

if not ALERT_EMAIL:
    raise RuntimeError("GMAIL_ADDRESS is not configured.")


failure_email = SmtpNotifier(
    smtp_conn_id="smtp_default",
    from_email=ALERT_EMAIL,
    to=ALERT_EMAIL,
    subject="[Airflow Failure] {{ dag.dag_id }} - {{ ti.task_id }}",
    html_content="""
    <h2>Flight Price Pipeline Failure</h2>

    <p><strong>DAG:</strong> {{ dag.dag_id }}</p>
    <p><strong>Failed task:</strong> {{ ti.task_id }}</p>
    <p><strong>Execution date:</strong> {{ logical_date }}</p>
    <p><strong>Attempt:</strong> {{ ti.try_number }}</p>
    <p><strong>Error:</strong></p>
    <pre>{{ exception }}</pre>

    <p>
        <strong>Task logs:</strong>
        <a href="{{ ti.log_url }}">View Airflow logs</a>
    </p>
    """,
)



@dag(
    dag_id="flight_price_pipeline",
    description="End-to-end flight price data engineering pipeline",
    start_date=pendulum.datetime(2026, 11, 11, tz="UTC"),
    dagrun_timeout=timedelta(hours=2),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": failure_email,
    },

    tags=["flight-price-analysis", "data-engineering"],
    doc_md="""
        # Flight Price Analysis Pipeline

        ## Overview

        This DAG orchestrates an end-to-end data engineering pipeline
        that processes flight price data from Bangladesh.

        The workflow extracts raw flight records, validates data quality,
        transforms the records, and loads processed data into PostgreSQL
        for analytical purposes.

        ## Pipeline Architecture

        **Source:** Bangladesh Flight Price CSV Dataset

        **Staging:** MySQL (`flight_staging`)

        **Transformation:** Python data processing

        **Destination:** PostgreSQL (`flight_analytics`)

        ## Tasks

        1. **ingest_flight_data**
        - Reads flight records from the source CSV.
        - Loads records into the MySQL staging database.

        2. **validate_flight_data**
        - Checks missing critical values.
        - Identifies invalid fares and durations.
        - Reports fare discrepancies.

        3. **transform_flight_data**
        - Processes records in batches.
        - Extracts departure date features.
        - Classifies peak and non-peak seasons.
        - Groups booking lead times.
        - Calculates expected fares and identifies discrepancies.

        4. **load_postgres_data**
        - Loads transformed records into PostgreSQL.
        - Uses source record IDs to prevent duplicate inserts.

        ## Data Quality

        Critical validation checks must pass before transformation.

        Fare discrepancies are reported as warnings and preserved
        for further investigation.

        ## Failure Handling

        - Each task has a maximum of two retries.
        - Retry delay is two minutes.
        - Failure notifications are sent after the final failed attempt.
        - Notifications are delivered through Gmail SMTP.
        - Email alerts include task information and a link to Airflow logs.

        ## Technologies

        - Apache Airflow
        - Python
        - MySQL
        - PostgreSQL
        - Docker
        """,
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

    @task
    def transform_flight_data(validation_result: dict):

        print("Starting flight data transformation...")

        hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        connection = hook.get_conn()

        try:
            print("Connected to MySQL successfully.")

            result = run_transformation(
                connection=connection,
                output_path=TRANSFORMED_CSV_PATH,
            )

            if (
                result["total_processed"]
                != validation_result["total_records"]
            ):
                raise RuntimeError(
                    "Transformation record count mismatch: "
                    f"validated={validation_result['total_records']}, "
                    f"transformed={result['total_processed']}"
                )

            print("TRANSFORMATION TASK COMPLETED.")

            return result

        finally:
            connection.close()
            print("MySQL connection closed.")

    @task
    def load_postgres_data(transformation_result: dict):

        print("Starting PostgreSQL loading...")

        transformed_file = Path(
            transformation_result["output_path"]
        )

        if not transformed_file.exists():
            raise FileNotFoundError(
                f"Transformed CSV not found: {transformed_file}"
            )

        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )
        connection = hook.get_conn()

        try:
            print("Connected to PostgreSQL successfully.")

            result = load_transformed_csv(
                csv_path=transformed_file,
                connection=connection,
                batch_size=1000,
            )

            if (
                result["total_read"]
                != transformation_result["total_processed"]
            ):
                raise RuntimeError(
                    "PostgreSQL loading record count mismatch: "
                    f"transformed={transformation_result['total_processed']}, "
                    f"read={result['total_read']}"
                )

            print("POSTGRESQL LOADING TASK COMPLETED.")

            return result

        finally:
            connection.close()
            print("PostgreSQL connection closed.")

    # --------------------------------------------------
    # Define tasks and dependencies
    # --------------------------------------------------

    ingestion_task = ingest_flight_data()

    validation_task = validate_flight_data(
        ingestion_task
    )

    transformation_task = transform_flight_data(
        validation_task
    )

    postgres_task = load_postgres_data(
        transformation_task
    )

    # --------------------------------------------------
    # Individual task documentation
    # --------------------------------------------------

    ingestion_task.operator.doc_md = """
### Flight Data Ingestion

**Purpose:** Load raw flight price records from the source CSV into MySQL.

**Source:**
- `Flight_Price_Dataset_of_Bangladesh.csv`

**Destination:**
- MySQL staging database
- Table: `raw_flight_prices`

**Processing:**
1. Verify the source CSV exists.
2. Connect using `mysql_flight_staging`.
3. Load records in batches of 1,000.
4. Return the number of ingested records.

**Failure conditions:**
- Missing source file.
- MySQL connection failure.
- Database insertion error.
"""

    validation_task.operator.doc_md = """
### Flight Data Quality Validation

**Purpose:** Validate the integrity and consistency of staged flight records.

**Database:** MySQL (`flight_staging`)

**Validation checks:**
- Missing critical values.
- Invalid flight fares.
- Invalid flight durations.
- Fare discrepancies between total fare and expected fare.

**Execution:**
1. Retrieve data-quality statistics.
2. Compare staged records with ingested records.
3. Fail if critical checks detect invalid records.
4. Report fare discrepancies as warnings.

**Output:** Data-quality summary passed to the transformation task.
"""

    transformation_task.operator.doc_md = """
### Flight Data Transformation

**Purpose:** Transform validated MySQL staging records into an analytics-ready dataset.

**Processing:**
- Normalize flight attributes.
- Extract departure hour, day, month and year.
- Classify peak and non-peak seasons.
- Group booking lead times.
- Calculate expected fares.
- Calculate and flag fare discrepancies.

**Batch size:** 1,000 records.

**Output:**
- `data/processed/transformed_flight_prices.csv`

**Quality control:**
Confirm that the number of transformed records matches
the number of validated records.
"""

    postgres_task.operator.doc_md = """
### PostgreSQL Analytics Loading

**Purpose:** Load transformed flight records into the PostgreSQL analytics database.

**Source:**
- `transformed_flight_prices.csv`

**Destination:**
- Database: `flight_analytics`
- Schema: `flight_analytics`
- Table: `transformed_flight_prices`

**Processing:**
1. Verify the transformed CSV exists.
2. Establish a PostgreSQL connection.
3. Convert CSV values into appropriate Python data types.
4. Insert records in batches of 1,000.
5. Skip duplicate source record IDs using `ON CONFLICT DO NOTHING`.
6. Commit the transaction.

**Quality control:**
Confirm that the number of records read matches
the number of records transformed.
"""


flight_price_pipeline()
