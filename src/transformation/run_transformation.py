
import csv
from pathlib import Path

from config.database import get_mysql_connection
from src.transformation.transform import transform_flight_records


PROJECT_ROOT = Path(__file__).resolve().parents[2]

OUTPUT_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "transformed_flight_prices.csv"
)

BATCH_SIZE = 1000

NEW_COLUMNS = [
    "departure_hour",
    "departure_day_name",
    "departure_month",
    "departure_year",
    "is_peak_season",
    "booking_lead_time_group",
    "expected_fare_bdt",
    "fare_discrepancy_bdt",
    "fare_discrepancy_flag",
]


def run_transformation(connection=None, output_path=OUTPUT_PATH):
    """
    Transform MySQL staging records and write a processed CSV.

    A connection can be injected by Airflow.
    If no connection is supplied, use the local database configuration.
    """

    owns_connection = connection is None
    cursor = None

    total_processed = 0
    discrepancy_count = 0

    try:
        print("Starting flight data transformation...")

        if owns_connection:
            connection = get_mysql_connection()

        print("Connected to MySQL successfully.")

        cursor = connection.cursor()
        cursor.execute("SELECT * FROM raw_flight_prices")

        source_columns = [
            column[0] for column in cursor.description
        ]

        fieldnames = source_columns + NEW_COLUMNS

        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with output_path.open(
            mode="w",
            newline="",
            encoding="utf-8",
        ) as output_file:

            writer = csv.DictWriter(
                output_file,
                fieldnames=fieldnames,
            )

            writer.writeheader()

            while True:
                rows = cursor.fetchmany(BATCH_SIZE)

                if not rows:
                    break

                records = [
                    dict(zip(source_columns, row))
                    for row in rows
                ]

                transformed_records = transform_flight_records(
                    records
                )

                writer.writerows(transformed_records)

                batch_discrepancies = sum(
                    record["fare_discrepancy_flag"] is True
                    for record in transformed_records
                )

                discrepancy_count += batch_discrepancies
                total_processed += len(transformed_records)

                print(
                    f"Processed {total_processed:,} records..."
                )

        print("\n========== TRANSFORMATION REPORT ==========")
        print(f"Total records processed: {total_processed:,}")
        print(f"Fare discrepancies: {discrepancy_count:,}")

        percentage = (
            discrepancy_count / total_processed * 100
            if total_processed else 0
        )

        print(f"Discrepancy percentage: {percentage:.2f}%")
        print(f"Output file: {output_path}")
        print("TRANSFORMATION COMPLETED SUCCESSFULLY.")

        return {
            "total_processed": total_processed,
            "discrepancy_count": discrepancy_count,
            "output_path": str(output_path),
        }

    except Exception as error:
        print(f"TRANSFORMATION FAILED: {error}")
        raise

    finally:
        if cursor:
            cursor.close()

        # Only close connections created by this function.
        # Airflow manages injected connections.
        if owns_connection and connection:
            connection.close()
            print("MySQL connection closed.")


if __name__ == "__main__":
    run_transformation()
