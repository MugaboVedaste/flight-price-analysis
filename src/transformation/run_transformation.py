
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


def run_transformation():
    connection = None
    cursor = None

    total_processed = 0
    discrepancy_count = 0

    try:
        print("Starting flight data transformation...")

        connection = get_mysql_connection()
        print("Connected to MySQL successfully.")

        cursor = connection.cursor(dictionary=True)

        cursor.execute("SELECT * FROM raw_flight_prices")

        source_columns = [
            column[0] for column in cursor.description
        ]

        fieldnames = source_columns + NEW_COLUMNS

        OUTPUT_PATH.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        with open(
            OUTPUT_PATH,
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
                records = cursor.fetchmany(BATCH_SIZE)

                if not records:
                    break

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
        print(
            f"Discrepancy percentage: "
            f"{(discrepancy_count / total_processed * 100):.2f}%"
            if total_processed else "Discrepancy percentage: N/A"
        )
        print(f"Output file: {OUTPUT_PATH}")
        print("TRANSFORMATION COMPLETED SUCCESSFULLY.")

    except Exception as error:
        print(f"TRANSFORMATION FAILED: {error}")
        raise

    finally:
        if cursor:
            cursor.close()

        if connection and connection.is_connected():
            connection.close()
            print("MySQL connection closed.")


if __name__ == "__main__":
    run_transformation()
