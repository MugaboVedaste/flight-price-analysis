
from pathlib import Path

from config.database import get_mysql_connection
from src.ingestion.mysql_loader import load_csv_to_mysql


# Project root directory
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Source CSV file
CSV_PATH = PROJECT_ROOT / "data" / "Flight_Price_Dataset_of_Bangladesh.csv"


def main():
    connection = None

    try:
        print("Starting flight data ingestion...")
        print(f"Source file: {CSV_PATH}")

        connection = get_mysql_connection()

        print("Connected to MySQL successfully.")

        loaded_records = load_csv_to_mysql(
            csv_path=CSV_PATH,
            connection=connection,
            batch_size=1000,
        )

        # Verify the records in MySQL
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM raw_flight_prices")
        database_count = cursor.fetchone()[0]
        cursor.close()

        print("\n========== INGESTION SUMMARY ==========")
        print(f"Records loaded: {loaded_records}")
        print(f"Records in MySQL: {database_count}")

        if loaded_records != database_count:
            raise RuntimeError(
                "Record count mismatch between loader and database."
            )

        print("INGESTION COMPLETED SUCCESSFULLY.")

    except Exception as error:
        print(f"INGESTION FAILED: {error}")
        raise

    finally:
        if connection and connection.is_connected():
            connection.close()
            print("MySQL connection closed.")


if __name__ == "__main__":
    main()
