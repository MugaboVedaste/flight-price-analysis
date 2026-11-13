
from config.database import get_mysql_connection

from src.validation.data_quality import (
    run_data_quality_checks,
    print_quality_report,
    get_failed_checks,
)


def main():
    connection = None

    try:
        print("Starting data-quality validation...")

        connection = get_mysql_connection()

        print("Connected to MySQL successfully.")

        results = run_data_quality_checks(connection)

        print_quality_report(results)

        # Critical checks must pass.
        failed_checks = get_failed_checks(results)

        if failed_checks:
            raise RuntimeError(
                f"Critical data-quality checks failed: {failed_checks}"
            )

        print("\nDATA QUALITY VALIDATION PASSED.")

        if results.get("fare_discrepancies", 0) > 0:
            print(
                "WARNING: Fare discrepancies detected. "
                "Original fares have been preserved."
            )

    finally:
        if connection is not None and connection.is_connected():
            connection.close()
            print("MySQL connection closed.")


if __name__ == "__main__":
    main()
