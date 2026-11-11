
from decimal import Decimal


def get_scalar(cursor, query):
    """Execute a query and return its first column."""
    cursor.execute(query)
    return cursor.fetchone()[0]


def run_data_quality_checks(connection):
    """
    Run data-quality checks against the MySQL staging table.

    The function does not modify or delete any source records.
    """

    cursor = connection.cursor()

    try:
        results = {}

        # 1. Total number of staged records
        results["total_records"] = get_scalar(
            cursor,
            """
            SELECT COUNT(*)
            FROM raw_flight_prices
            """,
        )

        # 2. Records with missing critical values
        results["missing_values"] = get_scalar(
            cursor,
            """
            SELECT COUNT(*)
            FROM raw_flight_prices
            WHERE airline IS NULL OR TRIM(airline) = ''
               OR source_code IS NULL OR TRIM(source_code) = ''
               OR destination_code IS NULL OR TRIM(destination_code) = ''
               OR departure_datetime IS NULL
               OR arrival_datetime IS NULL
               OR total_fare_bdt IS NULL
            """,
        )

        # 3. Records with negative fares
        results["invalid_fares"] = get_scalar(
            cursor,
            """
            SELECT COUNT(*)
            FROM raw_flight_prices
            WHERE base_fare_bdt < 0
               OR tax_surcharge_bdt < 0
               OR total_fare_bdt < 0
            """,
        )

        # 4. Records with invalid flight durations or timestamps
        results["invalid_durations"] = get_scalar(
            cursor,
            """
            SELECT COUNT(*)
            FROM raw_flight_prices
            WHERE duration_hours <= 0
               OR arrival_datetime <= departure_datetime
            """,
        )

        # 5. Records with fare calculation discrepancies
        results["fare_discrepancies"] = get_scalar(
            cursor,
            """
            SELECT COUNT(*)
            FROM raw_flight_prices
            WHERE ABS(
                total_fare_bdt -
                (base_fare_bdt + tax_surcharge_bdt)
            ) > 0.01
            """,
        )

        # 6. Seasonality distribution
        cursor.execute(
            """
            SELECT seasonality, COUNT(*)
            FROM raw_flight_prices
            GROUP BY seasonality
            ORDER BY COUNT(*) DESC
            """
        )

        results["seasonality_distribution"] = [
            {"seasonality": row[0], "count": row[1]}
            for row in cursor.fetchall()
        ]

        # 7. Calculate fare discrepancy percentage
        total = results["total_records"]

        results["fare_discrepancy_percentage"] = (
            round(
                Decimal(results["fare_discrepancies"])
                / Decimal(total) * Decimal("100"),
                2,
            )
            if total else Decimal("0.00")
        )

        return results

    finally:
        cursor.close()


def print_quality_report(results):
    """Print a readable data-quality report."""

    print("\n========== DATA QUALITY REPORT ==========")

    print(f"Total records: {results['total_records']}")
    print(f"Missing critical values: {results['missing_values']}")
    print(f"Invalid fare records: {results['invalid_fares']}")
    print(f"Invalid duration records: {results['invalid_durations']}")

    print("\n--- Fare Reconciliation ---")
    print(f"Discrepancies: {results['fare_discrepancies']}")
    print(
        "Discrepancy percentage: "
        f"{results['fare_discrepancy_percentage']}%"
    )

    print("\n--- Seasonality Distribution ---")

    for item in results["seasonality_distribution"]:
        print(
            f"{item['seasonality']}: "
            f"{item['count']} records"
        )

    print("\n=========================================")
