
from decimal import Decimal


# Columns the specification requires (staging-table names).
# CSV name -> staging column:
#   Airline -> airline, Source -> source_code,
#   Destination -> destination_code, Base Fare -> base_fare_bdt,
#   Tax & Surcharge -> tax_surcharge_bdt, Total Fare -> total_fare_bdt
REQUIRED_COLUMNS = (
    "airline",
    "source_code",
    "destination_code",
    "base_fare_bdt",
    "tax_surcharge_bdt",
    "total_fare_bdt",
)

# Valid airport codes (IATA) that appear in the Bangladesh flight dataset:
# domestic airports plus the international destinations served.
VALID_AIRPORT_CODES = frozenset(
    {
        # Bangladesh
        "BZL", "CGP", "CXB", "DAC", "JSR", "RJH", "SPD", "ZYL",
        # International
        "BKK", "CCU", "DEL", "DOH", "DXB", "IST", "JED", "JFK",
        "KUL", "LHR", "SIN", "YYZ",
    }
)

# If more than this share of CSV rows had to be rejected at ingestion,
# the load is considered unreliable and the pipeline fails.
MAX_REJECTED_PERCENTAGE = Decimal("5")

# Checks that stop the pipeline when they are greater than zero.
CRITICAL_CHECKS = (
    "missing_required_columns",
    "missing_values",
    "invalid_fares",
    "invalid_durations",
    "invalid_cities",
)


def get_scalar(cursor, query, params=None):
    """Execute a query and return its first column."""
    cursor.execute(query, params) if params else cursor.execute(query)
    return cursor.fetchone()[0]


def _in_list_sql(codes):
    """Render a SQL IN (...) list from trusted, hard-coded codes."""
    return ", ".join(f"'{code}'" for code in sorted(codes))


def _table_exists(cursor, table_name):
    return get_scalar(
        cursor,
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = DATABASE()
          AND table_name = %s
        """,
        (table_name,),
    ) > 0


def find_missing_required_columns(cursor):
    """Return the required columns absent from the staging table."""

    cursor.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'raw_flight_prices'
        """
    )

    existing = {str(row[0]).lower() for row in cursor.fetchall()}

    return [
        column
        for column in REQUIRED_COLUMNS
        if column not in existing
    ]


def run_data_quality_checks(connection):
    """
    Run data-quality checks against the MySQL staging table.

    The function does not modify or delete any source records.

    Checks
    ------
    missing_required_columns  required columns absent from the table
    missing_values            NULL/blank values in required fields
    invalid_fares             negative fares
    invalid_durations         non-positive duration or arrival <= departure
    invalid_cities            unknown airport code, or source == destination
    fare_discrepancies        total != base + tax (warning, rows preserved)
    empty_optional_fields     blank descriptive text fields (warning)
    rejected_rows             CSV rows rejected at ingestion (warning,
                              critical above MAX_REJECTED_PERCENTAGE)
    """

    cursor = connection.cursor()

    try:
        results = {}

        # 0. Required columns exist
        missing_columns = find_missing_required_columns(cursor)
        results["missing_required_column_names"] = missing_columns
        results["missing_required_columns"] = len(missing_columns)

        if missing_columns:
            # The remaining checks reference these columns.
            results["total_records"] = get_scalar(
                cursor,
                "SELECT COUNT(*) FROM raw_flight_prices",
            )
            return results

        # 1. Total number of staged records
        results["total_records"] = get_scalar(
            cursor,
            """
            SELECT COUNT(*)
            FROM raw_flight_prices
            """,
        )

        # 2. Missing / blank values in required fields.
        #    Fares, dates and categorical fields must be present and
        #    categorical text must not be empty.
        results["missing_values"] = get_scalar(
            cursor,
            """
            SELECT COUNT(*)
            FROM raw_flight_prices
            WHERE airline IS NULL OR TRIM(airline) = ''
               OR source_code IS NULL OR TRIM(source_code) = ''
               OR destination_code IS NULL OR TRIM(destination_code) = ''
               OR seasonality IS NULL OR TRIM(seasonality) = ''
               OR departure_datetime IS NULL
               OR arrival_datetime IS NULL
               OR base_fare_bdt IS NULL
               OR tax_surcharge_bdt IS NULL
               OR total_fare_bdt IS NULL
            """,
        )

        # 2b. Blank descriptive text (warning only)
        results["empty_optional_fields"] = get_scalar(
            cursor,
            """
            SELECT COUNT(*)
            FROM raw_flight_prices
            WHERE source_name IS NULL OR TRIM(source_name) = ''
               OR destination_name IS NULL OR TRIM(destination_name) = ''
               OR stopovers IS NULL OR TRIM(stopovers) = ''
               OR aircraft_type IS NULL OR TRIM(aircraft_type) = ''
               OR booking_class IS NULL OR TRIM(booking_class) = ''
               OR booking_source IS NULL OR TRIM(booking_source) = ''
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

        # 5. Invalid city / airport codes
        valid_codes = _in_list_sql(VALID_AIRPORT_CODES)

        results["invalid_cities"] = get_scalar(
            cursor,
            f"""
            SELECT COUNT(*)
            FROM raw_flight_prices
            WHERE UPPER(TRIM(source_code)) NOT IN ({valid_codes})
               OR UPPER(TRIM(destination_code)) NOT IN ({valid_codes})
               OR UPPER(TRIM(source_code)) = UPPER(TRIM(destination_code))
            """,
        )

        # 6. Records with fare calculation discrepancies
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

        # 7. Rows rejected at ingestion (malformed numbers / dates)
        results["rejected_rows"] = (
            get_scalar(
                cursor,
                "SELECT COUNT(*) FROM rejected_flight_rows",
            )
            if _table_exists(cursor, "rejected_flight_rows")
            else 0
        )

        # 8. Seasonality distribution
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

        # 9. Percentages
        total = results["total_records"]
        rows_seen = total + results["rejected_rows"]

        results["fare_discrepancy_percentage"] = (
            round(
                Decimal(results["fare_discrepancies"])
                / Decimal(total) * Decimal("100"),
                2,
            )
            if total else Decimal("0.00")
        )

        results["rejected_percentage"] = (
            round(
                Decimal(results["rejected_rows"])
                / Decimal(rows_seen) * Decimal("100"),
                2,
            )
            if rows_seen else Decimal("0.00")
        )

        return results

    finally:
        cursor.close()


def get_failed_checks(results):
    """
    Return the names of critical checks that failed.

    Includes the rejected-row check when too many CSV rows were
    rejected at ingestion.
    """

    failed = [
        check
        for check in CRITICAL_CHECKS
        if results.get(check, 0) > 0
    ]

    if (
        Decimal(str(results.get("rejected_percentage", 0)))
        > MAX_REJECTED_PERCENTAGE
    ):
        failed.append("rejected_rows")

    return failed


def print_quality_report(results):
    """Print a readable data-quality report."""

    print("\n========== DATA QUALITY REPORT ==========")

    print(f"Total records: {results['total_records']}")

    if results.get("missing_required_columns"):
        print(
            "Missing required columns: "
            f"{results['missing_required_column_names']}"
        )
        print("\n=========================================")
        return

    print("Required columns present: yes")
    print(f"Missing critical values: {results['missing_values']}")
    print(f"Invalid fare records: {results['invalid_fares']}")
    print(f"Invalid duration records: {results['invalid_durations']}")
    print(f"Invalid city/airport records: {results['invalid_cities']}")

    print("\n--- Warnings ---")
    print(
        "Records with blank descriptive fields: "
        f"{results['empty_optional_fields']}"
    )
    print(
        "Rows rejected at ingestion: "
        f"{results['rejected_rows']} "
        f"({results['rejected_percentage']}%)"
    )

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
