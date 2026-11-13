"""
Persist KPI results into PostgreSQL.

The KPI tables are defined in sql/kpi_schema.sql (the single source of
truth). They are emptied and refilled in ONE transaction, so a failed
run leaves the previous results untouched and readers never see a
partial result.
"""

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]

KPI_SCHEMA_PATH = PROJECT_ROOT / "sql" / "kpi_schema.sql"

SCHEMA = "flight_analytics"

# result key -> (table, columns)
KPI_TABLES = {
    "average_fare_by_airline": (
        "kpi_avg_fare_by_airline",
        (
            "airline",
            "booking_count",
            "average_fare_bdt",
            "minimum_fare_bdt",
            "maximum_fare_bdt",
        ),
    ),
    "seasonal_fare_variation": (
        "kpi_seasonal_fare_variation",
        (
            "season",
            "is_peak_season",
            "booking_count",
            "average_fare_bdt",
            "variation_vs_non_peak_pct",
        ),
    ),
    "booking_count_by_airline": (
        "kpi_booking_count_by_airline",
        ("airline", "booking_count", "percentage_of_total"),
    ),
    "most_popular_routes": (
        "kpi_popular_routes",
        (
            "route_rank",
            "source_code",
            "source_name",
            "destination_code",
            "destination_name",
            "booking_count",
            "average_fare_bdt",
        ),
    ),
    "fare_by_booking_lead_time": (
        "kpi_fare_by_booking_lead_time",
        (
            "booking_lead_time_group",
            "booking_count",
            "average_fare_bdt",
            "minimum_fare_bdt",
            "maximum_fare_bdt",
        ),
    ),
}

PEAK_TABLE = "kpi_peak_vs_non_peak_fare"

# (table column, key in the peak-vs-non-peak result dict)
PEAK_COLUMNS = (
    ("peak_booking_count", "peak_flight_count"),
    ("non_peak_booking_count", "non_peak_flight_count"),
    ("peak_average_fare_bdt", "peak_average_fare_bdt"),
    ("non_peak_average_fare_bdt", "non_peak_average_fare_bdt"),
    ("fare_difference_bdt", "fare_difference_bdt"),
    ("percentage_difference", "percentage_difference"),
)


def _insert_sql(table, columns):
    return (
        f"INSERT INTO {SCHEMA}.{table} ({', '.join(columns)}) "
        f"VALUES ({', '.join(['%s'] * len(columns))})"
    )


def store_kpis(connection, results):
    """
    Replace the stored KPI tables with `results`.

    Args:
        connection: open PostgreSQL connection (not closed here).
        results: dict returned by src.analytics.run_analytics.main().

    Returns:
        dict: number of rows stored per KPI table.
    """

    schema_sql = KPI_SCHEMA_PATH.read_text(encoding="utf-8")

    stored = {}
    cursor = connection.cursor()

    try:
        # CREATE ... IF NOT EXISTS keeps first runs working even if the
        # schema script was never applied by hand.
        cursor.execute(schema_sql)

        for result_key, (table, columns) in KPI_TABLES.items():
            rows = results[result_key]

            cursor.execute(f"DELETE FROM {SCHEMA}.{table}")

            if rows:
                cursor.executemany(
                    _insert_sql(table, columns),
                    [tuple(row[col] for col in columns) for row in rows],
                )

            stored[table] = len(rows)

        # Single-row peak vs non-peak roll-up.
        peak = results["peak_vs_non_peak_fare"]
        columns = tuple(target for target, _ in PEAK_COLUMNS)

        cursor.execute(f"DELETE FROM {SCHEMA}.{PEAK_TABLE}")
        cursor.execute(
            _insert_sql(PEAK_TABLE, columns),
            tuple(peak[source] for _, source in PEAK_COLUMNS),
        )
        stored[PEAK_TABLE] = 1

        connection.commit()

        return stored

    except Exception:
        connection.rollback()
        raise

    finally:
        cursor.close()
