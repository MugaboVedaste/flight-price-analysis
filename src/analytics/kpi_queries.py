"""
KPI queries for the flight price analytics database.

Required KPIs
-------------
1. Average Fare by Airline   -> get_average_fare_by_airline
2. Seasonal Fare Variation   -> get_seasonal_fare_variation (per season)
                                get_peak_vs_non_peak_fare   (roll-up)
3. Booking Count by Airline  -> get_booking_count_by_airline
4. Most Popular Routes       -> get_most_popular_routes

Extra KPI
---------
- Fare by booking lead time  -> get_fare_by_booking_lead_time

Every "booking" is one row (one flight booking record) in
flight_analytics.transformed_flight_prices. Fares are `total_fare_bdt`.
"""

from decimal import Decimal

from config.database import get_postgres_connection


TABLE = "flight_analytics.transformed_flight_prices"


def _get_connection(connection):
    """
    Use the supplied connection or create a local PostgreSQL connection.

    Returns:
        tuple: (connection, should_close)
    """
    if connection is not None:
        return connection, False

    return get_postgres_connection(), True


def _fetch_dicts(connection, query, params=None):
    """Run a query and return the rows as a list of dictionaries."""

    connection, should_close = _get_connection(connection)

    try:
        with connection.cursor() as cursor:
            cursor.execute(query, params)

            columns = [column[0] for column in cursor.description]

            return [
                dict(zip(columns, row))
                for row in cursor.fetchall()
            ]

    finally:
        if should_close:
            connection.close()


def _percentage_difference(value, baseline):
    """Percentage difference of value vs baseline (None if undefined)."""

    if value is None or baseline is None or baseline == 0:
        return None

    difference = round((value - baseline) / baseline * Decimal("100"), 2)

    # Adding zero turns a rounded "-0.00" into "0.00".
    return difference + Decimal("0.00")


# ------------------------------------------------------------------
# KPI 1: Average fare by airline
# ------------------------------------------------------------------

def get_average_fare_by_airline(connection=None):
    """
    Mean total fare grouped by airline.

    Returns:
        list[dict]: airline, booking_count, average/min/max fare (BDT).
    """

    query = f"""
        SELECT
            airline,
            COUNT(*) AS booking_count,
            ROUND(AVG(total_fare_bdt), 2) AS average_fare_bdt,
            ROUND(MIN(total_fare_bdt), 2) AS minimum_fare_bdt,
            ROUND(MAX(total_fare_bdt), 2) AS maximum_fare_bdt
        FROM {TABLE}
        GROUP BY airline
        ORDER BY average_fare_bdt DESC, airline;
    """

    return _fetch_dicts(connection, query)


# ------------------------------------------------------------------
# KPI 2: Seasonal fare variation
# ------------------------------------------------------------------

def get_seasonal_fare_variation(connection=None):
    """
    Average fare for every season (Eid, Hajj, Winter Holidays, Regular)
    with its variation against the non-peak (Regular) average.

    Peak seasons are defined in src/transformation/transform.py
    (PEAK_SEASONS): Eid, Hajj and Winter Holidays.

    Returns:
        list[dict]: one row per season.
    """

    query = f"""
        SELECT
            seasonality AS season,
            is_peak_season,
            COUNT(*) AS booking_count,
            ROUND(AVG(total_fare_bdt), 2) AS average_fare_bdt,
            SUM(total_fare_bdt) AS fare_sum
        FROM {TABLE}
        GROUP BY seasonality, is_peak_season
        ORDER BY average_fare_bdt DESC, seasonality;
    """

    rows = _fetch_dicts(connection, query)

    non_peak_total = sum(
        row["fare_sum"]
        for row in rows
        if row["is_peak_season"] is False
    )
    non_peak_count = sum(
        row["booking_count"]
        for row in rows
        if row["is_peak_season"] is False
    )

    baseline = (
        non_peak_total / non_peak_count if non_peak_count else None
    )

    for row in rows:
        del row["fare_sum"]
        row["variation_vs_non_peak_pct"] = _percentage_difference(
            row["average_fare_bdt"], baseline
        )

    return rows


def get_peak_vs_non_peak_fare(connection=None):
    """
    Compare average total fare in peak and non-peak seasons.

    Returns:
        dict: counts, average fares, absolute and percentage difference.
        Differences are None when either group has no records.
    """

    query = f"""
        SELECT
            COUNT(*) FILTER (
                WHERE is_peak_season = TRUE
            ) AS peak_flight_count,

            COUNT(*) FILTER (
                WHERE is_peak_season = FALSE
            ) AS non_peak_flight_count,

            ROUND(
                AVG(total_fare_bdt) FILTER (
                    WHERE is_peak_season = TRUE
                ), 2
            ) AS peak_average_fare_bdt,

            ROUND(
                AVG(total_fare_bdt) FILTER (
                    WHERE is_peak_season = FALSE
                ), 2
            ) AS non_peak_average_fare_bdt

        FROM {TABLE};
    """

    row = _fetch_dicts(connection, query)[0]

    peak_avg = row["peak_average_fare_bdt"]
    non_peak_avg = row["non_peak_average_fare_bdt"]

    can_compare = peak_avg is not None and non_peak_avg is not None

    return {
        "peak_flight_count": row["peak_flight_count"],
        "non_peak_flight_count": row["non_peak_flight_count"],
        "peak_average_fare_bdt": peak_avg,
        "non_peak_average_fare_bdt": non_peak_avg,
        "fare_difference_bdt": (
            round(peak_avg - non_peak_avg, 2) if can_compare else None
        ),
        "percentage_difference": _percentage_difference(
            peak_avg, non_peak_avg
        ),
    }


# ------------------------------------------------------------------
# KPI 3: Booking count by airline
# ------------------------------------------------------------------

def get_booking_count_by_airline(connection=None):
    """
    Total number of bookings per airline, with share of all bookings.

    Returns:
        list[dict]: airline, booking_count, percentage_of_total.
    """

    query = f"""
        SELECT
            airline,
            COUNT(*) AS booking_count,
            ROUND(
                COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (),
                2
            ) AS percentage_of_total
        FROM {TABLE}
        GROUP BY airline
        ORDER BY booking_count DESC, airline;
    """

    return _fetch_dicts(connection, query)


# ------------------------------------------------------------------
# KPI 4: Most popular routes
# ------------------------------------------------------------------

def get_most_popular_routes(limit=15, connection=None):
    """
    Top source-destination pairs ranked by booking count.

    Args:
        limit (int): Maximum number of routes to return.

    Returns:
        list[dict]: rank, route, booking_count, average fare.
    """

    if not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0:
        raise ValueError("limit must be a positive integer.")

    query = f"""
        SELECT
            RANK() OVER (ORDER BY COUNT(*) DESC) AS route_rank,
            source_code,
            source_name,
            destination_code,
            destination_name,
            COUNT(*) AS booking_count,
            ROUND(AVG(total_fare_bdt), 2) AS average_fare_bdt
        FROM {TABLE}
        GROUP BY
            source_code,
            source_name,
            destination_code,
            destination_name
        ORDER BY booking_count DESC, source_code, destination_code
        LIMIT %s;
    """

    return _fetch_dicts(connection, query, (limit,))


# ------------------------------------------------------------------
# Extra KPI: fare by booking lead time
# ------------------------------------------------------------------

def get_fare_by_booking_lead_time(connection=None):
    """
    Analyze fares across booking lead-time categories.

    Returns:
        list[dict]: booking counts and fare statistics by lead-time group.
    """

    query = f"""
        SELECT
            booking_lead_time_group,
            COUNT(*) AS booking_count,
            ROUND(AVG(total_fare_bdt), 2) AS average_fare_bdt,
            ROUND(MIN(total_fare_bdt), 2) AS minimum_fare_bdt,
            ROUND(MAX(total_fare_bdt), 2) AS maximum_fare_bdt
        FROM {TABLE}
        GROUP BY booking_lead_time_group
        ORDER BY
            CASE booking_lead_time_group
                WHEN '0-3 days' THEN 1
                WHEN '4-7 days' THEN 2
                WHEN '8-14 days' THEN 3
                WHEN '15-30 days' THEN 4
                WHEN '31-60 days' THEN 5
                WHEN '61+ days' THEN 6
                ELSE 7
            END;
    """

    return _fetch_dicts(connection, query)
