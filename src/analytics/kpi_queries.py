from config.database import get_postgres_connection


def get_average_fare_by_airline():
    """
    Calculate average, minimum, and maximum flight fares per airline.

    Returns:
        list[dict]: Fare statistics grouped by airline.
    """

    query = """
        SELECT
            airline,
            COUNT(*) AS flight_count,
            ROUND(AVG(total_fare_bdt), 2) AS average_fare_bdt,
            ROUND(MIN(total_fare_bdt), 2) AS minimum_fare_bdt,
            ROUND(MAX(total_fare_bdt), 2) AS maximum_fare_bdt
        FROM flight_analytics.transformed_flight_prices
        GROUP BY airline
        ORDER BY average_fare_bdt DESC;
    """

    connection = get_postgres_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(query)

            columns = [column[0] for column in cursor.description]
            rows = cursor.fetchall()

            return [
                dict(zip(columns, row))
                for row in rows
            ]

    finally:
        connection.close()


def get_peak_vs_non_peak_fare():
    """
    Compare average flight fares between peak and non-peak seasons.

    Returns:
        dict: Flight counts, average fares, and percentage difference.
    """

    query = """
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

        FROM flight_analytics.transformed_flight_prices;
    """

    connection = get_postgres_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            row = cursor.fetchone()

            peak_count, non_peak_count, peak_avg, non_peak_avg = row

            fare_difference = peak_avg - non_peak_avg

            percentage_difference = (
                fare_difference / non_peak_avg
            ) * 100

            return {
                "peak_flight_count": peak_count,
                "non_peak_flight_count": non_peak_count,
                "peak_average_fare_bdt": peak_avg,
                "non_peak_average_fare_bdt": non_peak_avg,
                "fare_difference_bdt": round(fare_difference, 2),
                "percentage_difference": round(percentage_difference, 2),
            }

    finally:
        connection.close()


def get_flight_count_by_airline():
    """
    Calculate flight record counts and percentage distribution by airline.

    Returns:
        list[dict]: Flight counts and percentages grouped by airline.
    """

    query = """
        SELECT
            airline,
            COUNT(*) AS flight_count,
            ROUND(
                COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (),
                2
            ) AS percentage_of_total
        FROM flight_analytics.transformed_flight_prices
        GROUP BY airline
        ORDER BY flight_count DESC;
    """

    connection = get_postgres_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(query)

            columns = [column[0] for column in cursor.description]
            rows = cursor.fetchall()

            return [
                dict(zip(columns, row))
                for row in rows
            ]

    finally:
        connection.close()


def get_most_popular_routes(limit=15):
    """
    Identify the most frequently recorded source-destination routes.

    Args:
        limit (int): Maximum number of routes to return.

    Returns:
        list[dict]: Route counts and average fares.
    """

    if not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0:
        raise ValueError("limit must be a positive integer.")

    query = """
        SELECT
            source_code,
            source_name,
            destination_code,
            destination_name,
            COUNT(*) AS flight_count,
            ROUND(AVG(total_fare_bdt), 2) AS average_fare_bdt
        FROM flight_analytics.transformed_flight_prices
        GROUP BY
            source_code,
            source_name,
            destination_code,
            destination_name
        ORDER BY flight_count DESC
        LIMIT %s;
    """

    connection = get_postgres_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(query, (limit,))

            columns = [column[0] for column in cursor.description]
            rows = cursor.fetchall()

            return [
                dict(zip(columns, row))
                for row in rows
            ]

    finally:
        connection.close()



def get_fare_by_booking_lead_time():
    """
    Analyze flight fares across booking lead-time categories.

    Returns:
        list[dict]: Flight counts and fare statistics by lead-time group.
    """

    query = """
        SELECT
            booking_lead_time_group,
            COUNT(*) AS flight_count,
            ROUND(AVG(total_fare_bdt), 2) AS average_fare_bdt,
            ROUND(MIN(total_fare_bdt), 2) AS minimum_fare_bdt,
            ROUND(MAX(total_fare_bdt), 2) AS maximum_fare_bdt
        FROM flight_analytics.transformed_flight_prices
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

    connection = get_postgres_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(query)

            columns = [column[0] for column in cursor.description]
            rows = cursor.fetchall()

            return [
                dict(zip(columns, row))
                for row in rows
            ]

    finally:
        connection.close()
