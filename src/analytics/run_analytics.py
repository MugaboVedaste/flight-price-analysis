
from src.analytics.kpi_queries import (
    get_average_fare_by_airline,
    get_seasonal_fare_variation,
    get_peak_vs_non_peak_fare,
    get_booking_count_by_airline,
    get_most_popular_routes,
    get_fare_by_booking_lead_time,
)
from src.analytics.kpi_storage import store_kpis
from config.database import get_postgres_connection


def main(connection=None, store=True):
    """
    Compute all flight price KPIs and (by default) store them in PostgreSQL.

    A connection can be injected by Airflow. If none is supplied, a local
    connection is opened (and closed) here.

    Returns:
        dict: KPI results. When stored, includes "stored_rows".
    """

    owns_connection = connection is None

    if owns_connection:
        connection = get_postgres_connection()

    try:
        results = {
            "average_fare_by_airline": get_average_fare_by_airline(connection=connection),
            "seasonal_fare_variation": get_seasonal_fare_variation(connection=connection),
            "peak_vs_non_peak_fare": get_peak_vs_non_peak_fare(connection=connection),
            "booking_count_by_airline": get_booking_count_by_airline(connection=connection),
            "most_popular_routes": get_most_popular_routes(connection=connection),
            "fare_by_booking_lead_time": get_fare_by_booking_lead_time(connection=connection),
        }

        print("\n========== FLIGHT PRICE ANALYTICS ==========\n")

        for kpi_name, kpi_results in results.items():
            print(f"\n{kpi_name.upper().replace('_', ' ')}")

            if isinstance(kpi_results, list):
                for result in kpi_results:
                    print(result)
            else:
                print(kpi_results)

        if store:
            results["stored_rows"] = store_kpis(connection, results)
            print(f"\nKPI tables stored: {results['stored_rows']}")

        print("\n========== ANALYTICS COMPLETED ==========\n")

        return results

    finally:
        if owns_connection:
            connection.close()


if __name__ == "__main__":
    main()
