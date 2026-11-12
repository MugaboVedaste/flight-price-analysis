
from src.analytics.kpi_queries import (
    get_average_fare_by_airline,
    get_peak_vs_non_peak_fare,
    get_flight_count_by_airline,
    get_most_popular_routes,
    get_fare_by_booking_lead_time,
)


def main(connection=None):
    """Execute all flight price analytics KPIs."""

    results = {
        "average_fare_by_airline": get_average_fare_by_airline(connection=connection),
        "peak_vs_non_peak_fare": get_peak_vs_non_peak_fare(connection=connection),
        "flight_count_by_airline": get_flight_count_by_airline(connection=connection),
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

    print("\n========== ANALYTICS COMPLETED ==========\n")

    return results


if __name__ == "__main__":
    main()
