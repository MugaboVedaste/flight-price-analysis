
from config.database import get_postgres_connection
from src.transformation.transform import generate_record_hash


BATCH_SIZE = 1000

BUSINESS_COLUMNS = (
    "airline",
    "source_code",
    "source_name",
    "destination_code",
    "destination_name",
    "departure_datetime",
    "arrival_datetime",
    "duration_hours",
    "stopovers",
    "aircraft_type",
    "booking_class",
    "booking_source",
    "base_fare_bdt",
    "tax_surcharge_bdt",
    "total_fare_bdt",
    "seasonality",
    "days_before_departure",
)


def backfill_record_hashes():
    """
    Generate and populate stable hashes for existing PostgreSQL records.
    """

    connection = get_postgres_connection()

    read_cursor = None
    write_cursor = None

    total_updated = 0

    try:
        print("Starting record hash migration...")

        read_cursor = connection.cursor()
        write_cursor = connection.cursor()

        columns_sql = ", ".join(BUSINESS_COLUMNS)

        read_cursor.execute(
            f"""
            SELECT id, {columns_sql}
            FROM flight_analytics.transformed_flight_prices
            WHERE record_hash IS NULL
            ORDER BY id
            """
        )

        while True:
            rows = read_cursor.fetchmany(BATCH_SIZE)

            if not rows:
                break

            updates = []

            for row in rows:
                record_id = row[0]

                record = dict(
                    zip(BUSINESS_COLUMNS, row[1:])
                )

                record_hash = generate_record_hash(record)

                updates.append((record_hash, record_id))

            write_cursor.executemany(
                """
                UPDATE flight_analytics.transformed_flight_prices
                SET record_hash = %s
                WHERE id = %s
                  AND record_hash IS NULL
                """,
                updates,
            )

            total_updated += write_cursor.rowcount

            print(
                f"Updated {total_updated:,} records..."
            )

        # Verify all records received a hash.
        write_cursor.execute(
            """
            SELECT COUNT(*)
            FROM flight_analytics.transformed_flight_prices
            WHERE record_hash IS NULL
            """
        )

        remaining_nulls = write_cursor.fetchone()[0]

        if remaining_nulls != 0:
            raise RuntimeError(
                f"{remaining_nulls} records still have NULL hashes."
            )

        connection.commit()

        print("\n========== HASH MIGRATION REPORT ==========")
        print(f"Records updated: {total_updated:,}")
        print(f"Records without hashes: {remaining_nulls:,}")
        print("HASH MIGRATION COMPLETED SUCCESSFULLY.")

        return total_updated

    except Exception as error:
        connection.rollback()
        print(f"HASH MIGRATION FAILED: {error}")
        raise

    finally:
        if read_cursor:
            read_cursor.close()

        if write_cursor:
            write_cursor.close()

        connection.close()


if __name__ == "__main__":
    backfill_record_hashes()
