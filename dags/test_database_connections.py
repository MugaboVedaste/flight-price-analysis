
import pendulum

from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(
    dag_id="test_database_connections",
    description="Test MySQL and PostgreSQL connectivity",
    start_date=pendulum.datetime(2026, 11, 10, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["flight-price-analysis", "testing"],
)
def test_database_connections():

    @task
    def test_mysql():
        hook = MySqlHook(mysql_conn_id="mysql_flight_staging")

        result = hook.get_first(
            "SELECT DATABASE(), VERSION()"
        )

        print(f"MySQL connection successful: {result}")

    @task
    def test_postgres():
        hook = PostgresHook(
            postgres_conn_id="postgres_flight_analytics"
        )

        result = hook.get_first(
            "SELECT current_database(), version()"
        )

        print(f"PostgreSQL connection successful: {result}")

    test_mysql()
    test_postgres()


test_database_connections()
