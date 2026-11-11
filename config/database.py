
import os

import mysql.connector
import psycopg2
from dotenv import load_dotenv

load_dotenv()


def get_mysql_connection():
    """Connect local Python to the MySQL staging database."""

    return mysql.connector.connect(
        host="localhost",
        port=int(os.getenv("MYSQL_PORT", 3306)),
        database=os.getenv("MYSQL_DATABASE", "flight_staging"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
    )


def get_postgres_connection():
    """Connect local Python to the PostgreSQL analytics database."""

    return psycopg2.connect(
        host="localhost",
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DATABASE", "flight_analytics"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
