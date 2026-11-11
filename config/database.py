
import os

import mysql.connector
from dotenv import load_dotenv

# Load environment variables from project root.
load_dotenv()


def get_mysql_connection():
    """Create a connection to the MySQL staging database."""

    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", 3306)),
        database=os.getenv("MYSQL_DATABASE", "flight_staging"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
    )
