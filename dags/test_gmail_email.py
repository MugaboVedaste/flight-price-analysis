
from datetime import datetime

from airflow.sdk import dag, task
from airflow.providers.smtp.utils.emailer import send_email_smtp
from airflow.providers.smtp.notifications.smtp import SmtpNotifier

import os


@dag(
    dag_id="test_gmail_email",
    description="Test Gmail SMTP email delivery",
    start_date=datetime(2026, 11, 11),
    schedule=None,
    catchup=False,
    tags=["testing", "email"],
)
def test_gmail_email():

    @task
    def send_test_email():
        recipient = os.getenv("GMAIL_ADDRESS")

        if not recipient:
            raise ValueError("GMAIL_ADDRESS is not configured.")

        send_email_smtp(
            to=recipient,
            subject="Airflow Gmail SMTP Test",
            html_content="""
                <h2>Airflow SMTP Test Successful</h2>
                <p>This is a test email sent from the Flight Price Analysis
                Airflow environment.</p>
            """,
        )

        return "Test email sent."

    send_test_email()


test_gmail_email()
