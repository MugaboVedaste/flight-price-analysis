
import csv
from decimal import Decimal

import pytest

from src.ingestion import mysql_loader


HEADER = list(mysql_loader.COLUMN_MAPPING)


def good_row(**overrides):
    row = {
        "Airline": " Biman Bangladesh ",
        "Source": "DAC",
        "Source Name": "Hazrat Shahjalal",
        "Destination": "CXB",
        "Destination Name": "Cox's Bazar",
        "Departure Date & Time": "2025-11-17 06:25:00",
        "Arrival Date & Time": "2025-11-17 07:38:10",
        "Duration (hrs)": "1.2195264539377717",
        "Stopovers": "Direct",
        "Aircraft Type": "Airbus A320",
        "Class": "Economy",
        "Booking Source": "Online Website",
        "Base Fare (BDT)": "21131.22",
        "Tax & Surcharge (BDT)": "5169.68",
        "Total Fare (BDT)": "26300.90",
        "Seasonality": "Regular",
        "Days Before Departure": "10",
    }
    row.update(overrides)
    return row


class FakeCursor:
    def __init__(self):
        self.executed = []
        self.inserted_batches = []

    def execute(self, sql, params=None):
        self.executed.append((" ".join(sql.split()), params))

    def executemany(self, sql, batch):
        self.inserted_batches.append(list(batch))

    def close(self):
        pass


class FakeConnection:
    def __init__(self):
        self.cursor_obj = FakeCursor()
        self.committed = False
        self.rolled_back = False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


def write_csv(path, rows, header=HEADER):
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)


def test_convert_row_parses_types_and_strips_text():
    converted = mysql_loader.convert_row(good_row())

    assert converted[0] == "Biman Bangladesh"
    assert converted[7] == Decimal("1.2195264539377717")
    assert converted[12] == Decimal("21131.22")
    assert converted[16] == 10


@pytest.mark.parametrize(
    "column, value",
    [
        ("Base Fare (BDT)", ""),
        ("Total Fare (BDT)", "abc"),
        ("Tax & Surcharge (BDT)", "NaN"),
        ("Departure Date & Time", "not a date"),
        ("Arrival Date & Time", ""),
        ("Days Before Departure", "ten"),
    ],
)
def test_convert_row_rejects_bad_values(column, value):
    with pytest.raises(ValueError):
        mysql_loader.convert_row(good_row(**{column: value}))


def test_negative_fare_is_loaded_and_left_for_validation():
    converted = mysql_loader.convert_row(
        good_row(**{"Base Fare (BDT)": "-100"})
    )

    assert converted[12] == Decimal("-100")


def test_load_skips_bad_rows_and_records_them(tmp_path):
    csv_path = tmp_path / "flights.csv"
    write_csv(
        csv_path,
        [
            good_row(),
            good_row(**{"Base Fare (BDT)": ""}),
            good_row(),
        ],
    )
    connection = FakeConnection()

    loaded = mysql_loader.load_csv_to_mysql(
        csv_path, connection, batch_size=10
    )

    cursor = connection.cursor_obj
    assert loaded == 2
    assert connection.committed
    assert sum(len(b) for b in cursor.inserted_batches) == 2

    rejected = [
        params
        for sql, params in cursor.executed
        if sql.startswith("INSERT INTO rejected_flight_rows")
    ]
    assert len(rejected) == 1
    row_number, reason, raw_row = rejected[0]
    assert row_number == 3  # header is row 1, bad row is the 2nd data row
    assert "Base Fare (BDT)" in reason


def test_load_fails_when_required_csv_column_is_missing(tmp_path):
    csv_path = tmp_path / "flights.csv"
    header = [c for c in HEADER if c != "Total Fare (BDT)"]
    rows = [{k: v for k, v in good_row().items() if k in header}]
    write_csv(csv_path, rows, header=header)
    connection = FakeConnection()

    with pytest.raises(ValueError, match="Missing CSV columns"):
        mysql_loader.load_csv_to_mysql(csv_path, connection)

    assert connection.rolled_back
    assert not connection.committed
