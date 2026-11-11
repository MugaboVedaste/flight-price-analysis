
from decimal import Decimal

from src.transformation.transform import (
    normalize_text,
    normalize_code,
    classify_peak_season,
    classify_booking_lead_time,
    calculate_fare_metrics,
    transform_flight_record,
)


def test_normalize_text():
    assert normalize_text("  Qatar   Airways  ") == "Qatar Airways"


def test_normalize_code():
    assert normalize_code("  dxb ") == "DXB"


def test_peak_season():
    assert classify_peak_season("Eid") is True
    assert classify_peak_season("Regular") is False


def test_booking_lead_time():
    assert classify_booking_lead_time(2) == "0-3 days"
    assert classify_booking_lead_time(10) == "8-14 days"
    assert classify_booking_lead_time(45) == "31-60 days"
    assert classify_booking_lead_time(-1) == "invalid"


def test_fare_metrics():
    result = calculate_fare_metrics(
        Decimal("100000"),
        Decimal("20000"),
        Decimal("125000"),
    )

    assert result["expected_fare_bdt"] == Decimal("120000")
    assert result["fare_discrepancy_bdt"] == Decimal("5000")
    assert result["fare_discrepancy_flag"] is True


def test_transform_flight_record():
    record = {
        "airline": " Qatar   Airways ",
        "source_code": " dxb ",
        "destination_code": " doh ",
        "source_name": " Dubai ",
        "destination_name": " Doha ",
        "departure_datetime": "2026-11-15 14:30:00",
        "arrival_datetime": "2026-11-15 16:00:00",
        "seasonality": "Winter Holidays",
        "days_before_departure": 20,
        "base_fare_bdt": Decimal("100000"),
        "tax_surcharge_bdt": Decimal("20000"),
        "total_fare_bdt": Decimal("120000"),
        "stopovers": "Non-stop",
        "aircraft_type": "Boeing 737",
        "booking_class": "Economy",
        "booking_source": "Online",
    }

    result = transform_flight_record(record)

    assert result["airline"] == "Qatar Airways"
    assert result["source_code"] == "DXB"
    assert result["destination_code"] == "DOH"
    assert result["departure_hour"] == 14
    assert result["departure_day_name"] == "Sunday"
    assert result["departure_month"] == 11
    assert result["is_peak_season"] is True
    assert result["booking_lead_time_group"] == "15-30 days"
    assert result["fare_discrepancy_flag"] is False
