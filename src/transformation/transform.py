
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping
import hashlib
import json


# Business rule: these seasons are classified as peak periods.
PEAK_SEASONS = {
    "eid",
    "hajj",
    "winter holidays",
}

# Differences greater than this tolerance are flagged.
FARE_TOLERANCE = Decimal("0.01")


def normalize_text(value: Any) -> Any:
    """Remove unnecessary whitespace and normalize text formatting."""
    if value is None:
        return None

    if not isinstance(value, str):
        return value

    return " ".join(value.strip().split())


def normalize_code(value: Any) -> Any:
    """Standardize airport/source/destination codes."""
    value = normalize_text(value)

    if isinstance(value, str):
        return value.upper()

    return value


def parse_datetime(value: Any) -> datetime | None:
    """Convert supported date values into datetime objects."""
    if value is None:
        return None

    if isinstance(value, datetime):
        return value

    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())

    if isinstance(value, str):
        formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
        ]

        for date_format in formats:
            try:
                return datetime.strptime(value.strip(), date_format)
            except ValueError:
                continue

    raise ValueError(f"Unsupported datetime value: {value!r}")


def to_decimal(value: Any) -> Decimal | None:
    """Safely convert numeric values into Decimal."""
    if value is None:
        return None

    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        raise ValueError(f"Invalid numeric value: {value!r}")

def generate_record_hash(record: Mapping[str, Any]) -> str:
    """
    Generate a deterministic SHA-256 hash for a flight record.

    Excludes generated IDs and ingestion timestamps.
    """

    business_columns = (
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

    canonical_record = {}

    for column in business_columns:
        value = record.get(column)

        if isinstance(value, Decimal):
            value = format(value.normalize(), "f")

        elif isinstance(value, datetime):
            value = value.isoformat(timespec="seconds")

        elif value is not None:
            value = str(value)

        canonical_record[column] = value

    serialized_record = json.dumps(
        canonical_record,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )

    return hashlib.sha256(
        serialized_record.encode("utf-8")
    ).hexdigest()


def classify_peak_season(seasonality: Any) -> bool | None:
    """Classify a flight as peak or non-peak."""
    season = normalize_text(seasonality)

    if season is None:
        return None

    return season.casefold() in PEAK_SEASONS


def classify_booking_lead_time(days: Any) -> str | None:
    """Group flights according to days before departure."""
    if days is None:
        return None

    days = int(days)

    if days < 0:
        return "invalid"

    if days <= 3:
        return "0-3 days"
    if days <= 7:
        return "4-7 days"
    if days <= 14:
        return "8-14 days"
    if days <= 30:
        return "15-30 days"
    if days <= 60:
        return "31-60 days"

    return "61+ days"


def calculate_fare_metrics(
    base_fare: Any,
    tax_surcharge: Any,
    total_fare: Any,
) -> dict:
    """Calculate expected fare and identify fare discrepancies."""
    base = to_decimal(base_fare)
    tax = to_decimal(tax_surcharge)
    total = to_decimal(total_fare)

    if base is None or tax is None or total is None:
        return {
            "expected_fare_bdt": None,
            "fare_discrepancy_bdt": None,
            "fare_discrepancy_flag": None,
        }

    expected = base + tax
    discrepancy = total - expected

    return {
        "expected_fare_bdt": expected,
        "fare_discrepancy_bdt": discrepancy,
        "fare_discrepancy_flag": (
            abs(discrepancy) > FARE_TOLERANCE
        ),
    }


def transform_flight_record(record: Mapping[str, Any]) -> dict:
    """Transform one raw flight record into an analytical record."""
    transformed = dict(record)

    # Standardize text and location codes.
    transformed["airline"] = normalize_text(
        record.get("airline")
    )
    transformed["source_code"] = normalize_code(
        record.get("source_code")
    )
    transformed["destination_code"] = normalize_code(
        record.get("destination_code")
    )

    for column in (
        "source_name",
        "destination_name",
        "stopovers",
        "aircraft_type",
        "booking_class",
        "booking_source",
        "seasonality",
    ):
        transformed[column] = normalize_text(
            record.get(column)
        )

    # Parse departure and arrival timestamps.
    departure = parse_datetime(
        record.get("departure_datetime")
    )
    arrival = parse_datetime(
        record.get("arrival_datetime")
    )

    transformed["departure_datetime"] = departure
    transformed["arrival_datetime"] = arrival

    # Extract departure-time features.
    transformed["departure_hour"] = (
        departure.hour if departure else None
    )
    transformed["departure_day_name"] = (
        departure.strftime("%A") if departure else None
    )
    transformed["departure_month"] = (
        departure.month if departure else None
    )
    transformed["departure_year"] = (
        departure.year if departure else None
    )

    # Classify seasonality.
    transformed["is_peak_season"] = classify_peak_season(
        transformed["seasonality"]
    )

    # Categorize booking lead time.
    transformed["booking_lead_time_group"] = (
        classify_booking_lead_time(
            record.get("days_before_departure")
        )
    )

    # Calculate fare metrics without changing original fares.
    fare_metrics = calculate_fare_metrics(
        record.get("base_fare_bdt"),
        record.get("tax_surcharge_bdt"),
        record.get("total_fare_bdt"),
    )

    transformed.update(fare_metrics)
        # Generate a stable identifier for duplicate detection.
    transformed["record_hash"] = generate_record_hash(
        transformed
    )

    return transformed


def transform_flight_records(
    records: Iterable[Mapping[str, Any]],
) -> list[dict]:
    """Transform a collection of flight records."""
    return [
        transform_flight_record(record)
        for record in records
    ]
