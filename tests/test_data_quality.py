
from decimal import Decimal

from src.validation import data_quality as dq


class FakeCursor:
    """Answers each query by matching a keyword in its SQL text."""

    def __init__(self, scalars, column_names, seasonality=None):
        self.scalars = scalars
        self.column_names = column_names
        self.seasonality = seasonality or [("Regular", 10)]
        self._rows = []
        self.queries = []

    def execute(self, query, params=None):
        self.queries.append(query)
        q = " ".join(query.split())

        if "information_schema.columns" in q:
            self._rows = [(name,) for name in self.column_names]
        elif "information_schema.tables" in q:
            self._rows = [(1,)]
        elif "GROUP BY seasonality" in q:
            self._rows = list(self.seasonality)
        else:
            for keyword, value in self.scalars.items():
                if keyword in q:
                    self._rows = [(value,)]
                    break
            else:
                raise AssertionError(f"Unexpected query: {q}")

    def fetchone(self):
        return self._rows[0]

    def fetchall(self):
        return self._rows

    def close(self):
        pass


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


ALL_COLUMNS = list(dq.REQUIRED_COLUMNS) + ["source_name", "seasonality"]

CLEAN_SCALARS = {
    # Order matters: more specific keywords first.
    "FROM rejected_flight_rows": 0,
    "source_name IS NULL": 0,
    "airline IS NULL": 0,
    "base_fare_bdt < 0": 0,
    "duration_hours <= 0": 0,
    "NOT IN": 0,
    "ABS(": 5,
    "SELECT COUNT(*) FROM raw_flight_prices": 100,
}


def make_connection(**overrides):
    scalars = {**CLEAN_SCALARS, **overrides.pop("scalars", {})}
    columns = overrides.pop("columns", ALL_COLUMNS)
    return FakeConnection(FakeCursor(scalars, columns, **overrides))


def test_clean_data_has_no_failed_checks():
    results = dq.run_data_quality_checks(make_connection())

    assert results["total_records"] == 100
    assert results["missing_required_columns"] == 0
    assert results["fare_discrepancies"] == 5
    assert results["fare_discrepancy_percentage"] == Decimal("5.00")
    assert dq.get_failed_checks(results) == []


def test_missing_required_column_is_critical():
    columns = [c for c in ALL_COLUMNS if c != "base_fare_bdt"]

    results = dq.run_data_quality_checks(make_connection(columns=columns))

    assert results["missing_required_column_names"] == ["base_fare_bdt"]
    assert dq.get_failed_checks(results) == ["missing_required_columns"]


def test_each_critical_check_is_reported():
    results = dq.run_data_quality_checks(
        make_connection(
            scalars={
                "airline IS NULL": 2,
                "base_fare_bdt < 0": 3,
                "duration_hours <= 0": 1,
                "NOT IN": 4,
            }
        )
    )

    assert dq.get_failed_checks(results) == [
        "missing_values",
        "invalid_fares",
        "invalid_durations",
        "invalid_cities",
    ]


def test_fare_discrepancies_and_blank_text_are_only_warnings():
    results = dq.run_data_quality_checks(
        make_connection(
            scalars={"ABS(": 50, "source_name IS NULL": 7}
        )
    )

    assert results["fare_discrepancies"] == 50
    assert results["empty_optional_fields"] == 7
    assert dq.get_failed_checks(results) == []


def test_rejected_rows_below_threshold_is_a_warning():
    # 2 rejected of 102 rows = 1.96%
    results = dq.run_data_quality_checks(
        make_connection(scalars={"FROM rejected_flight_rows": 2})
    )

    assert results["rejected_rows"] == 2
    assert dq.get_failed_checks(results) == []


def test_rejected_rows_above_threshold_fails():
    # 20 rejected of 120 rows = 16.67%
    results = dq.run_data_quality_checks(
        make_connection(scalars={"FROM rejected_flight_rows": 20})
    )

    assert dq.get_failed_checks(results) == ["rejected_rows"]


def test_city_check_uses_known_airport_codes():
    assert "DAC" in dq.VALID_AIRPORT_CODES
    assert "CXB" in dq.VALID_AIRPORT_CODES
    assert "XXX" not in dq.VALID_AIRPORT_CODES
    assert len(dq.VALID_AIRPORT_CODES) == 20
