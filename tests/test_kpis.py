
from decimal import Decimal

import pytest

from src.analytics import kpi_queries, kpi_storage


def season(name, peak, count, avg, fare_sum=None):
    return {
        "season": name,
        "is_peak_season": peak,
        "booking_count": count,
        "average_fare_bdt": Decimal(avg),
        "fare_sum": Decimal(fare_sum if fare_sum is not None else avg) * 1,
    }


def test_seasonal_variation_is_measured_against_non_peak(monkeypatch):
    rows = [
        season("Eid", True, 10, "150", fare_sum="1500"),
        season("Winter Holidays", True, 10, "120", fare_sum="1200"),
        season("Regular", False, 20, "100", fare_sum="2000"),
    ]
    monkeypatch.setattr(kpi_queries, "_fetch_dicts", lambda *a, **k: rows)

    result = kpi_queries.get_seasonal_fare_variation()
    by_season = {row["season"]: row for row in result}

    assert by_season["Eid"]["variation_vs_non_peak_pct"] == Decimal("50.00")
    assert by_season["Winter Holidays"]["variation_vs_non_peak_pct"] == Decimal("20.00")
    assert by_season["Regular"]["variation_vs_non_peak_pct"] == Decimal("0.00")
    assert all("fare_sum" not in row for row in result)


def test_seasonal_variation_without_non_peak_rows_is_none(monkeypatch):
    rows = [season("Eid", True, 10, "150", fare_sum="1500")]
    monkeypatch.setattr(kpi_queries, "_fetch_dicts", lambda *a, **k: rows)

    result = kpi_queries.get_seasonal_fare_variation()

    assert result[0]["variation_vs_non_peak_pct"] is None


def test_peak_vs_non_peak_computes_difference(monkeypatch):
    row = {
        "peak_flight_count": 10,
        "non_peak_flight_count": 20,
        "peak_average_fare_bdt": Decimal("150.00"),
        "non_peak_average_fare_bdt": Decimal("100.00"),
    }
    monkeypatch.setattr(kpi_queries, "_fetch_dicts", lambda *a, **k: [row])

    result = kpi_queries.get_peak_vs_non_peak_fare()

    assert result["fare_difference_bdt"] == Decimal("50.00")
    assert result["percentage_difference"] == Decimal("50.00")


def test_peak_vs_non_peak_without_non_peak_does_not_crash(monkeypatch):
    row = {
        "peak_flight_count": 10,
        "non_peak_flight_count": 0,
        "peak_average_fare_bdt": Decimal("150.00"),
        "non_peak_average_fare_bdt": None,
    }
    monkeypatch.setattr(kpi_queries, "_fetch_dicts", lambda *a, **k: [row])

    result = kpi_queries.get_peak_vs_non_peak_fare()

    assert result["fare_difference_bdt"] is None
    assert result["percentage_difference"] is None


@pytest.mark.parametrize("limit", [0, -1, "5", True, None])
def test_popular_routes_validates_limit(limit):
    with pytest.raises(ValueError):
        kpi_queries.get_most_popular_routes(limit=limit)


def test_kpi_queries_use_booking_count_naming():
    sources = [
        kpi_queries.get_booking_count_by_airline,
        kpi_queries.get_most_popular_routes,
    ]
    assert all(callable(fn) for fn in sources)
    assert not hasattr(kpi_queries, "get_flight_count_by_airline")


# ---------------------------------------------------------------- storage

class FakeCursor:
    def __init__(self, log, fail_on=None):
        self.log = log
        self.fail_on = fail_on

    def execute(self, sql, params=None):
        text = " ".join(sql.split())
        if self.fail_on and self.fail_on in text:
            raise RuntimeError("boom")
        self.log.append(("execute", text[:60]))

    def executemany(self, sql, rows):
        self.log.append(("executemany", len(rows)))

    def close(self):
        pass


class FakeConnection:
    def __init__(self, fail_on=None):
        self.log = []
        self.fail_on = fail_on
        self.committed = False
        self.rolled_back = False

    def cursor(self):
        return FakeCursor(self.log, self.fail_on)

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


def sample_results():
    return {
        "average_fare_by_airline": [
            {
                "airline": "A",
                "booking_count": 2,
                "average_fare_bdt": 1,
                "minimum_fare_bdt": 1,
                "maximum_fare_bdt": 1,
            }
        ],
        "seasonal_fare_variation": [
            {
                "season": "Regular",
                "is_peak_season": False,
                "booking_count": 2,
                "average_fare_bdt": 1,
                "variation_vs_non_peak_pct": 0,
            }
        ],
        "booking_count_by_airline": [
            {"airline": "A", "booking_count": 2, "percentage_of_total": 100}
        ],
        "most_popular_routes": [
            {
                "route_rank": 1,
                "source_code": "DAC",
                "source_name": "x",
                "destination_code": "CXB",
                "destination_name": "y",
                "booking_count": 2,
                "average_fare_bdt": 1,
            }
        ],
        "fare_by_booking_lead_time": [],
        "peak_vs_non_peak_fare": {
            "peak_flight_count": 0,
            "non_peak_flight_count": 2,
            "peak_average_fare_bdt": None,
            "non_peak_average_fare_bdt": 1,
            "fare_difference_bdt": None,
            "percentage_difference": None,
        },
    }


def test_store_kpis_replaces_every_table_in_one_transaction():
    connection = FakeConnection()

    stored = kpi_storage.store_kpis(connection, sample_results())

    assert connection.committed
    assert stored == {
        "kpi_avg_fare_by_airline": 1,
        "kpi_seasonal_fare_variation": 1,
        "kpi_booking_count_by_airline": 1,
        "kpi_popular_routes": 1,
        "kpi_fare_by_booking_lead_time": 0,
        "kpi_peak_vs_non_peak_fare": 1,
    }


def test_store_kpis_rolls_back_on_failure():
    connection = FakeConnection(fail_on="kpi_popular_routes")

    with pytest.raises(RuntimeError):
        kpi_storage.store_kpis(connection, sample_results())

    assert connection.rolled_back
    assert not connection.committed


def test_kpi_schema_defines_every_table_that_is_stored():
    schema = kpi_storage.KPI_SCHEMA_PATH.read_text(encoding="utf-8")
    tables = [t for t, _ in kpi_storage.KPI_TABLES.values()]
    tables.append(kpi_storage.PEAK_TABLE)

    for table in tables:
        assert f"flight_analytics.{table}" in schema
