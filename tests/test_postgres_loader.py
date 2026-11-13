
import csv
from decimal import Decimal

import pytest

from src.loading import postgres_loader as loader


def make_csv_row(n, record_hash=None):
    row = {column: "" for column in loader.COLUMN_MAPPING}
    row.update(
        {
            "id": str(n),
            "airline": "Biman Bangladesh",
            "source_code": "DAC",
            "destination_code": "CXB",
            "departure_datetime": "2025-11-17 06:25:00",
            "arrival_datetime": "2025-11-17 07:38:10",
            "duration_hours": "1.2",
            "base_fare_bdt": "100.50",
            "tax_surcharge_bdt": "20.00",
            "total_fare_bdt": "120.50",
            "days_before_departure": "10",
            "ingested_at": "2025-11-17 06:25:00",
            "departure_hour": "6",
            "departure_month": "11",
            "departure_year": "2025",
            "is_peak_season": "False",
            "fare_discrepancy_flag": "False",
            "record_hash": record_hash or f"hash-{n}",
        }
    )
    return row


def write_csv(path, rows):
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(loader.COLUMN_MAPPING))
        writer.writeheader()
        writer.writerows(rows)


class FakeDatabase:
    """Minimal stand-in for the target table (unique on hash and id)."""

    def __init__(self):
        self.hashes = set()
        self.ids = set()
        self.schema_applied = False


class FakeCursor:
    """
    Emulates the target table for COUNT(*) and the multi-row
    INSERT ... ON CONFLICT DO NOTHING RETURNING 1 statement.
    """

    def __init__(self, db, count_offset=0):
        self.db = db
        self.count_offset = count_offset
        self.statements = 0
        self.max_parameters = 0
        self._result = []

    def execute(self, sql, params=None):
        self.statements += 1
        text = " ".join(sql.split())

        if text.startswith("CREATE SCHEMA"):
            self.db.schema_applied = True
            return

        if text.startswith("SELECT COUNT(*)"):
            assert self.db.schema_applied, "table queried before schema"
            self._result = [(len(self.db.hashes) + self.count_offset,)]
            return

        assert text.startswith("INSERT INTO")
        assert "ON CONFLICT DO NOTHING" in text
        assert "RETURNING" in text

        width = len(loader.COLUMN_MAPPING)
        assert len(params) % width == 0
        assert text.count("(%s") == len(params) // width
        self.max_parameters = max(self.max_parameters, len(params))

        names = list(loader.COLUMN_MAPPING.values())
        id_index = names.index("source_record_id")
        hash_index = names.index("record_hash")

        self._result = []
        for start in range(0, len(params), width):
            values = params[start:start + width]
            if (
                values[hash_index] in self.db.hashes
                or values[id_index] in self.db.ids
            ):
                continue
            self.db.hashes.add(values[hash_index])
            self.db.ids.add(values[id_index])
            self._result.append((1,))

    def fetchone(self):
        return self._result[0]

    def fetchall(self):
        return self._result

    def close(self):
        pass


class FakeConnection:
    def __init__(self, db, count_offset=0):
        self.db = db
        self.count_offset = count_offset
        self.committed = False
        self.rolled_back = False
        self.cursors = []

    def cursor(self):
        cursor = FakeCursor(self.db, self.count_offset)
        self.cursors.append(cursor)
        return cursor

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


@pytest.fixture
def fake_execute_values():
    """Kept as a no-op name so the tests below read unchanged."""
    return None


def test_record_hash_is_loaded():
    assert loader.COLUMN_MAPPING["record_hash"] == "record_hash"


def test_convert_value_types():
    assert loader.convert_value("base_fare_bdt", "10.5") == Decimal("10.5")
    assert loader.convert_value("departure_hour", "6") == 6
    assert loader.convert_value("is_peak_season", "True") is True
    assert loader.convert_value("fare_discrepancy_flag", "false") is False
    assert loader.convert_value("airline", "") is None

    with pytest.raises(ValueError):
        loader.convert_value("is_peak_season", "maybe")


def test_load_inserts_all_rows_and_verifies_count(tmp_path, fake_execute_values):
    csv_path = tmp_path / "transformed.csv"
    write_csv(csv_path, [make_csv_row(n) for n in range(1, 6)])
    db = FakeDatabase()
    connection = FakeConnection(db)

    result = loader.load_transformed_csv(
        csv_path, batch_size=2, connection=connection
    )

    assert result["total_read"] == 5
    assert result["total_inserted"] == 5
    assert result["total_skipped"] == 0
    assert result["table_row_count"] == 5
    assert connection.committed


def test_rerun_with_new_source_ids_is_idempotent(tmp_path, fake_execute_values):
    """Same flights, new MySQL ids (as after a re-ingestion) -> no failure."""

    db = FakeDatabase()
    first = tmp_path / "first.csv"
    second = tmp_path / "second.csv"

    write_csv(first, [make_csv_row(n) for n in range(1, 4)])
    # Same hashes, but the MySQL auto-increment ids moved on.
    write_csv(
        second,
        [make_csv_row(n + 1000, record_hash=f"hash-{n}") for n in range(1, 4)],
    )

    loader.load_transformed_csv(first, connection=FakeConnection(db))
    result = loader.load_transformed_csv(second, connection=FakeConnection(db))

    assert result["total_read"] == 3
    assert result["total_inserted"] == 0
    assert result["total_skipped"] == 3
    assert result["table_row_count"] == 3


def test_row_count_mismatch_rolls_back(tmp_path, fake_execute_values):
    csv_path = tmp_path / "transformed.csv"
    write_csv(csv_path, [make_csv_row(1)])

    class DriftingCursor(FakeCursor):
        """The table count jumps after the first COUNT(*) query."""

        def execute(self, sql, params=None):
            super().execute(sql, params)
            if " ".join(sql.split()).startswith("SELECT COUNT(*)"):
                self.count_offset += 99

    class DriftingConnection(FakeConnection):
        def cursor(self):
            return DriftingCursor(self.db)

    connection = DriftingConnection(FakeDatabase())

    with pytest.raises(RuntimeError, match="verification failed"):
        loader.load_transformed_csv(csv_path, connection=connection)

    assert connection.rolled_back
    assert not connection.committed


def test_no_psycopg2_specific_helpers_are_used():
    """The Airflow hook returns psycopg 3 connections."""
    source = open(loader.__file__, encoding="utf-8").read()

    assert "import psycopg2" not in source
    assert "from psycopg2" not in source
    assert "execute_values(" not in source


def test_large_batches_are_split_below_the_bind_parameter_limit(
    tmp_path, monkeypatch
):
    width = len(loader.COLUMN_MAPPING)
    monkeypatch.setattr(loader, "MAX_PARAMETERS_PER_STATEMENT", width * 3)

    csv_path = tmp_path / "transformed.csv"
    write_csv(csv_path, [make_csv_row(n) for n in range(1, 11)])
    connection = FakeConnection(FakeDatabase())

    result = loader.load_transformed_csv(
        csv_path, batch_size=10, connection=connection
    )

    assert result["total_inserted"] == 10
    assert max(c.max_parameters for c in connection.cursors) <= width * 3


def test_schema_is_created_before_the_table_is_used(tmp_path, fake_execute_values):
    csv_path = tmp_path / "transformed.csv"
    write_csv(csv_path, [make_csv_row(1)])
    db = FakeDatabase()

    loader.load_transformed_csv(csv_path, connection=FakeConnection(db))

    assert db.schema_applied


def test_schema_file_is_idempotent_and_matches_the_loader():
    sql = loader.SCHEMA_PATH.read_text(encoding="utf-8")

    assert sql.lstrip().upper().startswith("CREATE SCHEMA IF NOT EXISTS")
    assert loader.TARGET_TABLE in sql

    for statement in sql.split(";"):
        statement = " ".join(statement.split()).upper()
        if statement.startswith("CREATE"):
            assert "IF NOT EXISTS" in statement, statement[:60]

    for column in loader.COLUMN_MAPPING.values():
        assert column in sql
