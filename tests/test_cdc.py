"""
Integration tests for CDC ingestion (Debezium format) to Delta or JDBC sink.

Uses recorded fixture files in ``tests/fixtures/debezium/``.
"""

from pathlib import Path

import pandas as pd
import pytest

from simpleetl.cdc import CDCEvent, CDCFixtureReader, CDCIngestor

FIXTURES_DIR = str(Path(__file__).parent / "fixtures" / "debezium")


class TestCDCEvent:
    def test_parse_create(self):
        event = CDCEvent({"op": "c", "after": {"id": 1, "name": "Alice"}})
        assert event.is_create()
        assert event.get_target_record() == {"id": 1, "name": "Alice"}

    def test_parse_delete(self):
        event = CDCEvent({"op": "d", "before": {"id": 1}, "after": None})
        assert event.is_delete()
        assert event.get_target_record() == {"id": 1}

    def test_parse_update(self):
        event = CDCEvent(
            {"op": "u", "before": {"id": 1}, "after": {"id": 1, "name": "Bob"}}
        )
        assert event.is_update()
        assert event.get_target_record() == {"id": 1, "name": "Bob"}


class TestCDCFixtureReader:
    def test_read_fixtures(self):
        reader = CDCFixtureReader(FIXTURES_DIR)
        events = reader.read_fixtures()
        # orders_events.json contains 4 events
        assert len(events) == 4
        assert events[0].op == "c"
        assert events[1].op == "u"
        assert events[2].op == "d"
        assert events[3].op == "c"

    def test_read_file_explicit(self):
        reader = CDCFixtureReader()
        events = reader.read_file(str(Path(FIXTURES_DIR) / "orders_events.json"))
        assert len(events) == 4


class TestCDCIngestorJDBC:
    """JDBC sink using SQLite (always available)."""

    def test_ingest_events_to_sqlite(self, tmp_path):
        import sqlalchemy

        db_path = str(tmp_path / "cdc_test.db")
        engine = sqlalchemy.create_engine(f"sqlite:///{db_path}")

        fixtures = CDCFixtureReader(FIXTURES_DIR).read_fixtures()
        # Pre-create table with PK so merge (update) works
        with engine.begin() as conn:
            conn.execute(
                sqlalchemy.text(
                    "CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, name TEXT, amount REAL, status TEXT)"
                )
            )
        ingestor = CDCIngestor(
            sink_type="jdbc",
            sink_path=db_path,
            table_name="orders",
            key_columns=["id"],
            engine=engine,
        )
        ingestor.apply_events(fixtures)

        result = pd.read_sql("SELECT * FROM orders ORDER BY id", engine)
        # After fixtures: create 101, update 101, delete 102 (not present initially), create 103
        # Since 102 never inserted (it was a delete without prior create in fixtures), we expect 101 and 103.
        # Actually fixtures delete 102 which was never inserted, so only 101 and 103 remain.
        assert len(result) == 2
        ids = set(result["id"].tolist())
        assert ids == {101, 103}
        row_101 = result[result["id"] == 101].iloc[0]
        assert row_101["status"] == "completed"

    def test_ingest_delete_existing_row(self, tmp_path):
        import sqlalchemy

        db_path = str(tmp_path / "cdc_delete.db")
        engine = sqlalchemy.create_engine(f"sqlite:///{db_path}")
        # Pre-seed table
        with engine.begin() as conn:
            conn.execute(
                sqlalchemy.text(
                    "CREATE TABLE orders (id INTEGER PRIMARY KEY, name TEXT, amount REAL)"
                )
            )
            conn.execute(
                sqlalchemy.text(
                    "INSERT INTO orders VALUES (1, 'Alice', 99.5), (2, 'Bob', 45.0)"
                )
            )

        # Filter to delete event for id=102 — but 102 isn't in DB, so nothing removed.
        # Instead, let's test with a delete of id=2 using a custom event.
        delete_2 = CDCEvent({"op": "d", "before": {"id": 2}, "after": None})
        ingestor = CDCIngestor(
            sink_type="jdbc",
            sink_path=db_path,
            table_name="orders",
            key_columns=["id"],
            engine=engine,
        )
        ingestor.apply_events([delete_2])

        result = pd.read_sql("SELECT * FROM orders", engine)
        assert len(result) == 1
        assert result.iloc[0]["id"] == 1


class TestCDCIngestorDelta:
    """Delta sink using deltalake (optional)."""

    def test_ingest_events_to_delta(self, tmp_path):
        pytest.importorskip("deltalake", reason="deltalake not installed")

        delta_path = str(tmp_path / "delta_cdc")
        fixtures = CDCFixtureReader(FIXTURES_DIR).read_fixtures()
        ingestor = CDCIngestor(
            sink_type="delta",
            sink_path=delta_path,
            table_name="orders",
            key_columns=["id"],
        )
        ingestor.apply_events(fixtures)

        from simpleetl.formats.delta import DeltaLakeReader

        reader = DeltaLakeReader()
        result = reader.read(delta_path)
        assert len(result) == 2
        ids = set(result["id"].tolist())
        assert ids == {101, 103}
