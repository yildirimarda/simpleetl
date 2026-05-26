"""
Tests for incremental/delta loading support.

Covers Watermark dataclass, FileWatermarkStore, DatabaseWatermarkStore,
WatermarkManager, DatabaseReader.incremental_query, DatabaseWriter.merge,
and ETLJob.run_incremental / merge_load.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, PropertyMock

import pandas as pd
import pytest
import sqlalchemy

from simpleetl.core.config import ETLJobConfig
from simpleetl.core.incremental import (
    Watermark,
    WatermarkManager,
    FileWatermarkStore,
    DatabaseWatermarkStore,
)
from simpleetl.core.job import ETLJob
from simpleetl.formats.database import DatabaseReader, DatabaseWriter


# ---------------------------------------------------------------------------
# Watermark dataclass
# ---------------------------------------------------------------------------


class TestWatermark:
    def test_create_watermark(self):
        wm = Watermark(
            job_name="test_job",
            source="users_table",
            column="updated_at",
            value="2024-01-01",
        )
        assert wm.job_name == "test_job"
        assert wm.source == "users_table"
        assert wm.column == "updated_at"
        assert wm.value == "2024-01-01"
        assert wm.updated_at is not None

    def test_watermark_default_timestamp(self):
        before = datetime.now(timezone.utc)
        wm = Watermark(
            job_name="j", source="s", column="c", value=100,
        )
        after = datetime.now(timezone.utc)
        parsed = datetime.fromisoformat(wm.updated_at)
        assert before <= parsed <= after

    def test_watermark_with_custom_timestamp(self):
        wm = Watermark(
            job_name="j", source="s", column="c",
            value=1, updated_at="2024-06-15T00:00:00+00:00",
        )
        assert wm.updated_at == "2024-06-15T00:00:00+00:00"


# ---------------------------------------------------------------------------
# FileWatermarkStore
# ---------------------------------------------------------------------------


class TestFileWatermarkStore:
    def test_set_and_get(self, tmp_path):
        store = FileWatermarkStore(str(tmp_path / "watermarks.json"))
        wm = Watermark(
            job_name="job1", source="src1",
            column="id", value=42,
        )
        store.set(wm)
        result = store.get("job1", "src1")
        assert result is not None
        assert result.job_name == "job1"
        assert result.source == "src1"
        assert result.column == "id"
        assert result.value == 42

    def test_get_nonexistent(self, tmp_path):
        store = FileWatermarkStore(str(tmp_path / "watermarks.json"))
        assert store.get("no_job", "no_source") is None

    def test_overwrite_watermark(self, tmp_path):
        store = FileWatermarkStore(str(tmp_path / "watermarks.json"))
        wm1 = Watermark(
            job_name="job1", source="src1",
            column="id", value=10,
        )
        store.set(wm1)
        wm2 = Watermark(
            job_name="job1", source="src1",
            column="id", value=20,
        )
        store.set(wm2)
        result = store.get("job1", "src1")
        assert result.value == 20

    def test_delete(self, tmp_path):
        store = FileWatermarkStore(str(tmp_path / "watermarks.json"))
        wm = Watermark(
            job_name="job1", source="src1",
            column="id", value=1,
        )
        store.set(wm)
        store.delete("job1", "src1")
        assert store.get("job1", "src1") is None

    def test_multiple_jobs(self, tmp_path):
        store = FileWatermarkStore(str(tmp_path / "watermarks.json"))
        store.set(Watermark("j1", "s1", "id", 1))
        store.set(Watermark("j2", "s2", "id", 2))
        assert store.get("j1", "s1").value == 1
        assert store.get("j2", "s2").value == 2

    def test_persists_to_disk(self, tmp_path):
        path = str(tmp_path / "watermarks.json")
        store1 = FileWatermarkStore(path)
        store1.set(Watermark("j1", "s1", "id", 99))
        store2 = FileWatermarkStore(path)
        result = store2.get("j1", "s1")
        assert result.value == 99

    def test_corrupted_file_returns_none(self, tmp_path):
        path = tmp_path / "watermarks.json"
        path.write_text("not valid json{{{")
        store = FileWatermarkStore(str(path))
        assert store.get("j", "s") is None


# ---------------------------------------------------------------------------
# DatabaseWatermarkStore
# ---------------------------------------------------------------------------


class TestDatabaseWatermarkStore:
    def test_set_and_get(self):
        engine = sqlalchemy.create_engine("sqlite:///:memory:")
        store = DatabaseWatermarkStore(engine)
        wm = Watermark(
            job_name="job1", source="src1",
            column="id", value=42,
        )
        store.set(wm)
        result = store.get("job1", "src1")
        assert result is not None
        assert result.job_name == "job1"
        assert result.source == "src1"
        assert result.column == "id"
        assert result.value == "42"

    def test_get_nonexistent(self):
        engine = sqlalchemy.create_engine("sqlite:///:memory:")
        store = DatabaseWatermarkStore(engine)
        assert store.get("no_job", "no_source") is None

    def test_upsert(self):
        engine = sqlalchemy.create_engine("sqlite:///:memory:")
        store = DatabaseWatermarkStore(engine)
        wm1 = Watermark("j1", "s1", "id", 10)
        store.set(wm1)
        wm2 = Watermark("j1", "s1", "id", 20)
        store.set(wm2)
        result = store.get("j1", "s1")
        assert result.value == "20"

    def test_delete(self):
        engine = sqlalchemy.create_engine("sqlite:///:memory:")
        store = DatabaseWatermarkStore(engine)
        store.set(Watermark("j1", "s1", "id", 1))
        store.delete("j1", "s1")
        assert store.get("j1", "s1") is None

    def test_with_connection_string(self):
        store = DatabaseWatermarkStore("sqlite:///:memory:")
        store.set(Watermark("j1", "s1", "id", 5))
        result = store.get("j1", "s1")
        assert result.value == "5"


# ---------------------------------------------------------------------------
# WatermarkManager
# ---------------------------------------------------------------------------


class TestWatermarkManager:
    def test_get_watermark(self, tmp_path):
        store = FileWatermarkStore(str(tmp_path / "wm.json"))
        manager = WatermarkManager(store)
        store.set(Watermark("j1", "s1", "id", 100))
        result = manager.get_watermark("j1", "s1")
        assert result is not None
        assert result.value == 100

    def test_set_watermark(self, tmp_path):
        store = FileWatermarkStore(str(tmp_path / "wm.json"))
        manager = WatermarkManager(store)
        wm = manager.set_watermark("j1", "s1", "id", 200)
        assert wm.value == 200
        assert wm.job_name == "j1"
        assert wm.column == "id"

    def test_reset_watermark(self, tmp_path):
        store = FileWatermarkStore(str(tmp_path / "wm.json"))
        manager = WatermarkManager(store)
        manager.set_watermark("j1", "s1", "id", 1)
        manager.reset_watermark("j1", "s1")
        assert manager.get_watermark("j1", "s1") is None

    def test_from_config_file_store(self):
        config = ETLJobConfig(
            name="test",
            input_format="csv",
            output_format="csv",
            watermark_store="file",
        )
        manager = WatermarkManager.from_config(config)
        assert isinstance(manager.store, FileWatermarkStore)

    def test_from_config_database_store(self):
        config = ETLJobConfig(
            name="test",
            input_format="csv",
            output_format="csv",
            watermark_store="database",
            database={"url": "sqlite:///:memory:"},
        )
        manager = WatermarkManager.from_config(config)
        assert isinstance(manager.store, DatabaseWatermarkStore)

    def test_from_config_database_url_in_params(self):
        config = ETLJobConfig(
            name="test",
            input_format="csv",
            output_format="csv",
            watermark_store="database",
            params={"database_url": "sqlite:///:memory:"},
        )
        manager = WatermarkManager.from_config(config)
        assert isinstance(manager.store, DatabaseWatermarkStore)

    def test_from_config_database_store_missing_url(self):
        config = ETLJobConfig(
            name="test",
            input_format="csv",
            output_format="csv",
            watermark_store="database",
        )
        with pytest.raises(ValueError, match="Database URL must be configured"):
            WatermarkManager.from_config(config)


# ---------------------------------------------------------------------------
# DatabaseReader.incremental_query
# ---------------------------------------------------------------------------


class TestIncrementalQuery:
    def test_basic_query(self):
        query = DatabaseReader.incremental_query(
            table="users",
            watermark_column="updated_at",
            last_value="2024-01-01",
        )
        assert "SELECT * FROM users" in query
        assert "WHERE updated_at > '2024-01-01'" in query
        assert "ORDER BY updated_at" in query

    def test_with_columns(self):
        query = DatabaseReader.incremental_query(
            table="users",
            watermark_column="id",
            last_value=100,
            additional_columns=["id", "name", "email"],
        )
        assert "SELECT id, name, email FROM users" in query
        assert "WHERE id > 100" in query

    def test_with_order_by(self):
        query = DatabaseReader.incremental_query(
            table="events",
            watermark_column="created_at",
            last_value="2024-06-01",
            order_by="event_id",
        )
        assert "ORDER BY event_id" in query
        assert "WHERE created_at > '2024-06-01'" in query

    def test_numeric_watermark(self):
        query = DatabaseReader.incremental_query(
            table="orders",
            watermark_column="order_id",
            last_value=5000,
        )
        assert "WHERE order_id > 5000" in query


# ---------------------------------------------------------------------------
# DatabaseWriter.merge
# ---------------------------------------------------------------------------


class TestDatabaseWriterMerge:
    def _make_df(self):
        return pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "value": [10, 20, 30],
        })

    def test_merge_sqlite(self):
        engine = sqlalchemy.create_engine("sqlite:///:memory:")
        # Create target table
        with engine.begin() as conn:
            conn.execute(sqlalchemy.text(
                "CREATE TABLE users (id INTEGER PRIMARY KEY, "
                "name TEXT, value INTEGER)"
            ))
            conn.execute(sqlalchemy.text(
                "INSERT INTO users VALUES (1, 'OldAlice', 99)"
            ))

        writer = DatabaseWriter()
        df = self._make_df()
        rows = writer.merge(
            data=df,
            destination=engine,
            table_name="users",
            key_columns=["id"],
        )
        assert rows > 0

        # Verify data was merged
        result = pd.read_sql("SELECT * FROM users ORDER BY id", engine)
        assert len(result) == 3
        assert result.loc[result["id"] == 1, "name"].values[0] == "Alice"

    def test_merge_generic_fallback(self):
        """Test the generic DELETE+INSERT fallback with a mock engine."""
        writer = DatabaseWriter()
        df = pd.DataFrame({
            "id": [1],
            "name": ["Test"],
        })

        # Use SQLite but mock the dialect name to trigger generic path
        engine = sqlalchemy.create_engine("sqlite:///:memory:")
        with patch.object(
            type(engine.dialect), "name", new_callable=PropertyMock
        ) as mock_dialect:
            mock_dialect.return_value = "unknown_db"
            with engine.begin() as conn:
                conn.execute(sqlalchemy.text(
                    "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"
                ))
                conn.execute(sqlalchemy.text("INSERT INTO t VALUES (1, 'Old')"))

            rows = writer.merge(
                data=df,
                destination=engine,
                table_name="t",
                key_columns=["id"],
            )
            assert rows > 0

    def test_merge_with_update_columns(self):
        engine = sqlalchemy.create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            conn.execute(sqlalchemy.text(
                "CREATE TABLE users (id INTEGER PRIMARY KEY, "
                "name TEXT, value INTEGER)"
            ))
            conn.execute(sqlalchemy.text(
                "INSERT INTO users VALUES (1, 'OldAlice', 99)"
            ))

        writer = DatabaseWriter()
        df = pd.DataFrame({
            "id": [1],
            "name": ["NewAlice"],
            "value": [100],
        })
        writer.merge(
            data=df,
            destination=engine,
            table_name="users",
            key_columns=["id"],
            update_columns=["name"],
        )

        result = pd.read_sql("SELECT * FROM users", engine)
        assert result["name"].values[0] == "NewAlice"


# ---------------------------------------------------------------------------
# ETLJob.run_incremental
# ---------------------------------------------------------------------------


class ConcreteJob(ETLJob):
    """A concrete ETL job for testing."""

    def __init__(self, config, extract_data=None):
        super().__init__(config)
        self.extract_data = extract_data
        self.extract_calls = []
        self.transform_calls = []
        self.load_calls = []

    def run(self):
        data = self.extract()
        transformed = self.transform(data)
        self.load(transformed)

    def extract(self, **kwargs):
        self.extract_calls.append(kwargs)
        return self.extract_data

    def transform(self, data):
        self.transform_calls.append(data)
        return data

    def load(self, data, **kwargs):
        self.load_calls.append((data, kwargs))


class TestRunIncremental:
    def test_raises_when_not_enabled(self):
        config = ETLJobConfig(
            name="test",
            input_format="csv",
            output_format="csv",
            incremental=False,
        )
        job = ConcreteJob(config)
        with pytest.raises(ValueError, match="not enabled"):
            job.run_incremental("source")

    def test_raises_when_no_column(self):
        config = ETLJobConfig(
            name="test",
            input_format="csv",
            output_format="csv",
            incremental=True,
            incremental_column=None,
        )
        job = ConcreteJob(config)
        with pytest.raises(ValueError, match="incremental_column"):
            job.run_incremental("source")

    def test_full_run_no_prior_watermark(self, tmp_path):
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["a", "b", "c"],
        })
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
            incremental=True,
            incremental_column="id",
            watermark_store="file",
        )
        job = ConcreteJob(config, extract_data=df)
        with patch("simpleetl.core.job.WatermarkManager") as MockWM:
            mock_mgr = MagicMock()
            mock_mgr.get_watermark.return_value = None
            MockWM.from_config.return_value = mock_mgr
            job.run_incremental("src")

            mock_mgr.get_watermark.assert_called_once_with("test_job", "src")
            mock_mgr.set_watermark.assert_called_once_with(
                job_name="test_job",
                source="src",
                column="id",
                value=3,
            )

    def test_run_with_prior_watermark(self):
        df = pd.DataFrame({
            "id": [4, 5],
            "name": ["d", "e"],
        })
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
            incremental=True,
            incremental_column="id",
        )
        job = ConcreteJob(config, extract_data=df)
        with patch("simpleetl.core.job.WatermarkManager") as MockWM:
            mock_mgr = MagicMock()
            wm = Watermark("test_job", "src", "id", 3)
            mock_mgr.get_watermark.return_value = wm
            MockWM.from_config.return_value = mock_mgr

            job.run_incremental("src")
            mock_mgr.set_watermark.assert_called_once_with(
                job_name="test_job",
                source="src",
                column="id",
                value=5,
            )

    def test_run_with_none_data(self):
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
            incremental=True,
            incremental_column="id",
        )
        job = ConcreteJob(config, extract_data=None)
        with patch("simpleetl.core.job.WatermarkManager") as MockWM:
            mock_mgr = MagicMock()
            mock_mgr.get_watermark.return_value = None
            MockWM.from_config.return_value = mock_mgr

            job.run_incremental("src")
            mock_mgr.set_watermark.assert_not_called()

    def test_run_with_empty_dataframe(self):
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
            incremental=True,
            incremental_column="id",
        )
        job = ConcreteJob(config, extract_data=pd.DataFrame())
        with patch("simpleetl.core.job.WatermarkManager") as MockWM:
            mock_mgr = MagicMock()
            mock_mgr.get_watermark.return_value = None
            MockWM.from_config.return_value = mock_mgr

            job.run_incremental("src")
            mock_mgr.set_watermark.assert_not_called()


# ---------------------------------------------------------------------------
# ETLJob.merge_load
# ---------------------------------------------------------------------------


class TestMergeLoad:
    def test_merge_load_delegates_to_writer(self):
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="database",
        )
        job = ConcreteJob(config)
        df = pd.DataFrame({"id": [1], "name": ["a"]})

        with patch("simpleetl.formats.database.DatabaseWriter") as MockWriter:
            mock_writer = MagicMock()
            MockWriter.return_value = mock_writer

            job.merge_load(
                data=df,
                destination="sqlite:///:memory:",
                table_name="users",
                merge_keys=["id"],
            )

            mock_writer.merge.assert_called_once_with(
                data=df,
                destination="sqlite:///:memory:",
                table_name="users",
                merge_keys=["id"],
            )

    def test_merge_load_with_kwargs(self):
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="database",
        )
        job = ConcreteJob(config)
        df = pd.DataFrame({"id": [1], "name": ["a"]})

        with patch("simpleetl.formats.database.DatabaseWriter") as MockWriter:
            mock_writer = MagicMock()
            MockWriter.return_value = mock_writer

            job.merge_load(
                data=df,
                destination="sqlite:///:memory:",
                table_name="users",
                merge_keys=["id"],
                schema="public",
            )

            call_kwargs = mock_writer.merge.call_args
            assert call_kwargs[1]["schema"] == "public"
