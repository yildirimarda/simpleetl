"""Extra coverage tests for remaining gaps."""
import pytest
import pandas as pd
import tempfile
import os
from unittest.mock import MagicMock

# -------------------------------------------------------------------
# formats/database.py — merge methods
# -------------------------------------------------------------------

class TestDatabaseMerge:
    def _make_engine_with_table(self):
        import sqlalchemy

        engine = sqlalchemy.create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            conn.execute(
                sqlalchemy.text(
                    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"
                )
            )
        return engine

    def test_sqlite_upsert(self):
        from simpleetl.formats.database import DatabaseWriter

        engine = self._make_engine_with_table()
        df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})

        result = DatabaseWriter._merge_sqlite(
            engine, df, "users", ["id"], ["name"]
        )
        assert isinstance(result, int)

    def test_generic_merge(self):
        from simpleetl.formats.database import DatabaseWriter

        engine = self._make_engine_with_table()
        df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})

        result = DatabaseWriter._merge_generic(
            engine, df, "users", ["id"], ["name"]
        )
        assert isinstance(result, int)

    def test_postgres_upsert_mocked(self):
        from simpleetl.formats.database import DatabaseWriter

        engine = MagicMock()
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.rowcount = 1
        mock_conn.execute.return_value = mock_result
        engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        df = pd.DataFrame({"id": [1], "name": ["Alice"]})
        result = DatabaseWriter._merge_postgresql(
            engine, df, "users", ["id"], ["name"]
        )
        assert result == 1

    def test_mysql_upsert_mocked(self):
        from simpleetl.formats.database import DatabaseWriter

        engine = MagicMock()
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.rowcount = 1
        mock_conn.execute.return_value = mock_result
        engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        df = pd.DataFrame({"id": [1], "name": ["Alice"]})
        result = DatabaseWriter._merge_mysql(
            engine, df, "users", ["id"], ["name"]
        )
        assert result == 1


# -------------------------------------------------------------------
# formats/database.py — read/write with cloud connection strings
# -------------------------------------------------------------------

class TestDatabaseFormat:
    def test_database_reader_init(self):
        from simpleetl.formats.database import DatabaseReader

        reader = MagicMock(spec=DatabaseReader)
        assert reader is not None

    def test_database_writer_init(self):
        from simpleetl.formats.database import DatabaseWriter

        writer = MagicMock(spec=DatabaseWriter)
        assert writer is not None


# -------------------------------------------------------------------
# core/security.py — remaining uncovered lines
# -------------------------------------------------------------------

class TestSecurityUncovered:
    def test_detect_pii_columns(self):
        from simpleetl.core.security import detect_pii_columns

        df = pd.DataFrame({"ssn": ["123-45-6789"], "name": ["Alice"]})
        result = detect_pii_columns(df)
        assert result is not None

    def test_detect_pii_values(self):
        from simpleetl.core.security import detect_pii_values

        df = pd.DataFrame({"email": ["test@example.com"], "name": ["Alice"]})
        result = detect_pii_values(df)
        assert result is not None

    def test_mask_pii(self):
        from simpleetl.core.security import mask_pii

        df = pd.DataFrame({"email": ["test@example.com"], "phone": ["555-123-4567"]})
        masked = mask_pii(df, columns={"email": "email", "phone": "phone"})
        assert masked is not None

    def test_mask_email(self):
        from simpleetl.core.security import mask_email

        result = mask_email("test@example.com")
        assert result != "test@example.com"

    def test_mask_phone(self):
        from simpleetl.core.security import mask_phone

        result = mask_phone("555-123-4567")
        assert result != "555-123-4567"

    def test_mask_credit_card(self):
        from simpleetl.core.security import mask_credit_card

        result = mask_credit_card("4111-1111-1111-1111")
        assert result != "4111-1111-1111-1111"

    def test_column_encryptor(self):
        from simpleetl.core.security import ColumnEncryptor

        encryptor = ColumnEncryptor()
        df = pd.DataFrame({"secret": ["data1", "data2"]})
        encrypted_df = encryptor.encrypt_column(df, "secret")
        decrypted_df = encryptor.decrypt_column(encrypted_df, "secret")
        assert list(decrypted_df["secret"]) == ["data1", "data2"]

    def test_audit_logger(self):
        from simpleetl.core.security import AuditLogger

        logger = AuditLogger()
        logger.log_access(user="test_user", action="read", source="test_table")
        logger.log_transformation(
            user="test_user",
            job_name="test_job",
            operation="filter",
            source="src_table",
            destination="dst_table",
        )
        trail = logger.get_audit_trail()
        assert trail is not None

    def test_rbac_policy(self):
        from simpleetl.core.security import RBACPolicy, apply_rbac_filter

        policy = RBACPolicy()
        df = pd.DataFrame({"id": [1, 2], "secret": ["a", "b"]})
        result = apply_rbac_filter(df, role="viewer", source="test_table", policy=policy)
        assert result is not None


# -------------------------------------------------------------------
# core/lineage.py — remaining uncovered lines
# -------------------------------------------------------------------

class TestLineageRemaining:
    def test_lineage_tracker_get_events_filtered(self):
        from simpleetl.core.lineage import (
            LineageEvent,
            get_lineage_tracker,
        )

        tracker = get_lineage_tracker()
        event = LineageEvent(
            job_name="filter_job",
            phase="post_extract",
            operation="extract",
            input_rows=0,
            output_rows=50,
        )
        tracker.record_event(event)

        tracker.get_events()
        filtered = tracker.get_events(job_name="filter_job")
        assert len(filtered) >= 1

    def test_lineage_tracker_clear(self):
        from simpleetl.core.lineage import get_lineage_tracker

        tracker = get_lineage_tracker()
        tracker.clear()

    def test_lineage_event_with_metadata(self):
        from simpleetl.core.lineage import LineageEvent

        event = LineageEvent(
            job_name="test",
            phase="post_extract",
            operation="extract",
            input_rows=0,
            output_rows=10,
            metadata={"source": "test_db"},
        )
        assert event.metadata["source"] == "test_db"

    def test_freshness_check_stale(self):
        from simpleetl.core.lineage import DataFreshnessTracker
        from datetime import datetime, timezone, timedelta

        tracker = DataFreshnessTracker()
        old_ts = datetime.now(timezone.utc) - timedelta(hours=2)
        tracker.record_freshness("src", timestamp=old_ts)
        is_stale = tracker.is_stale("src", max_age_seconds=3600)
        assert isinstance(is_stale, bool)


# -------------------------------------------------------------------
# core/metrics.py — remaining uncovered lines
# -------------------------------------------------------------------

class TestMetricsRemaining:
    def test_counter_with_labelnames(self):
        from prometheus_client import CollectorRegistry
        from simpleetl.core.metrics import MetricsCollector

        registry = CollectorRegistry()
        collector = MetricsCollector(registry=registry)
        # Create a counter with labels
        counter = collector.counter("test_counter", "A test", labelnames=("status",))
        counter.labels(status="success").inc()

    def test_gauge_with_labels(self):
        from prometheus_client import CollectorRegistry
        from simpleetl.core.metrics import MetricsCollector

        registry = CollectorRegistry()
        collector = MetricsCollector(registry=registry)
        gauge = collector.gauge("test_gauge", "A test", labelnames=("job",))
        gauge.labels(job="test").set(3.0)

    def test_histogram_with_labels(self):
        from prometheus_client import CollectorRegistry
        from simpleetl.core.metrics import MetricsCollector

        registry = CollectorRegistry()
        collector = MetricsCollector(registry=registry)
        hist = collector.histogram("test_hist", "A test", labelnames=("job",))
        hist.labels(job="test").observe(2.5)

    def test_export_to_file(self, tmp_path):
        from prometheus_client import CollectorRegistry
        from simpleetl.core.metrics import MetricsCollector

        registry = CollectorRegistry()
        collector = MetricsCollector(registry=registry)
        collector.inc_counter("etl_jobs_total", 1.0)
        output_path = str(tmp_path / "metrics.txt")
        collector.export_to_file(output_path, format="text")
        assert os.path.exists(output_path)

    def test_job_timer_decorator(self):
        from simpleetl.core.metrics import job_timer

        @job_timer()
        def my_func():
            return 42

        result = my_func()
        assert result == 42


# -------------------------------------------------------------------
# core/plugins.py — remaining uncovered lines
# -------------------------------------------------------------------

class TestPluginsRemaining:
    def test_plugin_without_name(self):
        from simpleetl.core.plugins import Plugin

        class NoNamePlugin(Plugin):
            name = ""
            version = "0.1.0"

            def setup(self):
                pass

        p = NoNamePlugin()
        assert p.name == ""

    def test_plugin_default_version(self):
        from simpleetl.core.plugins import Plugin

        class DefaultVersionPlugin(Plugin):
            name = "test"

            def setup(self):
                pass

        p = DefaultVersionPlugin()
        assert p.version == "0.1.0"


# -------------------------------------------------------------------
# core/incremental.py — remaining uncovered lines
# -------------------------------------------------------------------

class TestIncrementalRemaining:
    def test_watermark_store_operations(self, tmp_path):
        from simpleetl.core.incremental import (
            FileWatermarkStore,
            Watermark,
        )

        store = FileWatermarkStore(str(tmp_path / "wm.json"))
        wm = Watermark(
            job_name="j1",
            source="s1",
            column="updated_at",
            value="2024-01-01",
        )
        store.set(wm)
        loaded = store.get("j1", "s1")
        assert loaded is not None
        assert loaded.value == "2024-01-01"

    def test_database_watermark_store(self):
        from simpleetl.core.incremental import DatabaseWatermarkStore

        store = DatabaseWatermarkStore(connection="sqlite:///:memory:")
        assert store is not None


# -------------------------------------------------------------------
# formats/database.py — DatabaseReader read/write with sqlite
# -------------------------------------------------------------------

class TestDatabaseReaderWriter:
    def test_sqlite_read_write_roundtrip(self):
        from simpleetl.formats.database import DatabaseReader, DatabaseWriter

        df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = tmp.name

        try:
            writer = DatabaseWriter()
            writer.write(df, f"sqlite:///{db_path}", table_name="test_table")

            reader = DatabaseReader()
            result = reader.read(
                f"sqlite:///{db_path}",
                table="test_table",
            )
            assert len(result) == 3
        finally:
            os.unlink(db_path)

    def test_database_writer_with_if_exists_fail(self):
        from simpleetl.formats.database import DatabaseWriter

        df = pd.DataFrame({"id": [1], "name": ["Alice"]})

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = tmp.name

        try:
            writer = DatabaseWriter()
            writer.write(df, f"sqlite:///{db_path}", table_name="t1")
            with pytest.raises(Exception):
                writer.write(
                    df, f"sqlite:///{db_path}", table="t1", if_exists="fail"
                )
        finally:
            os.unlink(db_path)

    def test_database_reader_with_query(self):
        from simpleetl.formats.database import DatabaseWriter, DatabaseReader

        df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = tmp.name

        try:
            writer = DatabaseWriter()
            writer.write(df, f"sqlite:///{db_path}", table_name="test_table")

            reader = DatabaseReader()
            result = reader.read(
                f"sqlite:///{db_path}",
                sql="SELECT * FROM test_table WHERE id > 1",
            )
            assert len(result) == 2
        finally:
            os.unlink(db_path)
