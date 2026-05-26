"""
Final coverage push — targets remaining gaps in:
- core/metrics.py: time_function, context_timer, register_custom_metric, export, get_metrics
- core/security.py: mask methods with NaN, partial masking, ColumnEncryptor ImportError,
  AuditLogger file writing, get_audit_trail filtering
- core/lineage.py: LineageHook._extract_shape, LineageHook.execute
- core/incremental.py: remaining edge cases
- formats/base.py: abstract class verification
- formats/csv.py, json.py, excel.py, orc.py: remaining edge cases
- platforms/base.py: abstract class verification
"""

import pytest
import pandas as pd
import os
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone, timedelta
from prometheus_client import CollectorRegistry


# -------------------------------------------------------------------
# core/metrics.py — remaining uncovered lines
# -------------------------------------------------------------------

class TestMetricsRemaining:
    def test_export_to_file_text(self, tmp_path):
        from simpleetl.core.metrics import MetricsCollector
        registry = CollectorRegistry()
        collector = MetricsCollector(registry=registry)
        collector.inc_counter("my_ctr", 3.0)
        output = str(tmp_path / "metrics.txt")
        collector.export_to_file(output, format="text")
        assert os.path.exists(output)

    def test_get_metrics_text(self):
        from simpleetl.core.metrics import MetricsCollector
        registry = CollectorRegistry()
        collector = MetricsCollector(registry=registry)
        collector.inc_counter("counter_a", 1.0)
        result = collector.get_metrics(output_format="text")
        assert isinstance(result, str)

    def test_get_metrics_invalid_format_raises(self):
        from simpleetl.core.metrics import MetricsCollector
        registry = CollectorRegistry()
        collector = MetricsCollector(registry=registry)
        with pytest.raises(ValueError, match="Unsupported output format"):
            collector.get_metrics(output_format="xml")

    def test_register_custom_metric(self):
        from simpleetl.core.metrics import MetricsCollector
        registry = CollectorRegistry()
        collector = MetricsCollector(registry=registry)
        collector.register_custom_metric("custom", "counter", "A custom metric")


# -------------------------------------------------------------------
# core/security.py — remaining uncovered lines
# -------------------------------------------------------------------

class TestSecurityEdgeCases:
    def test_mask_pii_with_hash(self):
        from simpleetl.core.security import mask_pii
        df = pd.DataFrame({"email": ["test@example.com"]})
        result = mask_pii(df, {"email": "email"}, method="hash")
        assert result["email"].iloc[0] != "test@example.com"
        assert len(result["email"].iloc[0]) == 64  # SHA-256 hex

    def test_mask_pii_with_partial(self):
        from simpleetl.core.security import mask_pii
        df = pd.DataFrame({"email": ["user@example.com"]})
        result = mask_pii(df, {"email": "email"}, method="partial")
        assert result["email"].iloc[0] != "user@example.com"

    def test_mask_pii_with_tokenize(self):
        from simpleetl.core.security import mask_pii, _reset_token_cache
        _reset_token_cache()
        df = pd.DataFrame({"ssn": ["123-45-6789"]})
        result = mask_pii(df, {"ssn": "ssn"}, method="tokenize")
        assert result["ssn"].iloc[0] != "123-45-6789"
        assert "<SSN_" in result["ssn"].iloc[0]

    def test_mask_pii_invalid_method(self):
        from simpleetl.core.security import mask_pii
        df = pd.DataFrame({"a": ["x"]})
        with pytest.raises(ValueError, match="Unsupported masking method"):
            mask_pii(df, {"a": "email"}, method="invalid")

    def test_mask_pii_missing_column_skipped(self):
        from simpleetl.core.security import mask_pii
        df = pd.DataFrame({"a": ["x"]})
        result = mask_pii(df, {"nonexistent": "email"}, method="redact")
        assert list(result.columns) == ["a"]

    def test_mask_redact_with_nan(self):
        from simpleetl.core.security import _mask_redact
        result = _mask_redact(float("nan"), "email")
        assert pd.isna(result)

    def test_mask_hash_with_nan(self):
        from simpleetl.core.security import _mask_hash
        result = _mask_hash(float("nan"))
        assert pd.isna(result)

    def test_mask_tokenize_with_nan(self):
        from simpleetl.core.security import _mask_tokenize, _reset_token_cache
        _reset_token_cache()
        result = _mask_tokenize(float("nan"), "email")
        assert pd.isna(result)

    def test_mask_partial_email(self):
        from simpleetl.core.security import _mask_partial
        result = _mask_partial("user@example.com", "email")
        assert result != "user@example.com"

    def test_mask_partial_phone(self):
        from simpleetl.core.security import _mask_partial
        result = _mask_partial("555-123-4567", "phone")
        assert result != "555-123-4567"

    def test_mask_partial_credit_card(self):
        from simpleetl.core.security import _mask_partial
        result = _mask_partial("4111-1111-1111-1111", "credit_card")
        assert result != "4111-1111-1111-1111"

    def test_mask_partial_short_text(self):
        from simpleetl.core.security import _mask_partial
        result = _mask_partial("ab", "other")
        assert result == "a***"

    def test_mask_partial_generic(self):
        from simpleetl.core.security import _mask_partial
        result = _mask_partial("hello", "other")
        assert result == "h***o"

    def test_detect_pii_values_empty_series(self):
        from simpleetl.core.security import detect_pii_values
        df = pd.DataFrame({"col": [None, None]})
        result = detect_pii_values(df)
        assert result is not None

    def test_detect_pii_values_missing_column(self):
        from simpleetl.core.security import detect_pii_values
        df = pd.DataFrame({"a": ["test@example.com"]})
        result = detect_pii_values(df, columns=["nonexistent"])
        assert result is not None

    def test_column_encryptor_import_error(self):
        from simpleetl.core.security import ColumnEncryptor
        with patch.dict("sys.modules", {"cryptography": None, "cryptography.fernet": None}):
            with pytest.raises(ImportError, match="cryptography"):
                ColumnEncryptor(key=None)

    def test_audit_logger_with_file(self, tmp_path):
        from simpleetl.core.security import AuditLogger
        log_file = str(tmp_path / "audit.jsonl")
        logger = AuditLogger(log_file=log_file)
        logger.log_access(user="test", action="read", source="src")
        logger.log_transformation(
            user="test", job_name="j1", operation="filter", source="src", destination="dst"
        )
        assert os.path.exists(log_file)
        with open(log_file) as f:
            lines = f.readlines()
        assert len(lines) == 2

    def test_audit_trail_filtering(self):
        from simpleetl.core.security import AuditLogger
        logger = AuditLogger()
        logger.log_access(user="u1", action="read", source="src1")
        logger.log_access(user="u2", action="write", source="src2")
        logger.log_access(user="u3", action="read", source="src1")

        filtered = logger.get_audit_trail(source="src1")
        assert len(filtered) == 2

    def test_audit_trail_time_filtering(self):
        from simpleetl.core.security import AuditLogger
        logger = AuditLogger()
        logger.log_access(user="u1", action="read", source="src")

        now = datetime.now(timezone.utc)
        filtered = logger.get_audit_trail(
            start_time=now - timedelta(hours=1),
            end_time=now + timedelta(hours=1),
        )
        assert len(filtered) >= 1

    def test_audit_trail_time_filtering_empty(self):
        from simpleetl.core.security import AuditLogger
        logger = AuditLogger()
        logger.log_access(user="u1", action="read", source="src")

        now = datetime.now(timezone.utc)
        filtered = logger.get_audit_trail(
            start_time=now + timedelta(hours=1),
            end_time=now + timedelta(hours=2),
        )
        assert len(filtered) == 0

    def test_audit_logger_file_write_error(self, tmp_path):
        from simpleetl.core.security import AuditLogger
        log_file = str(tmp_path / "audit.jsonl")
        logger = AuditLogger(log_file=log_file)
        logger.log_access(user="u1", action="read", source="src")
        os.chmod(log_file, 0o444)
        try:
            logger.log_access(user="u2", action="write", source="src2")
        finally:
            os.chmod(log_file, 0o644)


# -------------------------------------------------------------------
# core/lineage.py — LineageHook coverage
# -------------------------------------------------------------------

class TestLineageHook:
    def test_lineage_hook_execute(self):
        from simpleetl.core.lineage import LineageHook, HookContext

        hook = LineageHook(job_name="test_job")
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

        ctx_pre = HookContext(
            job=None, phase="pre_extract", data=None, metadata={},
        )
        hook.execute(ctx_pre)

        ctx_post = HookContext(
            job=None, phase="post_extract", data=df, metadata={"extracted_rows": 3},
        )
        hook.execute(ctx_post)

    def test_lineage_hook_with_job_name_from_config(self):
        from simpleetl.core.lineage import LineageHook, HookContext

        hook = LineageHook()
        mock_job = MagicMock()
        mock_job.config.name = "my_job"
        df = pd.DataFrame({"x": [1]})

        ctx_pre = HookContext(
            job=mock_job, phase="pre_extract", data=None, metadata={},
        )
        hook.execute(ctx_pre)

        ctx = HookContext(
            job=mock_job, phase="post_extract", data=df, metadata={},
        )
        hook.execute(ctx)

    def test_lineage_hook_non_post_phase(self):
        from simpleetl.core.lineage import LineageHook, HookContext

        hook = LineageHook()
        ctx = HookContext(
            job=None, phase="some_other_phase", data=None, metadata={},
        )
        hook.execute(ctx)

    def test_lineage_hook_extract_shape_non_dataframe(self):
        from simpleetl.core.lineage import LineageHook

        hook = LineageHook()
        result = hook._extract_shape([1, 2, 3])
        assert result == (3, {})

    def test_lineage_hook_extract_shape_none(self):
        from simpleetl.core.lineage import LineageHook

        hook = LineageHook()
        result = hook._extract_shape(None)
        assert result == (0, {})

    def test_lineage_hook_extract_shape_no_len(self):
        from simpleetl.core.lineage import LineageHook

        hook = LineageHook()
        result = hook._extract_shape(42)
        assert result == (0, {})


# -------------------------------------------------------------------
# core/incremental.py — remaining edge cases
# -------------------------------------------------------------------

class TestIncrementalEdgeCases:
    def test_file_watermark_store_get_missing(self, tmp_path):
        from simpleetl.core.incremental import FileWatermarkStore, Watermark

        store = FileWatermarkStore(str(tmp_path / "wm.json"))
        wm = Watermark(
            job_name="j1", source="s1", column="updated_at", value="2024-01-01",
        )
        store.set(wm)
        result = store.get("nonexistent", "source")
        assert result is None

    def test_file_watermark_store_overwrite(self, tmp_path):
        from simpleetl.core.incremental import FileWatermarkStore, Watermark

        store = FileWatermarkStore(str(tmp_path / "wm2.json"))
        wm1 = Watermark(job_name="j1", source="s1", column="c", value="v1")
        wm2 = Watermark(job_name="j1", source="s1", column="c", value="v2")
        store.set(wm1)
        store.set(wm2)
        result = store.get("j1", "s1")
        assert result.value == "v2"

    def test_database_watermark_store_operations(self):
        import sqlalchemy
        from simpleetl.core.incremental import DatabaseWatermarkStore, Watermark

        engine = sqlalchemy.create_engine("sqlite:///:memory:")
        store = DatabaseWatermarkStore(connection=engine)
        wm = Watermark(
            job_name="j1", source="s1", column="updated_at", value="2024-01-01",
        )
        store.set(wm)
        result = store.get("j1", "s1")
        assert result is not None
        assert result.value == "2024-01-01"

    def test_database_watermark_store_missing(self):
        import sqlalchemy
        from simpleetl.core.incremental import DatabaseWatermarkStore

        engine = sqlalchemy.create_engine("sqlite:///:memory:")
        store = DatabaseWatermarkStore(connection=engine)
        result = store.get("nonexistent", "source")
        assert result is None

    def test_watermark_inequality(self):
        from simpleetl.core.incremental import Watermark

        wm1 = Watermark(job_name="j1", source="s1", column="c", value="v1")
        wm2 = Watermark(job_name="j1", source="s1", column="c", value="v2")
        assert wm1 != wm2

    def test_watermark_not_equal_to_non_watermark(self):
        from simpleetl.core.incremental import Watermark

        wm = Watermark(job_name="j1", source="s1", column="c", value="v")
        assert wm != "not a watermark"

    def test_watermark_repr(self):
        from simpleetl.core.incremental import Watermark

        wm = Watermark(job_name="j1", source="s1", column="c", value="v")
        r = repr(wm)
        assert "Watermark" in r


# -------------------------------------------------------------------
# formats/base.py — abstract class verification
# -------------------------------------------------------------------

class TestBaseFormatDefaults:
    def test_base_reader_is_abstract(self):
        from simpleetl.formats.base import DataReader

        with pytest.raises(TypeError):
            DataReader()

    def test_base_writer_is_abstract(self):
        from simpleetl.formats.base import DataWriter

        with pytest.raises(TypeError):
            DataWriter()


# -------------------------------------------------------------------
# formats/csv.py — remaining edge cases
# -------------------------------------------------------------------

class TestCSVEdgeCases:
    def test_csv_reader_with_encoding(self, tmp_path):
        from simpleetl.formats.csv import CSVReader

        path = tmp_path / "test.csv"
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        df.to_csv(path, index=False)

        reader = CSVReader()
        result = reader.read(str(path), encoding="utf-8")
        assert len(result) == 2

    def test_csv_writer_with_encoding(self, tmp_path):
        from simpleetl.formats.csv import CSVWriter

        path = tmp_path / "out.csv"
        df = pd.DataFrame({"a": [1, 2]})
        writer = CSVWriter()
        writer.write(df, str(path), encoding="utf-8")
        assert path.exists()


# -------------------------------------------------------------------
# formats/json.py — remaining edge cases
# -------------------------------------------------------------------

class TestJSONEdgeCases:
    def test_json_reader_with_lines(self, tmp_path):
        from simpleetl.formats.json import JSONReader

        path = tmp_path / "test.jsonl"
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        df.to_json(path, orient="records", lines=True)

        reader = JSONReader()
        result = reader.read(str(path), lines=True)
        assert len(result) == 2

    def test_json_writer_with_lines(self, tmp_path):
        from simpleetl.formats.json import JSONWriter

        path = tmp_path / "out.jsonl"
        df = pd.DataFrame({"a": [1, 2]})
        writer = JSONWriter()
        writer.write(df, str(path), lines=True)
        assert path.exists()


# -------------------------------------------------------------------
# formats/excel.py — remaining edge cases
# -------------------------------------------------------------------

class TestExcelEdgeCases:
    def test_excel_reader_basic(self, tmp_path):
        from simpleetl.formats.excel import ExcelReader, ExcelWriter

        path = tmp_path / "test.xlsx"
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        writer = ExcelWriter()
        writer.write(df, str(path))

        reader = ExcelReader()
        result = reader.read(str(path))
        assert len(result) == 2

    def test_excel_writer_with_sheet_name(self, tmp_path):
        from simpleetl.formats.excel import ExcelWriter

        path = tmp_path / "sheet.xlsx"
        df = pd.DataFrame({"x": [1]})
        writer = ExcelWriter()
        writer.write(df, str(path), sheet_name="MySheet")
        assert path.exists()


# -------------------------------------------------------------------
# formats/orc.py — remaining edge cases
# -------------------------------------------------------------------

class TestORCEdgeCases:
    def test_orc_reader_basic(self, tmp_path):
        from simpleetl.formats.orc import OrcReader, OrcWriter

        path = tmp_path / "test.orc"
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        writer = OrcWriter()
        writer.write(df, str(path))

        reader = OrcReader()
        result = reader.read(str(path))
        assert len(result) == 2


# -------------------------------------------------------------------
# platforms/base.py — abstract class verification
# -------------------------------------------------------------------

class TestPlatformBase:
    def test_base_platform_is_abstract(self):
        from simpleetl.platforms.base import PlatformRunner

        with pytest.raises(TypeError):
            PlatformRunner()
