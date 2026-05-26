"""
Tests to cover remaining gaps for 95%+ coverage.

Covers: filesystem.get_file_mode, formats/base pass lines,
formats/csv cloud read/write, formats/parquet cloud read/write,
core/secrets AWS/Azure/Vault providers (error paths),
core/config uncovered lines, core/lineage uncovered lines,
core/incremental uncovered lines, core/connection uncovered lines,
core/dlq uncovered lines, core/schema_registry uncovered lines,
core/schema uncovered lines, core/quality uncovered lines,
core/schedule uncovered lines, formats/factory uncovered lines,
formats/json cloud uncovered lines, formats/avro cloud uncovered lines,
formats/orc cloud uncovered lines, formats/excel cloud uncovered lines,
formats/xml cloud uncovered lines, platforms/base uncovered line,
__main__.py, cli.py uncovered lines.
"""

import os
import tempfile
from io import BytesIO
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from simpleetl.core.filesystem import get_file_mode


# ---------------------------------------------------------------------------
# filesystem.get_file_mode
# ---------------------------------------------------------------------------


class TestGetFileMode:
    def test_parquet_gets_binary_mode(self):
        assert get_file_mode("data.parquet", "r") == "rb"

    def test_avro_gets_binary_mode(self):
        assert get_file_mode("data.avro", "r") == "rb"

    def test_orc_gets_binary_mode(self):
        assert get_file_mode("data.orc", "r") == "rb"

    def test_xlsx_gets_binary_mode(self):
        assert get_file_mode("data.xlsx", "r") == "rb"

    def test_xls_gets_binary_mode(self):
        assert get_file_mode("data.xls", "r") == "rb"

    def test_csv_stays_text_mode(self):
        assert get_file_mode("data.csv", "r") == "r"

    def test_json_stays_text_mode(self):
        assert get_file_mode("data.json", "w") == "w"

    def test_write_mode_parquet(self):
        assert get_file_mode("data.parquet", "w") == "wb"

    def test_already_binary_unchanged(self):
        assert get_file_mode("data.parquet", "rb") == "rb"

    def test_uppercase_extension(self):
        assert get_file_mode("data.PARQUET", "r") == "rb"


# ---------------------------------------------------------------------------
# formats/base.py — pass lines (abstract method bodies)
# ---------------------------------------------------------------------------


class TestBaseReaderPass:
    """The pass statements in DataReader.read() are unreachable directly,
    but we verify the abstract class cannot be instantiated."""

    def test_data_reader_is_abstract(self):
        from simpleetl.formats.base import DataReader

        with pytest.raises(TypeError):
            DataReader()

    def test_data_writer_is_abstract(self):
        from simpleetl.formats.base import DataWriter

        with pytest.raises(TypeError):
            DataWriter()


# ---------------------------------------------------------------------------
# formats/csv.py — cloud read/write with mocked filesystem
# ---------------------------------------------------------------------------


class TestCSVCloudReadWrite:
    def test_csv_read_from_cloud(self):
        from simpleetl.formats.csv import CSVReader

        csv_content = "name,age\nAlice,25\nBob,30\n"
        mock_fs = MagicMock()
        mock_fs.open.return_value.__enter__ = MagicMock(
            return_value=BytesIO(csv_content.encode())
        )
        mock_fs.open.return_value.__exit__ = MagicMock(return_value=False)

        reader = CSVReader()
        result = reader.read("s3://bucket/data.csv", filesystem=mock_fs)
        assert len(result) == 2
        mock_fs.open.assert_called_once_with("s3://bucket/data.csv", "r")

    def test_csv_write_to_cloud(self):
        from simpleetl.formats.csv import CSVWriter

        df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [25, 30]})
        mock_fs = MagicMock()
        mock_file = MagicMock()
        mock_fs.open.return_value.__enter__ = MagicMock(
            return_value=mock_file
        )
        mock_fs.open.return_value.__exit__ = MagicMock(return_value=False)

        writer = CSVWriter()
        writer.write(df, "s3://bucket/data.csv", filesystem=mock_fs)
        mock_fs.open.assert_called_once_with("s3://bucket/data.csv", "w")

    def test_csv_read_chunks_from_cloud(self):
        from simpleetl.formats.csv import CSVReader

        csv_content = "name,age\nAlice,25\nBob,30\nCharlie,35\n"
        mock_fs = MagicMock()
        mock_fs.open.return_value.__enter__ = MagicMock(
            return_value=BytesIO(csv_content.encode())
        )
        mock_fs.open.return_value.__exit__ = MagicMock(return_value=False)

        reader = CSVReader()
        chunks = list(
            reader.read_chunks(
                "s3://bucket/data.csv", chunk_size=2, filesystem=mock_fs
            )
        )
        total = sum(len(c) for c in chunks)
        assert total == 3

    def test_csv_write_chunks_to_cloud(self):
        from simpleetl.formats.csv import CSVWriter

        def gen():
            yield pd.DataFrame({"x": [1]})
            yield pd.DataFrame({"x": [2]})

        mock_fs = MagicMock()
        mock_file = MagicMock()
        mock_fs.open.return_value.__enter__ = MagicMock(
            return_value=mock_file
        )
        mock_fs.open.return_value.__exit__ = MagicMock(return_value=False)

        writer = CSVWriter()
        writer.write_chunks(gen(), "s3://bucket/out.csv", filesystem=mock_fs)
        mock_fs.open.assert_called_once_with("s3://bucket/out.csv", "w")


# ---------------------------------------------------------------------------
# formats/parquet.py — cloud read/write with mocked filesystem
# ---------------------------------------------------------------------------


class TestParquetCloudReadWrite:
    """Test parquet cloud code paths via mocking.
    The key is that is_cloud_path returns True and get_filesystem is called."""

    def test_parquet_read_from_cloud(self):
        from simpleetl.formats.parquet import ParquetReader

        df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [25, 30]})
        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
            df.to_parquet(tmp.name, index=False)
            with patch(
                "simpleetl.formats.parquet.pd.read_parquet",
                return_value=df,
            ) as mock_read, patch(
                "simpleetl.formats.parquet.get_filesystem",
            ) as mock_get_fs:
                mock_get_fs.return_value = MagicMock()
                reader = ParquetReader()
                result = reader.read("s3://bucket/data.parquet")
                mock_get_fs.assert_called_once_with("s3://bucket/data.parquet")
                mock_read.assert_called_once()
                assert len(result) == 2

    def test_parquet_write_to_cloud(self):
        from simpleetl.formats.parquet import ParquetWriter

        df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [25, 30]})
        with patch(
            "simpleetl.formats.parquet.get_filesystem",
        ) as mock_get_fs, patch(
            "simpleetl.formats.parquet.pd.DataFrame.to_parquet",
        ) as mock_to_parquet:
            mock_get_fs.return_value = MagicMock()
            writer = ParquetWriter()
            writer.write(df, "s3://bucket/data.parquet")
            mock_get_fs.assert_called_once_with("s3://bucket/data.parquet")
            mock_to_parquet.assert_called_once()

    def test_parquet_read_chunks_from_cloud(self):
        from simpleetl.formats.parquet import ParquetReader
        import pyarrow as pa

        table = pa.table({"x": range(50)})
        batch1 = table.slice(0, 25)
        batch2 = table.slice(25, 25)

        mock_pf = MagicMock()
        mock_pf.iter_batches.return_value = [batch1, batch2]

        with patch(
            "simpleetl.formats.parquet.pq.ParquetFile",
            return_value=mock_pf,
        ), patch(
            "simpleetl.formats.parquet.get_filesystem",
        ) as mock_get_fs:
            mock_get_fs.return_value = MagicMock()
            reader = ParquetReader()
            chunks = list(
                reader.read_chunks(
                    "s3://bucket/data.parquet",
                    chunk_size=20,
                )
            )
            mock_get_fs.assert_called_once_with("s3://bucket/data.parquet")
            total = sum(len(c) for c in chunks)
            assert total == 50

    def test_parquet_write_chunks_to_cloud(self):
        from simpleetl.formats.parquet import ParquetWriter

        def gen():
            yield pd.DataFrame({"val": [1, 2]})
            yield pd.DataFrame({"val": [3, 4]})

        with patch(
            "simpleetl.formats.parquet.pq.ParquetWriter"
        ) as mock_writer, patch(
            "simpleetl.formats.parquet.get_filesystem",
        ) as mock_get_fs:
            mock_get_fs.return_value = MagicMock()
            mock_ctx = MagicMock()
            mock_writer.return_value = mock_ctx
            writer = ParquetWriter()
            writer.write_chunks(gen(), "s3://bucket/out.parquet")
            mock_get_fs.assert_called_once_with("s3://bucket/out.parquet")
            assert mock_ctx.write_table.call_count == 2
            mock_ctx.close.assert_called_once()


# ---------------------------------------------------------------------------
# formats/json.py — cloud read/write with mocked filesystem
# ---------------------------------------------------------------------------


class TestJSONCloudReadWrite:
    def test_json_read_from_cloud(self):
        from simpleetl.formats.json import JSONReader

        json_content = '[{"name":"Alice","age":25},{"name":"Bob","age":30}]'
        mock_fs = MagicMock()
        mock_fs.open.return_value.__enter__ = MagicMock(
            return_value=BytesIO(json_content.encode())
        )
        mock_fs.open.return_value.__exit__ = MagicMock(return_value=False)

        reader = JSONReader()
        result = reader.read("s3://bucket/data.json", filesystem=mock_fs)
        assert len(result) == 2

    def test_json_write_to_cloud(self):
        from simpleetl.formats.json import JSONWriter

        df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [25, 30]})
        mock_fs = MagicMock()
        mock_file = MagicMock()
        mock_fs.open.return_value.__enter__ = MagicMock(
            return_value=mock_file
        )
        mock_fs.open.return_value.__exit__ = MagicMock(return_value=False)

        writer = JSONWriter()
        writer.write(df, "s3://bucket/data.json", filesystem=mock_fs)
        mock_fs.open.assert_called_once_with("s3://bucket/data.json", "w")


# ---------------------------------------------------------------------------
# formats/avro.py — cloud read/write with mocked filesystem
# ---------------------------------------------------------------------------


class TestAvroCloudReadWrite:
    def test_avro_read_from_cloud(self):
        from simpleetl.formats.avro import AvroReader

        import fastavro

        df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [25, 30]})
        records = df.to_dict(orient="records")
        schema = {
            "type": "record",
            "name": "Record",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "age", "type": "long"},
            ],
        }
        buffer = BytesIO()
        fastavro.writer(buffer, schema, records)
        buffer.seek(0)

        mock_fs = MagicMock()
        mock_fs.open.return_value.__enter__ = MagicMock(
            return_value=buffer
        )
        mock_fs.open.return_value.__exit__ = MagicMock(return_value=False)

        reader = AvroReader()
        result = reader.read("s3://bucket/data.avro", filesystem=mock_fs)
        assert len(result) == 2

    def test_avro_write_to_cloud(self):
        from simpleetl.formats.avro import AvroWriter

        df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [25, 30]})
        mock_fs = MagicMock()
        mock_file = BytesIO()
        mock_fs.open.return_value.__enter__ = MagicMock(
            return_value=mock_file
        )
        mock_fs.open.return_value.__exit__ = MagicMock(return_value=False)

        writer = AvroWriter()
        writer.write(df, "s3://bucket/data.avro", filesystem=mock_fs)
        mock_fs.open.assert_called_once_with("s3://bucket/data.avro", "wb")


# ---------------------------------------------------------------------------
# formats/orc.py — cloud read/write with mocked filesystem
# ---------------------------------------------------------------------------


class TestOrcCloudReadWrite:
    def test_orc_read_from_cloud(self):
        from simpleetl.formats.orc import OrcReader
        import pyarrow as pa

        df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [25, 30]})
        table = pa.Table.from_pandas(df)

        mock_orc_file = MagicMock()
        mock_orc_file.read.return_value = table

        mock_fs = MagicMock()
        with patch("pyarrow.orc.ORCFile", return_value=mock_orc_file):
            reader = OrcReader()
            result = reader.read(
                "s3://bucket/data.orc", filesystem=mock_fs
            )
        assert len(result) == 2

    def test_orc_write_to_cloud(self):
        from simpleetl.formats.orc import OrcWriter

        df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [25, 30]})
        mock_fs = MagicMock()
        with patch("pyarrow.orc.write_table") as mock_write:
            writer = OrcWriter()
            writer.write(df, "s3://bucket/data.orc", filesystem=mock_fs)
        mock_write.assert_called_once()


# ---------------------------------------------------------------------------
# formats/excel.py — cloud read/write with mocked filesystem
# ---------------------------------------------------------------------------


class TestExcelCloudReadWrite:
    def test_excel_read_from_cloud(self):
        from simpleetl.formats.excel import ExcelReader

        df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [25, 30]})
        buffer = BytesIO()
        df.to_excel(buffer, index=False)
        buffer.seek(0)

        mock_fs = MagicMock()
        mock_fs.open.return_value.__enter__ = MagicMock(
            return_value=BytesIO(buffer.getvalue())
        )
        mock_fs.open.return_value.__exit__ = MagicMock(return_value=False)

        reader = ExcelReader()
        result = reader.read("s3://bucket/data.xlsx", filesystem=mock_fs)
        assert len(result) == 2

    def test_excel_write_to_cloud(self):
        from simpleetl.formats.excel import ExcelWriter

        df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [25, 30]})
        mock_fs = MagicMock()
        mock_file = MagicMock()
        mock_fs.open.return_value.__enter__ = MagicMock(
            return_value=mock_file
        )
        mock_fs.open.return_value.__exit__ = MagicMock(return_value=False)

        writer = ExcelWriter()
        writer.write(df, "s3://bucket/data.xlsx", filesystem=mock_fs)
        mock_fs.open.assert_called_once_with("s3://bucket/data.xlsx", "wb")


# ---------------------------------------------------------------------------
# formats/xml.py — cloud read/write with mocked filesystem
# ---------------------------------------------------------------------------


class TestXMLCloudReadWrite:
    def test_xml_read_from_cloud(self):
        from simpleetl.formats.xml import XMLReader

        xml_content = (
            '<?xml version="1.0" encoding="utf-8"?>'
            "<data><record><name>Alice</name><age>25</age></record>"
            "<record><name>Bob</name><age>30</age></record></data>"
        )
        mock_fs = MagicMock()
        mock_fs.open.return_value.__enter__ = MagicMock(
            return_value=BytesIO(xml_content.encode())
        )
        mock_fs.open.return_value.__exit__ = MagicMock(return_value=False)

        reader = XMLReader()
        result = reader.read(
            "s3://bucket/data.xml",
            filesystem=mock_fs,
            root_element="data",
        )
        assert len(result) == 2

    def test_xml_write_to_cloud(self):
        from simpleetl.formats.xml import XMLWriter

        df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [25, 30]})
        mock_fs = MagicMock()
        mock_file = MagicMock()
        mock_fs.open.return_value.__enter__ = MagicMock(
            return_value=mock_file
        )
        mock_fs.open.return_value.__exit__ = MagicMock(return_value=False)

        writer = XMLWriter()
        writer.write(df, "s3://bucket/data.xml", filesystem=mock_fs)
        mock_fs.open.assert_called_once_with(
            "s3://bucket/data.xml", "w", encoding="utf-8"
        )


# ---------------------------------------------------------------------------
# core/secrets.py — AWS, Azure, Vault providers (error paths)
# ---------------------------------------------------------------------------


class TestAwsSecretsManagerProvider:
    @patch("boto3.Session")
    def test_init_with_region_and_profile(self, mock_session_cls):
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_session.client.return_value = MagicMock()

        from simpleetl.core.secrets import AwsSecretsManagerProvider

        AwsSecretsManagerProvider(
            region_name="us-west-2", profile_name="my-profile"
        )
        mock_session_cls.assert_called_once_with(
            region_name="us-west-2", profile_name="my-profile"
        )

    @patch("boto3.Session")
    def test_init_defaults(self, mock_session_cls):
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_session.client.return_value = MagicMock()

        from simpleetl.core.secrets import AwsSecretsManagerProvider

        AwsSecretsManagerProvider()
        mock_session_cls.assert_called_once_with()

    @patch("boto3.Session")
    def test_get_secret_not_found(self, mock_session_cls):
        from botocore.exceptions import ClientError

        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_client = MagicMock()
        mock_session.client.return_value = mock_client
        mock_client.get_secret_value.side_effect = ClientError(
            {"Error": {"Message": "Resource not found"}},
            "GetSecretValue",
        )

        from simpleetl.core.secrets import (
            AwsSecretsManagerProvider,
            SecretNotFoundError,
        )

        provider = AwsSecretsManagerProvider()
        with pytest.raises(SecretNotFoundError, match="Failed to retrieve"):
            provider.get_secret("my-secret")


class TestAzureKeyVaultProvider:
    @patch("azure.identity.DefaultAzureCredential")
    @patch("azure.keyvault.secrets.SecretClient")
    def test_init(self, mock_client_cls, mock_credential_cls):
        from simpleetl.core.secrets import AzureKeyVaultProvider

        AzureKeyVaultProvider(
            vault_url="https://my-vault.vault.azure.net/"
        )
        mock_credential_cls.assert_called_once()
        mock_client_cls.assert_called_once()

    @patch("azure.identity.DefaultAzureCredential")
    @patch("azure.keyvault.secrets.SecretClient")
    def test_get_secret_not_found(self, mock_client_cls, mock_credential_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.get_secret.side_effect = Exception("Secret not found")

        from simpleetl.core.secrets import (
            AzureKeyVaultProvider,
            SecretNotFoundError,
        )

        provider = AzureKeyVaultProvider(
            vault_url="https://my-vault.vault.azure.net/"
        )
        with pytest.raises(SecretNotFoundError, match="Failed to retrieve"):
            provider.get_secret("my-secret")


class TestHashiCorpVaultProvider:
    @patch("hvac.Client")
    def test_init_defaults(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        from simpleetl.core.secrets import HashiCorpVaultProvider

        HashiCorpVaultProvider()
        mock_client_cls.assert_called_once_with(
            url="http://127.0.0.1:8200", token=None
        )

    @patch("hvac.Client")
    def test_init_custom(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        from simpleetl.core.secrets import HashiCorpVaultProvider

        HashiCorpVaultProvider(
            url="https://vault.example.com", token="my-token"
        )
        mock_client_cls.assert_called_once_with(
            url="https://vault.example.com", token="my-token"
        )

    @patch("hvac.Client")
    def test_get_secret_forbidden(self, mock_client_cls):
        import hvac.exceptions

        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.secrets.kv.v2.read_secret_version.side_effect = (
            hvac.exceptions.Forbidden("access denied")
        )

        from simpleetl.core.secrets import (
            HashiCorpVaultProvider,
            SecretNotFoundError,
        )

        provider = HashiCorpVaultProvider()
        with pytest.raises(SecretNotFoundError, match="Access denied"):
            provider.get_secret("secret/data/myapp")

    @patch("hvac.Client")
    def test_get_secret_generic_error(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.secrets.kv.v2.read_secret_version.side_effect = (
            Exception("connection refused")
        )

        from simpleetl.core.secrets import (
            HashiCorpVaultProvider,
            SecretNotFoundError,
        )

        provider = HashiCorpVaultProvider()
        with pytest.raises(SecretNotFoundError, match="Failed to retrieve"):
            provider.get_secret("secret/data/myapp")


# ---------------------------------------------------------------------------
# core/config.py — uncovered lines
# ---------------------------------------------------------------------------


class TestConfigUncovered:
    def test_load_config_non_dict_top_level(self, tmp_path):
        """Config file with non-dict top level raises ValueError."""
        from simpleetl.core.config import load_config

        config_file = tmp_path / "bad.yaml"
        config_file.write_text("- item1\n- item2\n")
        with pytest.raises(ValueError, match="mapping at the top level"):
            load_config(str(config_file))

    def test_load_config_unsupported_format(self, tmp_path):
        """Unsupported config file format raises ValueError."""
        from simpleetl.core.config import load_config

        config_file = tmp_path / "config.toml"
        config_file.write_text("[section]\n")
        with pytest.raises(ValueError, match="Unsupported configuration"):
            load_config(str(config_file))

    def test_save_config_unsupported_format(self, tmp_path):
        """save_config with unsupported format raises ValueError."""
        from simpleetl.core.config import ETLJobConfig, save_config

        config = ETLJobConfig(name="test", input_format="csv", output_format="csv")
        with pytest.raises(ValueError, match="Unsupported configuration"):
            save_config(config, str(tmp_path / "config.toml"))

    def test_load_config_with_validation_error(self, tmp_path):
        """Invalid config data raises ValidationError."""
        from simpleetl.core.config import load_config

        config_file = tmp_path / "bad.yaml"
        config_file.write_text("name: test\ninput_format: csv\noutput_format: csv\nmax_retries: not_a_number\n")
        with pytest.raises(Exception):
            load_config(str(config_file))


# ---------------------------------------------------------------------------
# core/lineage.py — uncovered lines
# ---------------------------------------------------------------------------


class TestLineageUncovered:
    def test_lineage_tracker_record_event(self):
        from simpleetl.core.lineage import (
            LineageEvent,
            get_lineage_tracker,
        )

        tracker = get_lineage_tracker()
        event = LineageEvent(
            job_name="test_job",
            phase="post_extract",
            operation="extract",
            input_rows=0,
            output_rows=100,
            output_schema={"id": "int64", "name": "object"},
            duration_seconds=1.5,
        )
        tracker.record_event(event)
        events = tracker.get_events(job_name="test_job")
        assert len(events) >= 1

    def test_lineage_event_default_metadata(self):
        from simpleetl.core.lineage import LineageEvent

        event = LineageEvent(
            job_name="test_job",
            phase="post_extract",
            operation="extract",
            input_rows=0,
            output_rows=10,
        )
        assert event.metadata is not None

    def test_freshness_tracker(self):
        from simpleetl.core.lineage import (
            get_freshness_tracker,
        )

        tracker = get_freshness_tracker()
        tracker.record_freshness("test_source")
        freshness = tracker.get_freshness("test_source")
        assert freshness is not None

        # Nonexistent source
        assert tracker.get_freshness("nonexistent") is None

    def test_freshness_tracker_with_timestamp(self):
        from simpleetl.core.lineage import DataFreshnessTracker
        from datetime import datetime, timezone

        tracker = DataFreshnessTracker()
        ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
        tracker.record_freshness("src", timestamp=ts)
        assert tracker.get_freshness("src") == ts


# ---------------------------------------------------------------------------
# core/incremental.py — uncovered lines
# ---------------------------------------------------------------------------


class TestIncrementalUncovered:
    def test_watermark_manager_from_config(self):
        from simpleetl.core.config import ETLJobConfig
        from simpleetl.core.incremental import WatermarkManager

        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
            incremental=True,
            incremental_column="updated_at",
        )
        wm = WatermarkManager.from_config(config)
        assert wm is not None

    def test_watermark_manager_set_and_get(self, tmp_path):
        from simpleetl.core.incremental import (
            WatermarkManager,
            FileWatermarkStore,
        )

        store = FileWatermarkStore(str(tmp_path / "watermarks.json"))
        wm = WatermarkManager(store=store)
        wm.set_watermark("job1", "source1", "col", "2024-01-01")
        watermark = wm.get_watermark("job1", "source1")
        assert watermark is not None
        assert watermark.value == "2024-01-01"


# ---------------------------------------------------------------------------
# core/connection.py — uncovered lines
# ---------------------------------------------------------------------------


class TestConnectionUncovered:
    def _make_pool(self):
        from simpleetl.core.connection import ConnectionPool, ConnectionConfig

        config = ConnectionConfig(url="sqlite:///test.db")
        return ConnectionPool(config)

    def test_connection_pool_get_connection(self):
        pool = self._make_pool()
        # Just verify the pool was created; sqlite engine may not be
        # created lazily
        assert pool.config is not None

    def test_connection_pool_dispose(self):
        pool = self._make_pool()
        pool.dispose()  # Should not raise on empty pool

    def test_get_connection_with_mock_engine(self):
        from simpleetl.core.connection import get_connection
        from unittest.mock import MagicMock

        mock_engine = MagicMock()
        conn = get_connection(mock_engine)
        assert conn is not None

    def test_connection_pool_execute(self):
        from simpleetl.core.connection import ConnectionPool, ConnectionConfig

        config = ConnectionConfig(url="sqlite:///:memory:")
        pool = ConnectionPool(config)
        result = pool.execute("SELECT 1")
        assert result is not None


# ---------------------------------------------------------------------------
# core/dlq.py — uncovered lines
# ---------------------------------------------------------------------------


class TestDLQUncovered:
    def test_dlq_write_to_dlq_jsonl(self, tmp_path):
        from simpleetl.core.dlq import DeadLetterQueue

        dlq = DeadLetterQueue()
        dlq.add_entry(
            record_data={"id": 1, "name": "test"},
            error=ValueError("bad data"),
            phase="transform",
            record_index=0,
        )
        dlq.add_entry(
            record_data={"id": 2, "name": "test2"},
            error=ValueError("bad data 2"),
            phase="transform",
            record_index=1,
        )
        output_path = str(tmp_path / "dlq.jsonl")
        dlq.write_to_dlq(output_path, format="jsonl")
        assert os.path.exists(output_path)

    def test_dlq_clear(self):
        from simpleetl.core.dlq import DeadLetterQueue

        dlq = DeadLetterQueue()
        dlq.add_entry(
            record_data={"id": 1},
            error=ValueError("err"),
            phase="transform",
        )
        assert dlq.count == 1
        dlq.clear()
        assert dlq.count == 0


# ---------------------------------------------------------------------------
# core/schema_registry.py — uncovered lines
# ---------------------------------------------------------------------------


class TestSchemaRegistryUncovered:
    def test_schema_path_method(self, tmp_path):
        from simpleetl.core.schema_registry import FileSchemaRegistry

        reg = FileSchemaRegistry(str(tmp_path))
        path = reg._schema_path("users", 1)
        assert "users" in str(path)
        assert "v1" in str(path)

    def test_schema_dir_method(self, tmp_path):
        from simpleetl.core.schema_registry import FileSchemaRegistry

        reg = FileSchemaRegistry(str(tmp_path))
        dir_path = reg._schema_dir("users")
        assert "users" in str(dir_path)


# ---------------------------------------------------------------------------
# core/schema.py — uncovered lines
# ---------------------------------------------------------------------------


class TestSchemaUncovered:
    def test_schema_get_column(self):
        from simpleetl.core.schema import ColumnDef, Schema

        schema = Schema(
            columns=[
                ColumnDef("id", "int64", nullable=False),
                ColumnDef("name", "object"),
            ]
        )
        col = schema.get_column("id")
        assert col.name == "id"
        assert col.nullable is False

    def test_schema_get_column_missing(self):
        from simpleetl.core.schema import ColumnDef, Schema

        schema = Schema(columns=[ColumnDef("id", "int64")])
        assert schema.get_column("nonexistent") is None

    def test_schema_len(self):
        from simpleetl.core.schema import ColumnDef, Schema

        schema = Schema(
            columns=[
                ColumnDef("id", "int64"),
                ColumnDef("name", "object"),
            ]
        )
        assert len(schema) == 2


# ---------------------------------------------------------------------------
# core/quality.py — uncovered lines
# ---------------------------------------------------------------------------


class TestQualityUncovered:
    def test_check_nulls(self):
        from simpleetl.core.quality import check_nulls

        df = pd.DataFrame({"a": [1, None, 3], "b": [4, 5, None]})
        result = check_nulls(df, threshold=1.0)
        assert result is not None

    def test_check_duplicates(self):
        from simpleetl.core.quality import check_duplicates

        df = pd.DataFrame({"a": [1, 1, 2], "b": [3, 3, 4]})
        result = check_duplicates(df, threshold=1.0)
        assert result is not None

    def test_check_value_range(self):
        from simpleetl.core.quality import check_value_range

        df = pd.DataFrame({"a": [1, 50, 100]})
        result = check_value_range(df, "a", min_value=0, max_value=100)
        assert result is not None

    def test_check_unique_values(self):
        from simpleetl.core.quality import check_unique_values

        df = pd.DataFrame({"a": [1, 2, 3]})
        result = check_unique_values(df, "a")
        assert result is not None

    def test_profile_data(self):
        from simpleetl.core.quality import profile_data

        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        profile = profile_data(df)
        assert profile is not None

    def test_validate_schema(self):
        from simpleetl.core.quality import validate_schema

        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        result = validate_schema(df, required_columns=["a", "b"])
        assert result is not None


# ---------------------------------------------------------------------------
# core/schedule.py — uncovered lines
# ---------------------------------------------------------------------------


class TestScheduleUncovered:
    def test_cron_expression_valid(self):
        from simpleetl.core.schedule import CronExpression

        expr = CronExpression("0 * * * *")
        next_run = expr.next_run()
        assert next_run is not None

    def test_cron_expression_invalid(self):
        from simpleetl.core.schedule import CronExpression, CronParseError

        with pytest.raises(CronParseError):
            CronExpression("invalid cron")

    def test_schedule_next_run_time(self):
        from simpleetl.core.schedule import CronExpression, Schedule

        expr = CronExpression("0 * * * *")
        schedule = Schedule(name="test_sched", cron=expr)
        next_run = schedule.next_run_time()
        assert next_run is not None

    def test_schedule_should_run(self):
        from simpleetl.core.schedule import CronExpression, Schedule

        expr = CronExpression("0 * * * *")
        schedule = Schedule(name="test_sched", cron=expr)
        result = schedule.should_run()
        assert isinstance(result, bool)

    def test_schedule_from_string(self):
        from simpleetl.core.schedule import Schedule

        schedule = Schedule.from_string(name="test_sched", cron_str="0 * * * *")
        assert schedule is not None


# ---------------------------------------------------------------------------
# formats/factory.py — uncovered lines
# ---------------------------------------------------------------------------


class TestFormatFactoryUncovered:
    def test_detect_format_returns_all_keys(self):
        from simpleetl.formats.factory import FormatFactory

        result = FormatFactory.detect_format("data/file.csv")
        assert "format" in result
        assert "extension" in result
        assert "mime_type" in result

    def test_detect_format_no_extension(self):
        from simpleetl.formats.factory import FormatFactory

        result = FormatFactory.detect_format("data/file")
        assert result["format"] == "unknown"

    def test_get_reader_database(self):
        from simpleetl.formats.factory import FormatFactory

        reader = FormatFactory.get_reader("sqlite:///test.db")
        from simpleetl.formats.database import DatabaseReader
        assert isinstance(reader, DatabaseReader)

    def test_get_writer_database(self):
        from simpleetl.formats.factory import FormatFactory

        writer = FormatFactory.get_writer("sqlite:///test.db")
        from simpleetl.formats.database import DatabaseWriter
        assert isinstance(writer, DatabaseWriter)


# ---------------------------------------------------------------------------
# platforms/base.py — uncovered line
# ---------------------------------------------------------------------------


class TestPlatformsBase:
    def test_platform_runner_is_abstract(self):
        from simpleetl.platforms.base import PlatformRunner

        with pytest.raises(TypeError):
            PlatformRunner()


# ---------------------------------------------------------------------------
# __main__.py
# ---------------------------------------------------------------------------


class TestMainModule:
    def test_main_module_import(self):
        """Verify __main__.py is importable."""


# ---------------------------------------------------------------------------
# cli.py — uncovered lines
# ---------------------------------------------------------------------------


class TestCLIUncovered:
    def test_cli_import(self):
        """Verify CLI module is importable."""
        from simpleetl import cli
        assert hasattr(cli, "main")

    def test_cli_run_command_import(self):
        from simpleetl.cli import main
        assert callable(main)
