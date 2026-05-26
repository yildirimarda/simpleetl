"""
Tests for filesystem abstraction and cloud storage support.
"""

import os
import tempfile
from unittest.mock import MagicMock, patch
from io import BytesIO

import pandas as pd
import pytest

from simpleetl.core.filesystem import (
    get_filesystem,
    is_cloud_path,
    split_path,
    get_cloud_type,
)
from simpleetl.formats import (
    CSVReader,
    CSVWriter,
    JSONReader,
    JSONWriter,
    ParquetReader,
    ParquetWriter,
    AvroReader,
    AvroWriter,
    OrcReader,
    OrcWriter,
    XMLReader,
    XMLWriter,
    ExcelReader,
    ExcelWriter,
    FormatFactory,
)


class TestIsCloudPath:
    """Test cloud path detection."""

    def test_s3_path(self):
        assert is_cloud_path("s3://my-bucket/data/file.csv") is True

    def test_gs_path(self):
        assert is_cloud_path("gs://my-bucket/data/file.csv") is True

    def test_gcs_path(self):
        assert is_cloud_path("gcs://my-bucket/data/file.csv") is True

    def test_abfs_path(self):
        assert is_cloud_path(
            "abfs://container@account.dfs.core.windows.net/data/file.csv"
        ) is True

    def test_abfss_path(self):
        assert is_cloud_path(
            "abfss://container@account.dfs.core.windows.net/data/file.csv"
        ) is True

    def test_local_path(self):
        assert is_cloud_path("/local/path/file.csv") is False

    def test_relative_path(self):
        assert is_cloud_path("data/file.csv") is False

    def test_empty_string(self):
        assert is_cloud_path("") is False


class TestSplitPath:
    """Test path splitting for cloud and local paths."""

    def test_s3_split(self):
        bucket, prefix = split_path("s3://my-bucket/data/file.csv")
        assert bucket == "my-bucket"
        assert prefix == "data/file.csv"

    def test_s3_split_no_prefix(self):
        bucket, prefix = split_path("s3://my-bucket/")
        assert bucket == "my-bucket"
        assert prefix == ""

    def test_s3_split_root_only(self):
        bucket, prefix = split_path("s3://my-bucket")
        assert bucket == "my-bucket"
        assert prefix == ""

    def test_gs_split(self):
        bucket, prefix = split_path("gs://my-bucket/data/file.csv")
        assert bucket == "my-bucket"
        assert prefix == "data/file.csv"

    def test_abfss_split(self):
        bucket, prefix = split_path(
            "abfss://container@account.dfs.core.windows.net/data/file.csv"
        )
        assert bucket == "container@account.dfs.core.windows.net"
        assert prefix == "data/file.csv"

    def test_local_path(self):
        bucket, prefix = split_path("/local/path/file.csv")
        assert bucket == ""
        assert prefix == "/local/path/file.csv"


class TestGetCloudType:
    """Test cloud type detection."""

    def test_s3_type(self):
        assert get_cloud_type("s3://bucket/file.csv") == "s3"

    def test_gs_type(self):
        assert get_cloud_type("gs://bucket/file.csv") == "gs"

    def test_gcs_type(self):
        assert get_cloud_type("gcs://bucket/file.csv") == "gs"

    def test_abfs_type(self):
        assert get_cloud_type("abfs://container/path/file.csv") == "abfs"

    def test_abfss_type(self):
        assert get_cloud_type("abfss://container/path/file.csv") == "abfs"

    def test_local_type(self):
        assert get_cloud_type("/local/path/file.csv") is None


class TestGetFilesystem:
    """Test filesystem creation."""

    def test_local_filesystem(self):
        fs = get_filesystem("/local/path/")
        protocol = fs.protocol
        if isinstance(protocol, tuple):
            assert "file" in protocol
        else:
            assert protocol == "file"

    @patch("fsspec.filesystem")
    def test_s3_filesystem(self, mock_fs):
        mock_fs.return_value = MagicMock()
        get_filesystem("s3://bucket/path/")
        mock_fs.assert_called_once_with("s3")

    @patch("fsspec.filesystem")
    def test_gs_filesystem(self, mock_fs):
        mock_fs.return_value = MagicMock()
        get_filesystem("gs://bucket/path/")
        mock_fs.assert_called_once_with("gs")

    @patch("fsspec.filesystem")
    def test_abfs_filesystem(self, mock_fs):
        mock_fs.return_value = MagicMock()
        get_filesystem("abfss://container/path/")
        mock_fs.assert_called_once_with("abfs")


class TestFormatFactoryCloudPaths:
    """Test FormatFactory with cloud storage paths."""

    def test_get_reader_s3_csv(self):
        reader = FormatFactory.get_reader("s3://bucket/data.csv")
        assert isinstance(reader, CSVReader)

    def test_get_reader_s3_json(self):
        reader = FormatFactory.get_reader("s3://bucket/data.json")
        assert isinstance(reader, JSONReader)

    def test_get_reader_s3_parquet(self):
        reader = FormatFactory.get_reader("s3://bucket/data.parquet")
        assert isinstance(reader, ParquetReader)

    def test_get_reader_s3_avro(self):
        reader = FormatFactory.get_reader("s3://bucket/data.avro")
        assert isinstance(reader, AvroReader)

    def test_get_reader_s3_orc(self):
        reader = FormatFactory.get_reader("s3://bucket/data.orc")
        assert isinstance(reader, OrcReader)

    def test_get_reader_s3_xml(self):
        reader = FormatFactory.get_reader("s3://bucket/data.xml")
        assert isinstance(reader, XMLReader)

    def test_get_reader_s3_excel(self):
        reader = FormatFactory.get_reader("s3://bucket/data.xlsx")
        assert isinstance(reader, ExcelReader)

    def test_get_writer_s3_csv(self):
        writer = FormatFactory.get_writer("s3://bucket/data.csv")
        assert isinstance(writer, CSVWriter)

    def test_get_writer_s3_json(self):
        writer = FormatFactory.get_writer("s3://bucket/data.json")
        assert isinstance(writer, JSONWriter)

    def test_get_writer_s3_parquet(self):
        writer = FormatFactory.get_writer("s3://bucket/data.parquet")
        assert isinstance(writer, ParquetWriter)

    def test_get_writer_gs_csv(self):
        writer = FormatFactory.get_writer("gs://bucket/data.csv")
        assert isinstance(writer, CSVWriter)

    def test_get_writer_abfs_csv(self):
        writer = FormatFactory.get_writer(
            "abfss://container@account.dfs.core.windows.net/data.csv"
        )
        assert isinstance(writer, CSVWriter)

    def test_get_reader_no_extension_cloud_raises(self):
        with pytest.raises(ValueError, match="Cannot detect format"):
            FormatFactory.get_reader("s3://bucket/data")

    def test_get_writer_no_extension_cloud_raises(self):
        with pytest.raises(ValueError, match="Cannot detect format"):
            FormatFactory.get_writer("s3://bucket/data")

    def test_get_reader_no_extension_local_defaults_csv(self):
        reader = FormatFactory.get_reader("data")
        assert isinstance(reader, CSVReader)

    def test_get_writer_no_extension_local_defaults_csv(self):
        writer = FormatFactory.get_writer("data")
        assert isinstance(writer, CSVWriter)

    def test_detect_format_s3_csv(self):
        info = FormatFactory.detect_format("s3://bucket/data.csv")
        assert info["format"] == "csv"
        assert info["extension"] == ".csv"

    def test_detect_format_gs_parquet(self):
        info = FormatFactory.detect_format("gs://bucket/data.parquet")
        assert info["format"] == "parquet"
        assert info["extension"] == ".parquet"

    def test_detect_format_with_query_string(self):
        info = FormatFactory.detect_format(
            "s3://bucket/data.csv?versionId=abc123"
        )
        assert info["format"] == "csv"
        assert info["extension"] == ".csv"

    def test_detect_format_with_fragment(self):
        info = FormatFactory.detect_format("s3://bucket/data.json#section")
        assert info["format"] == "json"
        assert info["extension"] == ".json"

    def test_supported_formats(self):
        formats = FormatFactory.supported_formats()
        assert "csv" in formats
        assert "json" in formats
        assert "parquet" in formats
        assert "avro" in formats
        assert "orc" in formats
        assert "xml" in formats
        assert "xlsx" in formats


class TestCSVCloudRoundTrip:
    """Test CSV reader/writer with mocked cloud filesystem."""

    def test_csv_read_from_cloud(self):
        """Test CSV reading from cloud storage with mocked filesystem."""
        pd.DataFrame(
            {"name": ["Alice", "Bob"], "age": [25, 30]}
        )
        csv_content = "name,age\nAlice,25\nBob,30\n"

        mock_fs = MagicMock()
        mock_fs.open.return_value.__enter__ = MagicMock(
            return_value=BytesIO(csv_content.encode())
        )
        mock_fs.open.return_value.__exit__ = MagicMock(return_value=False)

        reader = CSVReader()
        result = reader.read("s3://bucket/data.csv", filesystem=mock_fs)

        assert len(result) == 2
        assert list(result.columns) == ["name", "age"]
        mock_fs.open.assert_called_once_with("s3://bucket/data.csv", "r")

    def test_csv_write_to_cloud(self):
        """Test CSV writing to cloud storage with mocked filesystem."""
        df = pd.DataFrame(
            {"name": ["Alice", "Bob"], "age": [25, 30]}
        )

        mock_fs = MagicMock()
        mock_file = MagicMock()
        mock_fs.open.return_value.__enter__ = MagicMock(
            return_value=mock_file
        )
        mock_fs.open.return_value.__exit__ = MagicMock(return_value=False)

        writer = CSVWriter()
        writer.write(df, "s3://bucket/data.csv", filesystem=mock_fs)

        mock_fs.open.assert_called_once_with("s3://bucket/data.csv", "w")


class TestJSONCloudRoundTrip:
    """Test JSON reader/writer with mocked cloud filesystem."""

    def test_json_read_from_cloud(self):
        """Test JSON reading from cloud storage with mocked filesystem."""
        json_content = '[{"name":"Alice","age":25},{"name":"Bob","age":30}]'

        mock_fs = MagicMock()
        mock_fs.open.return_value.__enter__ = MagicMock(
            return_value=BytesIO(json_content.encode())
        )
        mock_fs.open.return_value.__exit__ = MagicMock(return_value=False)

        reader = JSONReader()
        result = reader.read("s3://bucket/data.json", filesystem=mock_fs)

        assert len(result) == 2
        mock_fs.open.assert_called_once_with("s3://bucket/data.json", "r")

    def test_json_write_to_cloud(self):
        """Test JSON writing to cloud storage with mocked filesystem."""
        df = pd.DataFrame(
            {"name": ["Alice", "Bob"], "age": [25, 30]}
        )

        mock_fs = MagicMock()
        mock_file = MagicMock()
        mock_fs.open.return_value.__enter__ = MagicMock(
            return_value=mock_file
        )
        mock_fs.open.return_value.__exit__ = MagicMock(return_value=False)

        writer = JSONWriter()
        writer.write(df, "s3://bucket/data.json", filesystem=mock_fs)

        mock_fs.open.assert_called_once_with("s3://bucket/data.json", "w")


def _aws_credentials_available():
    """Check if valid, non-expired AWS credentials are available."""
    try:
        import boto3
        session = boto3.Session()
        credentials = session.get_credentials()
        if credentials is None:
            return False
        creds = credentials.get_frozen_credentials()
        if not creds or not creds.access_key:
            return False
        # Validate credentials are not expired by making a lightweight STS call
        sts = session.client("sts", region_name="us-east-1")
        sts.get_caller_identity()
        return True
    except Exception:
        return False


@pytest.mark.skipif(
    not _aws_credentials_available(),
    reason="Valid AWS credentials not available (may be expired)"
)
class TestParquetCloudRoundTrip:
    """Test Parquet reader/writer with mocked cloud filesystem."""

    def test_parquet_read_from_cloud(self):
        """Test Parquet reading from cloud storage with mocked filesystem."""
        from unittest.mock import patch
        import pyarrow.parquet as pq

        df = pd.DataFrame(
            {"name": ["Alice", "Bob"], "age": [25, 30]}
        )

        with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp:
            df.to_parquet(tmp.name, index=False)
            with patch('simpleetl.formats.parquet.pq.ParquetFile') as mock_pf:
                mock_pf.return_value.read.return_value = (
                    pq.ParquetFile(tmp.name).read()
                )
                reader = ParquetReader()
                result = reader.read("s3://bucket/data.parquet")

                assert len(result) == 2

    def test_parquet_write_to_cloud(self):
        """Test Parquet writing to cloud storage with mocked filesystem."""
        from unittest.mock import patch, MagicMock

        df = pd.DataFrame(
            {"name": ["Alice", "Bob"], "age": [25, 30]}
        )

        with patch('simpleetl.formats.parquet.pq.ParquetWriter') as mock_writer:
            mock_writer.return_value = MagicMock()
            writer = ParquetWriter()
            writer.write(df, "s3://bucket/data.parquet")
            mock_writer.assert_called_once()


class TestAvroCloudRoundTrip:
    """Test Avro reader/writer with mocked cloud filesystem."""

    def test_avro_read_from_cloud(self):
        """Test Avro reading from cloud storage with mocked filesystem."""
        import fastavro
        from io import BytesIO as AvroBytesIO

        df = pd.DataFrame(
            {"name": ["Alice", "Bob"], "age": [25, 30]}
        )
        records = df.to_dict(orient="records")
        schema = {
            "type": "record",
            "name": "Record",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "age", "type": "long"},
            ],
        }

        buffer = AvroBytesIO()
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
        mock_fs.open.assert_called_once_with("s3://bucket/data.avro", "rb")

    def test_avro_write_to_cloud(self):
        """Test Avro writing to cloud storage with mocked filesystem."""
        import fastavro

        df = pd.DataFrame(
            {"name": ["Alice", "Bob"], "age": [25, 30]}
        )

        mock_fs = MagicMock()
        mock_file = BytesIO()
        mock_fs.open.return_value.__enter__ = MagicMock(
            return_value=mock_file
        )
        mock_fs.open.return_value.__exit__ = MagicMock(return_value=False)

        writer = AvroWriter()
        writer.write(df, "s3://bucket/data.avro", filesystem=mock_fs)

        mock_fs.open.assert_called_once_with("s3://bucket/data.avro", "wb")
        # Verify Avro data was written to the BytesIO buffer
        mock_file.seek(0)
        reader = fastavro.reader(mock_file)
        records = list(reader)
        assert len(records) == 2


class TestOrcCloudRoundTrip:
    """Test ORC reader/writer with mocked cloud filesystem."""

    def test_orc_read_from_cloud(self):
        """Test ORC reading from cloud storage with mocked filesystem."""
        import pyarrow as pa

        df = pd.DataFrame(
            {"name": ["Alice", "Bob"], "age": [25, 30]}
        )
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
        """Test ORC writing to cloud storage with mocked filesystem."""
        df = pd.DataFrame(
            {"name": ["Alice", "Bob"], "age": [25, 30]}
        )

        mock_fs = MagicMock()

        with patch("pyarrow.orc.write_table") as mock_write:
            writer = OrcWriter()
            writer.write(df, "s3://bucket/data.orc", filesystem=mock_fs)

        mock_write.assert_called_once()


class TestExcelCloudRoundTrip:
    """Test Excel reader/writer with mocked cloud filesystem."""

    def test_excel_read_from_cloud(self):
        """Test Excel reading from cloud storage with mocked filesystem."""
        df = pd.DataFrame(
            {"name": ["Alice", "Bob"], "age": [25, 30]}
        )
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
        mock_fs.open.assert_called_once_with("s3://bucket/data.xlsx", "rb")

    def test_excel_write_to_cloud(self):
        """Test Excel writing to cloud storage with mocked filesystem."""
        df = pd.DataFrame(
            {"name": ["Alice", "Bob"], "age": [25, 30]}
        )

        mock_fs = MagicMock()
        mock_file = MagicMock()
        mock_fs.open.return_value.__enter__ = MagicMock(
            return_value=mock_file
        )
        mock_fs.open.return_value.__exit__ = MagicMock(return_value=False)

        writer = ExcelWriter()
        writer.write(df, "s3://bucket/data.xlsx", filesystem=mock_fs)

        mock_fs.open.assert_called_once_with("s3://bucket/data.xlsx", "wb")


class TestXMLCloudRoundTrip:
    """Test XML reader/writer with mocked cloud filesystem."""

    def test_xml_read_from_cloud(self):
        """Test XML reading from cloud storage with mocked filesystem."""
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
        mock_fs.open.assert_called_once_with(
            "s3://bucket/data.xml", "r", encoding="utf-8"
        )

    def test_xml_write_to_cloud(self):
        """Test XML writing to cloud storage with mocked filesystem."""
        df = pd.DataFrame(
            {"name": ["Alice", "Bob"], "age": [25, 30]}
        )

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


class TestLocalPathsStillWork:
    """Regression tests: ensure local paths still work after cloud changes."""

    def test_csv_local_roundtrip(self):
        df = pd.DataFrame(
            {"name": ["Alice", "Bob"], "age": [25, 30]}
        )
        with tempfile.NamedTemporaryFile(
            suffix=".csv", delete=False
        ) as f:
            temp_file = f.name
        try:
            writer = CSVWriter()
            writer.write(df, temp_file)
            reader = CSVReader()
            result = reader.read(temp_file)
            assert len(result) == 2
            pd.testing.assert_frame_equal(df, result)
        finally:
            os.unlink(temp_file)

    def test_json_local_roundtrip(self):
        df = pd.DataFrame(
            {"name": ["Alice", "Bob"], "age": [25, 30]}
        )
        with tempfile.NamedTemporaryFile(
            suffix=".json", delete=False
        ) as f:
            temp_file = f.name
        try:
            writer = JSONWriter()
            writer.write(df, temp_file)
            # JSONWriter defaults to lines=True, orient='records' (JSONL)
            reader = JSONReader()
            result = reader.read(temp_file, lines=True, orient='records')
            assert len(result) == 2
        finally:
            os.unlink(temp_file)

    def test_parquet_local_roundtrip(self):
        df = pd.DataFrame(
            {"name": ["Alice", "Bob"], "age": [25, 30]}
        )
        with tempfile.NamedTemporaryFile(
            suffix=".parquet", delete=False
        ) as f:
            temp_file = f.name
        try:
            writer = ParquetWriter()
            writer.write(df, temp_file)
            reader = ParquetReader()
            result = reader.read(temp_file)
            assert len(result) == 2
        finally:
            os.unlink(temp_file)

    def test_avro_local_roundtrip(self):
        df = pd.DataFrame(
            {"name": ["Alice", "Bob"], "age": [25, 30]}
        )
        with tempfile.NamedTemporaryFile(
            suffix=".avro", delete=False
        ) as f:
            temp_file = f.name
        try:
            writer = AvroWriter()
            writer.write(df, temp_file)
            reader = AvroReader()
            result = reader.read(temp_file)
            assert len(result) == 2
        finally:
            os.unlink(temp_file)

    def test_orc_local_roundtrip(self):
        df = pd.DataFrame(
            {"name": ["Alice", "Bob"], "age": [25, 30]}
        )
        with tempfile.NamedTemporaryFile(
            suffix=".orc", delete=False
        ) as f:
            temp_file = f.name
        try:
            writer = OrcWriter()
            writer.write(df, temp_file)
            reader = OrcReader()
            result = reader.read(temp_file)
            assert len(result) == 2
        finally:
            os.unlink(temp_file)

    def test_xml_local_roundtrip(self):
        df = pd.DataFrame(
            {"name": ["Alice", "Bob"], "age": [25, 30]}
        )
        with tempfile.NamedTemporaryFile(
            suffix=".xml", delete=False
        ) as f:
            temp_file = f.name
        try:
            writer = XMLWriter()
            writer.write(
                df, temp_file, root_element="data", record_element="record"
            )
            reader = XMLReader()
            result = reader.read(temp_file, root_element="data")
            assert len(result) == 2
        finally:
            os.unlink(temp_file)

    def test_excel_local_roundtrip(self):
        df = pd.DataFrame(
            {"name": ["Alice", "Bob"], "age": [25, 30]}
        )
        with tempfile.NamedTemporaryFile(
            suffix=".xlsx", delete=False
        ) as f:
            temp_file = f.name
        try:
            writer = ExcelWriter()
            writer.write(df, temp_file)
            reader = ExcelReader()
            result = reader.read(temp_file)
            assert len(result) == 2
        finally:
            os.unlink(temp_file)
