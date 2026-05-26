"""
Extended tests for format readers/writers.

Covers: ParquetReader/Writer edge cases, CSVReader/Writer edge cases,
base DataReader/DataWriter default chunked methods, FormatFactory edge cases.
"""

import pandas as pd

from simpleetl.formats.csv import CSVReader, CSVWriter
from simpleetl.formats.parquet import ParquetReader, ParquetWriter
from simpleetl.formats.json import JSONReader, JSONWriter
from simpleetl.formats.base import DataReader, DataWriter
from simpleetl.formats.factory import FormatFactory


# ---------------------------------------------------------------------------
# CSV chunked reading/writing
# ---------------------------------------------------------------------------


class TestCSVChunked:
    def test_read_chunks_local(self, tmp_path):
        df = pd.DataFrame({"a": range(100), "b": range(100)})
        path = tmp_path / "test.csv"
        df.to_csv(path, index=False)

        reader = CSVReader()
        chunks = list(reader.read_chunks(str(path), chunk_size=30))
        total_rows = sum(len(c) for c in chunks)
        assert total_rows == 100
        assert len(chunks) >= 3

    def test_read_chunks_single_row(self, tmp_path):
        df = pd.DataFrame({"a": [1]})
        path = tmp_path / "single.csv"
        df.to_csv(path, index=False)

        reader = CSVReader()
        chunks = list(reader.read_chunks(str(path), chunk_size=10))
        assert len(chunks) == 1
        assert len(chunks[0]) == 1

    def test_write_chunks_local(self, tmp_path):
        dest = tmp_path / "output.csv"

        def chunk_generator():
            for i in range(3):
                yield pd.DataFrame({"x": [i]})

        writer = CSVWriter()
        writer.write_chunks(chunk_generator(), str(dest))

        result = pd.read_csv(dest)
        assert len(result) == 3

    def test_write_chunks_first_has_header(self, tmp_path):
        dest = tmp_path / "header_test.csv"

        def gen():
            yield pd.DataFrame({"col": [1]})
            yield pd.DataFrame({"col": [2]})

        writer = CSVWriter()
        writer.write_chunks(gen(), str(dest))

        content = dest.read_text()
        lines = content.strip().split("\n")
        assert lines[0] == "col"
        assert len(lines) == 3  # header + 2 data rows


# ---------------------------------------------------------------------------
# Parquet edge cases
# ---------------------------------------------------------------------------


class TestParquetExtended:
    def test_read_with_columns(self, tmp_path):
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4], "c": [5, 6]})
        path = tmp_path / "test.parquet"
        df.to_parquet(path)

        reader = ParquetReader()
        result = reader.read(str(path), columns=["a", "b"])
        assert list(result.columns) == ["a", "b"]

    def test_read_chunks_parquet(self, tmp_path):
        df = pd.DataFrame({"x": range(100)})
        path = tmp_path / "chunked.parquet"
        df.to_parquet(path)

        reader = ParquetReader()
        chunks = list(reader.read_chunks(str(path), chunk_size=30))
        total = sum(len(c) for c in chunks)
        assert total == 100

    def test_read_chunks_with_columns(self, tmp_path):
        df = pd.DataFrame({"a": range(50), "b": range(50), "c": range(50)})
        path = tmp_path / "chunked_cols.parquet"
        df.to_parquet(path)

        reader = ParquetReader()
        chunks = list(
            reader.read_chunks(str(path), chunk_size=20, columns=["a", "c"])
        )
        result = pd.concat(chunks, ignore_index=True)
        assert list(result.columns) == ["a", "c"]

    def test_writer_default_compression(self, tmp_path):
        df = pd.DataFrame({"x": [1, 2, 3]})
        path = tmp_path / "snappy.parquet"
        writer = ParquetWriter()
        writer.write(df, str(path))
        assert path.exists()

    def test_writer_custom_compression(self, tmp_path):
        df = pd.DataFrame({"x": [1, 2, 3]})
        path = tmp_path / "gzip.parquet"
        writer = ParquetWriter()
        writer.write(df, str(path), compression="gzip")
        assert path.exists()

    def test_write_chunks_parquet(self, tmp_path):
        dest = tmp_path / "chunked_out.parquet"

        def gen():
            for i in range(3):
                yield pd.DataFrame({"val": [i * 10, i * 10 + 1]})

        writer = ParquetWriter()
        writer.write_chunks(gen(), str(dest))

        result = pd.read_parquet(dest)
        assert len(result) == 6

    def test_write_chunks_single(self, tmp_path):
        dest = tmp_path / "single_chunk.parquet"

        def gen():
            yield pd.DataFrame({"a": [1, 2]})

        writer = ParquetWriter()
        writer.write_chunks(gen(), str(dest))

        result = pd.read_parquet(dest)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# Base class default implementations
# ---------------------------------------------------------------------------


class TestBaseReaderDefaults:
    def test_read_chunks_default(self):
        """Default read_chunks reads all data as single chunk."""

        class SimpleReader(DataReader):
            def read(self, source, **kwargs):
                return pd.DataFrame({"x": [1, 2, 3]})

        reader = SimpleReader()
        chunks = list(reader.read_chunks("any"))
        assert len(chunks) == 1
        assert len(chunks[0]) == 3


class TestBaseWriterDefaults:
    def test_write_chunks_default(self):
        """Default write_chunks concatenates all chunks."""

        received = []

        class SimpleWriter(DataWriter):
            def write(self, data, destination, **kwargs):
                received.append(len(data))

        writer = SimpleWriter()

        def gen():
            yield pd.DataFrame({"a": [1, 2]})
            yield pd.DataFrame({"a": [3, 4]})

        writer.write_chunks(gen(), "dest")
        assert received[0] == 4

    def test_write_chunks_empty(self):
        """Default write_chunks with empty iterator writes empty DF."""

        received = []

        class SimpleWriter(DataWriter):
            def write(self, data, destination, **kwargs):
                received.append(data)

        writer = SimpleWriter()
        writer.write_chunks(iter([]), "dest")
        assert len(received) == 1
        assert len(received[0]) == 0


# ---------------------------------------------------------------------------
# FormatFactory edge cases
# ---------------------------------------------------------------------------


class TestFormatFactory:
    def test_get_reader(self):
        reader = FormatFactory.get_reader("csv")
        assert isinstance(reader, CSVReader)

    def test_get_writer(self):
        writer = FormatFactory.get_writer("csv")
        assert isinstance(writer, CSVWriter)

    def test_supported_formats(self):
        formats = FormatFactory.supported_formats()
        assert "csv" in formats
        assert "json" in formats
        assert "parquet" in formats

    def test_detect_format_csv(self):
        result = FormatFactory.detect_format("data/file.csv")
        assert result["format"] == "csv"

    def test_detect_format_json(self):
        result = FormatFactory.detect_format("data/file.json")
        assert result["format"] == "json"

    def test_detect_format_parquet(self):
        result = FormatFactory.detect_format("data/file.parquet")
        assert result["format"] == "parquet"

    def test_detect_format_uppercase(self):
        result = FormatFactory.detect_format("data/file.CSV")
        assert result["format"] == "csv"

    def test_detect_format_unknown(self):
        result = FormatFactory.detect_format("data/file.unknown")
        assert result["format"] == "unknown"

    def test_detect_format_returns_dict(self):
        result = FormatFactory.detect_format("f.csv")
        assert "extension" in result
        assert "mime_type" in result

    def test_get_reader_defaults_to_csv(self):
        """Local paths with unknown extension default to CSV."""
        reader = FormatFactory.get_reader("data/file.xyz")
        assert isinstance(reader, CSVReader)

    def test_get_writer_defaults_to_csv(self):
        """Local paths with unknown extension default to CSV."""
        writer = FormatFactory.get_writer("data/file.xyz")
        assert isinstance(writer, CSVWriter)

    def test_get_reader_database(self):
        reader = FormatFactory.get_reader("sqlite:///test.db")
        from simpleetl.formats.database import DatabaseReader
        assert isinstance(reader, DatabaseReader)

    def test_get_writer_database(self):
        writer = FormatFactory.get_writer("sqlite:///test.db")
        from simpleetl.formats.database import DatabaseWriter
        assert isinstance(writer, DatabaseWriter)


# ---------------------------------------------------------------------------
# JSON format tests
# ---------------------------------------------------------------------------


class TestJSONExtended:
    def test_read_json(self, tmp_path):
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        path = tmp_path / "test.json"
        df.to_json(path, orient="records")

        reader = JSONReader()
        result = reader.read(str(path), orient="records")
        assert len(result) == 2

    def test_write_json(self, tmp_path):
        df = pd.DataFrame({"x": [1, 2, 3]})
        path = tmp_path / "out.json"
        writer = JSONWriter()
        writer.write(df, str(path))
        assert path.exists()

    def test_round_trip(self, tmp_path):
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        path = tmp_path / "roundtrip.json"
        writer = JSONWriter()
        writer.write(df, str(path))
        reader = JSONReader()
        result = reader.read(str(path), lines=True)
        assert len(result) == 3
