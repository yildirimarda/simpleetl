"""Tests for transactional sink exactly-once guarantees."""

import os
import tempfile
import pandas as pd

from simpleetl.formats.csv import CSVWriter
from simpleetl.formats.json import JSONWriter
from simpleetl.formats.parquet import ParquetWriter
from simpleetl.formats.xml import XMLWriter
from simpleetl.formats.excel import ExcelWriter
from simpleetl.formats.database import DatabaseWriter


class TestTransactionalFileSinks:
    def test_csv_atomic_rename(self, tmp_path):
        df = pd.DataFrame({"a": [1, 2]})
        dest = tmp_path / "out.csv"
        writer = CSVWriter()
        writer.write(df, str(dest))
        assert dest.exists()
        assert not any(f.startswith(".tmp_") for f in os.listdir(tmp_path))
        result = pd.read_csv(str(dest))
        assert len(result) == 2

    def test_json_atomic_rename(self, tmp_path):
        df = pd.DataFrame({"a": [1]})
        dest = tmp_path / "out.json"
        writer = JSONWriter()
        writer.write(df, str(dest))
        assert dest.exists()
        result = pd.read_json(str(dest), lines=True)
        assert len(result) == 1

    def test_parquet_atomic_rename(self, tmp_path):
        df = pd.DataFrame({"a": [1, 2]})
        dest = tmp_path / "out.parquet"
        writer = ParquetWriter()
        writer.write(df, str(dest))
        assert dest.exists()
        result = pd.read_parquet(str(dest))
        assert len(result) == 2

    def test_xml_atomic_rename(self, tmp_path):
        df = pd.DataFrame({"a": ["x"]})
        dest = tmp_path / "out.xml"
        writer = XMLWriter()
        writer.write(df, str(dest))
        assert dest.exists()

    def test_excel_atomic_rename(self, tmp_path):
        df = pd.DataFrame({"a": [1]})
        dest = tmp_path / "out.xlsx"
        writer = ExcelWriter()
        writer.write(df, str(dest))
        assert dest.exists()


class TestTransactionalDatabaseSink:
    def test_database_staging_swap(self):
        df = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            conn_str = f"sqlite:///{db_path}"
            writer = DatabaseWriter()
            writer.write(df, conn_str, table_name="test_table")
            result = pd.read_sql("SELECT * FROM test_table", conn_str)
            assert len(result) == 2
            assert list(result.columns) == ["id", "value"]
        finally:
            os.unlink(db_path)
