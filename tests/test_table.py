"""Tests for the Table database abstraction class."""

import tempfile
import pandas as pd
import pytest

from simpleetl.formats.database import Table


class TestTable:
    """Test cases for Table class."""

    def test_table_init_with_connection_string(self):
        """Test Table initializes correctly with a connection string."""
        table = Table("users", connection_string="sqlite:///:memory:")
        assert table.table_name == "users"
        assert table.schema is None
        assert table.engine is not None

    def test_table_init_with_schema(self):
        """Test Table with schema name."""
        table = Table(
            "users",
            connection_string="sqlite:///:memory:",
            schema="analytics",
        )
        assert table.schema == "analytics"
        assert table.get_full_name() == "analytics.users"

    def test_table_exists_sqlite(self):
        """Test exists() returns False for non-existent table, True after creation."""
        table = Table("test_table", connection_string="sqlite:///:memory:")
        assert table.exists() is False

        df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
        table.write(df, if_exists="replace")
        assert table.exists() is True

    def test_table_read_write(self):
        """Test Table read and write operations."""
        table = Table("test_table", connection_string="sqlite:///:memory:")

        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "value": [10.5, 20.0, 30.5],
        })
        table.write(df, if_exists="replace")

        result = table.read()
        assert len(result) == 3
        assert list(result.columns) == ["id", "name", "value"]

    def test_table_read_with_where(self):
        """Test Table read with WHERE clause."""
        table = Table("test_table", connection_string="sqlite:///:memory:")

        df = pd.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Alice", "Charlie", "Bob"],
        })
        table.write(df, if_exists="replace")

        result = table.read(where="id > 2", order_by="id")
        assert len(result) == 3
        assert list(result["id"]) == [3, 4, 5]

    def test_table_read_with_columns(self):
        """Test Table read with column selection."""
        table = Table("test_table", connection_string="sqlite:///:memory:")

        df = pd.DataFrame({
            "id": [1],
            "name": ["Alice"],
            "secret": ["hidden"],
        })
        table.write(df, if_exists="replace")

        result = table.read(columns=["id", "name"])
        assert list(result.columns) == ["id", "name"]
        assert "secret" not in result.columns

    def test_table_read_chunks(self):
        """Test Table chunked reading."""
        table = Table("test_table", connection_string="sqlite:///:memory:")

        df = pd.DataFrame({
            "id": range(100),
            "value": range(100),
        })
        table.write(df, if_exists="replace")

        chunks = list(table.read_chunks(chunk_size=25))
        assert len(chunks) == 4
        total_rows = sum(len(c) for c in chunks)
        assert total_rows == 100

    def test_table_upsert_method_exists(self):
        """Test that upsert method exists and is callable."""
        table = Table("test_table", connection_string="sqlite:///:memory:")
        assert hasattr(table, "upsert")
        assert callable(table.upsert)

    def test_table_truncate(self):
        """Test Table truncate operation."""
        table = Table("test_table", connection_string="sqlite:///:memory:")

        df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        table.write(df, if_exists="replace")
        assert len(table.read()) == 3

        table.truncate()
        assert len(table.read()) == 0

    def test_table_no_connection_raises(self):
        """Test Table raises ValueError when no connection provided."""
        with pytest.raises(ValueError, match="Must provide one of"):
            Table("users")

    def test_table_read_no_engine_raises(self):
        """Test read() raises when engine is not available."""
        table = Table("users", connection_string="sqlite:///:memory:")
        table._engine = None
        with pytest.raises(ValueError, match="No database engine"):
            table.read()

    def test_table_write_no_engine_raises(self):
        """Test write() raises when engine is not available."""
        table = Table("users", connection_string="sqlite:///:memory:")
        df = pd.DataFrame({"id": [1]})
        table._engine = None
        with pytest.raises(ValueError, match="No database engine"):
            table.write(df)

    def test_table_upsert_no_engine_raises(self):
        """Test upsert() raises when engine is not available."""
        table = Table("users", connection_string="sqlite:///:memory:")
        df = pd.DataFrame({"id": [1]})
        table._engine = None
        with pytest.raises(ValueError, match="No database engine"):
            table.upsert(df, key_columns=["id"])