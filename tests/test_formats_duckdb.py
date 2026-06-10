"""Tests for DuckDB format reader/writer and sql_transform (Phase 8.2)."""

import sys

import pandas as pd
import pytest

duckdb = pytest.importorskip("duckdb", reason="duckdb not installed")

from simpleetl.formats.duckdb import DuckDBReader, DuckDBWriter  # noqa: E402
from simpleetl.transformations import sql_transform  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_db(tmp_path, table_name="sales"):
    """Create a DuckDB file with a simple test table."""
    path = str(tmp_path / "test.duckdb")
    conn = duckdb.connect(path)
    conn.execute(
        f"CREATE TABLE {table_name} AS "
        "SELECT 1 AS id, 'Alice' AS name, 100.0 AS revenue "
        "UNION ALL "
        "SELECT 2, 'Bob', 200.0 "
        "UNION ALL "
        "SELECT 3, 'Charlie', 150.0"
    )
    conn.close()
    return path


# ---------------------------------------------------------------------------
# DuckDBReader
# ---------------------------------------------------------------------------


class TestDuckDBReader:
    def test_read_with_table(self, tmp_path):
        path = _make_db(tmp_path)
        reader = DuckDBReader(read_only=False)
        df = reader.read(path, table="sales")
        assert len(df) == 3
        assert list(df.columns) == ["id", "name", "revenue"]

    def test_read_with_query(self, tmp_path):
        path = _make_db(tmp_path)
        reader = DuckDBReader(read_only=False)
        df = reader.read(path, query="SELECT name FROM sales WHERE revenue > 120")
        assert len(df) == 2
        assert set(df["name"]) == {"Bob", "Charlie"}

    def test_read_requires_query_or_table(self, tmp_path):
        path = _make_db(tmp_path)
        reader = DuckDBReader(read_only=False)
        with pytest.raises(ValueError, match="'query' or 'table'"):
            reader.read(path)

    def test_read_returns_dataframe(self, tmp_path):
        path = _make_db(tmp_path)
        reader = DuckDBReader(read_only=False)
        df = reader.read(path, table="sales")
        assert isinstance(df, pd.DataFrame)

    def test_read_chunks_yields_dataframes(self, tmp_path):
        path = _make_db(tmp_path)
        reader = DuckDBReader(read_only=False)
        chunks = list(reader.read_chunks(path, chunk_size=2, table="sales"))
        assert len(chunks) >= 1
        total_rows = sum(len(c) for c in chunks)
        assert total_rows == 3

    def test_read_chunks_requires_query_or_table(self, tmp_path):
        path = _make_db(tmp_path)
        reader = DuckDBReader(read_only=False)
        with pytest.raises(ValueError, match="'query' or 'table'"):
            list(reader.read_chunks(path))

    def test_read_chunks_with_query(self, tmp_path):
        path = _make_db(tmp_path)
        reader = DuckDBReader(read_only=False)
        chunks = list(
            reader.read_chunks(
                path, chunk_size=2, query="SELECT * FROM sales ORDER BY id"
            )
        )
        rows = pd.concat(chunks, ignore_index=True)
        assert list(rows["id"]) == [1, 2, 3]

    def test_import_error_when_duckdb_missing(self, tmp_path):
        original = sys.modules.get("duckdb")
        sys.modules["duckdb"] = None  # type: ignore[assignment]
        try:
            from importlib import reload
            import simpleetl.formats.duckdb as mod
            reload(mod)
            with pytest.raises(ImportError, match="duckdb"):
                mod.DuckDBReader().read(str(tmp_path / "x.duckdb"), table="t")
        finally:
            if original is not None:
                sys.modules["duckdb"] = original
            else:
                del sys.modules["duckdb"]


# ---------------------------------------------------------------------------
# DuckDBWriter
# ---------------------------------------------------------------------------


class TestDuckDBWriter:
    def test_write_creates_table(self, tmp_path):
        path = str(tmp_path / "out.duckdb")
        df = pd.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "c"]})
        writer = DuckDBWriter()
        writer.write(df, path, table_name="results")

        conn = duckdb.connect(path, read_only=True)
        result = conn.execute("SELECT * FROM results ORDER BY x").df()
        conn.close()
        assert list(result["x"]) == [1, 2, 3]

    def test_write_append_mode(self, tmp_path):
        path = str(tmp_path / "out.duckdb")
        df1 = pd.DataFrame({"val": [1, 2]})
        df2 = pd.DataFrame({"val": [3, 4]})
        writer = DuckDBWriter()
        writer.write(df1, path, table_name="t")
        writer.write(df2, path, table_name="t", mode="append")

        conn = duckdb.connect(path, read_only=True)
        result = conn.execute("SELECT val FROM t ORDER BY val").df()
        conn.close()
        assert list(result["val"]) == [1, 2, 3, 4]

    def test_write_replace_mode(self, tmp_path):
        path = str(tmp_path / "out.duckdb")
        df1 = pd.DataFrame({"val": [1, 2, 3]})
        df2 = pd.DataFrame({"val": [99]})
        writer = DuckDBWriter()
        writer.write(df1, path, table_name="t")
        writer.write(df2, path, table_name="t", mode="replace")

        conn = duckdb.connect(path, read_only=True)
        result = conn.execute("SELECT val FROM t").df()
        conn.close()
        assert list(result["val"]) == [99]

    def test_write_error_mode_fails_if_exists(self, tmp_path):
        path = str(tmp_path / "out.duckdb")
        df = pd.DataFrame({"val": [1]})
        writer = DuckDBWriter()
        writer.write(df, path, table_name="t")
        with pytest.raises(ValueError, match="already exists"):
            writer.write(df, path, table_name="t", mode="error")

    def test_write_invalid_mode(self, tmp_path):
        path = str(tmp_path / "out.duckdb")
        df = pd.DataFrame({"val": [1]})
        writer = DuckDBWriter()
        with pytest.raises(ValueError, match="Invalid mode"):
            writer.write(df, path, table_name="t", mode="invalid")

    def test_write_default_table_name(self, tmp_path):
        path = str(tmp_path / "out.duckdb")
        df = pd.DataFrame({"col": [42]})
        DuckDBWriter().write(df, path)

        conn = duckdb.connect(path, read_only=True)
        result = conn.execute("SELECT col FROM data").df()
        conn.close()
        assert list(result["col"]) == [42]


# ---------------------------------------------------------------------------
# sql_transform
# ---------------------------------------------------------------------------


class TestSqlTransform:
    def test_basic_select(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [10, 20, 30]})
        result = sql_transform(df, "SELECT a, b FROM data WHERE a > 1")
        assert len(result) == 2
        assert list(result["a"]) == [2, 3]

    def test_aggregation(self):
        df = pd.DataFrame(
            {"category": ["A", "A", "B", "B"], "value": [10, 20, 5, 15]}
        )
        result = sql_transform(
            df, "SELECT category, SUM(value) AS total FROM data GROUP BY category ORDER BY category"
        )
        assert list(result["category"]) == ["A", "B"]
        assert list(result["total"]) == [30, 20]

    def test_custom_table_name(self):
        df = pd.DataFrame({"n": [1, 2, 3]})
        result = sql_transform(df, "SELECT n * 2 AS doubled FROM my_table", table_name="my_table")
        assert list(result["doubled"]) == [2, 4, 6]

    def test_returns_dataframe(self):
        df = pd.DataFrame({"x": [1]})
        result = sql_transform(df, "SELECT x FROM data")
        assert isinstance(result, pd.DataFrame)

    def test_column_renaming(self):
        df = pd.DataFrame({"old_name": [1, 2]})
        result = sql_transform(df, "SELECT old_name AS new_name FROM data")
        assert "new_name" in result.columns

    def test_window_function(self):
        df = pd.DataFrame({"v": [3, 1, 2]})
        result = sql_transform(
            df, "SELECT v, RANK() OVER (ORDER BY v) AS rnk FROM data"
        )
        assert list(sorted(result["rnk"])) == [1, 2, 3]

    def test_join_two_registrations(self):
        """DuckDB in-memory can be used for more complex transformations."""
        import duckdb as _duckdb
        conn = _duckdb.connect(":memory:")
        df_a = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
        df_b = pd.DataFrame({"id": [1, 2], "score": [90, 80]})
        conn.register("a", df_a)
        conn.register("b", df_b)
        result = conn.execute("SELECT a.name, b.score FROM a JOIN b ON a.id = b.id").df()
        assert list(result["name"]) == ["Alice", "Bob"]

    def test_import_error_when_duckdb_missing(self):
        original = sys.modules.get("duckdb")
        sys.modules["duckdb"] = None  # type: ignore[assignment]
        try:
            with pytest.raises(ImportError, match="duckdb"):
                sql_transform(pd.DataFrame({"x": [1]}), "SELECT * FROM data")
        finally:
            if original is not None:
                sys.modules["duckdb"] = original
            else:
                del sys.modules["duckdb"]
