"""
Tests for Snowflake and BigQuery warehouse dialect support.

SQL is captured via mocked engines/connections -- no real warehouse
accounts (or driver packages) are required.
"""

import re
from unittest.mock import MagicMock, PropertyMock, patch

import pandas as pd
import pytest
import sqlalchemy

from simpleetl.core.schema import (
    ArrayType,
    ColumnDef,
    FieldDef,
    MapType,
    Schema,
    SQLDialect,
    StructType,
    generate_ddl,
)
from simpleetl.formats.database import DatabaseReader, DatabaseWriter


def _make_mock_engine(rowcount: int = 1):
    """Build a mocked engine whose connection records executed SQL."""
    engine = MagicMock()
    conn = MagicMock()
    result = MagicMock()
    result.rowcount = rowcount
    conn.execute.return_value = result
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    return engine, conn


def _executed_sql(conn) -> list:
    """Return the list of SQL strings executed on a mocked connection."""
    return [str(call.args[0]) for call in conn.execute.call_args_list]


# ---------------------------------------------------------------------------
# Snowflake MERGE
# ---------------------------------------------------------------------------


class TestMergeSnowflake:
    """Tests for DatabaseWriter._merge_snowflake SQL generation."""

    def _make_df(self):
        return pd.DataFrame(
            {
                "id": [1, 2],
                "name": ["Alice", "Bob"],
                "value": [10, 20],
            }
        )

    def test_merge_sql_shape(self):
        engine, conn = _make_mock_engine(rowcount=2)
        rows = DatabaseWriter._merge_snowflake(
            engine, self._make_df(), "users", ["id"], ["name", "value"]
        )
        assert rows == 2

        merge = [s for s in _executed_sql(conn) if "MERGE INTO" in s]
        assert len(merge) == 1
        sql = merge[0]
        assert "MERGE INTO users AS target" in sql
        assert "USING (SELECT id, name, value FROM" in sql
        assert "ON target.id = source.id" in sql
        assert (
            "WHEN MATCHED THEN UPDATE SET "
            "target.name = source.name, target.value = source.value"
        ) in sql
        assert "WHEN NOT MATCHED THEN INSERT (id, name, value)" in sql
        assert "VALUES (source.id, source.name, source.value)" in sql

    def test_staging_table_create_and_cleanup(self):
        engine, conn = _make_mock_engine()
        DatabaseWriter._merge_snowflake(
            engine, self._make_df(), "users", ["id"], ["name"]
        )

        executed = _executed_sql(conn)
        assert "CREATE TEMPORARY TABLE" in executed[0]
        assert "LIKE users" in executed[0]
        assert re.search(r"users_staging_[0-9a-f]{8}", executed[0])
        assert "DROP TABLE IF EXISTS" in executed[-1]
        assert re.search(r"users_staging_[0-9a-f]{8}", executed[-1])

    def test_staging_rows_inserted(self):
        engine, conn = _make_mock_engine()
        DatabaseWriter._merge_snowflake(
            engine, self._make_df(), "users", ["id"], ["name"]
        )
        inserts = [s for s in _executed_sql(conn) if "INSERT INTO" in s]
        # One parameterized INSERT per DataFrame row into the staging table
        assert len(inserts) == 2
        assert all("VALUES (:id, :name, :value)" in s for s in inserts)

    def test_merge_with_schema(self):
        engine, conn = _make_mock_engine()
        DatabaseWriter._merge_snowflake(
            engine,
            self._make_df(),
            "users",
            ["id"],
            ["name"],
            schema="analytics",
        )
        merge = [s for s in _executed_sql(conn) if "MERGE INTO" in s][0]
        assert "MERGE INTO analytics.users AS target" in merge
        assert "analytics.users_staging_" in merge

    def test_composite_key_conditions(self):
        engine, conn = _make_mock_engine()
        DatabaseWriter._merge_snowflake(
            engine, self._make_df(), "users", ["id", "name"], ["value"]
        )
        merge = [s for s in _executed_sql(conn) if "MERGE INTO" in s][0]
        assert ("ON target.id = source.id AND target.name = source.name") in merge

    def test_no_update_columns_omits_when_matched(self):
        engine, conn = _make_mock_engine()
        DatabaseWriter._merge_snowflake(engine, self._make_df(), "users", ["id"], [])
        merge = [s for s in _executed_sql(conn) if "MERGE INTO" in s][0]
        assert "WHEN MATCHED" not in merge
        assert "WHEN NOT MATCHED THEN INSERT" in merge

    def test_cleanup_runs_on_error(self):
        engine, conn = _make_mock_engine()
        ok_result = MagicMock()
        ok_result.rowcount = 1

        def explode_on_merge(stmt, params=None):
            if "MERGE INTO" in str(stmt):
                raise RuntimeError("merge failed")
            return ok_result

        conn.execute.side_effect = explode_on_merge
        with pytest.raises(RuntimeError, match="merge failed"):
            DatabaseWriter._merge_snowflake(
                engine, self._make_df(), "users", ["id"], ["name"]
            )
        assert any("DROP TABLE IF EXISTS" in s for s in _executed_sql(conn))


# ---------------------------------------------------------------------------
# BigQuery MERGE
# ---------------------------------------------------------------------------


class TestMergeBigQuery:
    """Tests for DatabaseWriter._merge_bigquery SQL generation."""

    def _make_df(self):
        return pd.DataFrame(
            {
                "id": [1, 2],
                "name": ["Alice", "Bob"],
            }
        )

    def test_merge_sql_shape_with_backtick_quoting(self):
        engine, conn = _make_mock_engine(rowcount=2)
        rows = DatabaseWriter._merge_bigquery(
            engine, self._make_df(), "users", ["id"], ["name"]
        )
        assert rows == 2

        merge = [s for s in _executed_sql(conn) if "MERGE INTO" in s]
        assert len(merge) == 1
        sql = merge[0]
        assert "MERGE INTO `users` AS target" in sql
        assert re.search(
            r"USING \(SELECT id, name FROM `users_staging_[0-9a-f]{8}`\)", sql
        )
        assert "ON target.id = source.id" in sql
        assert "WHEN MATCHED THEN UPDATE SET target.name = source.name" in sql
        assert "WHEN NOT MATCHED THEN INSERT (id, name)" in sql
        assert "VALUES (source.id, source.name)" in sql

    def test_staging_table_create_and_cleanup(self):
        engine, conn = _make_mock_engine()
        DatabaseWriter._merge_bigquery(
            engine, self._make_df(), "users", ["id"], ["name"]
        )
        executed = _executed_sql(conn)
        # A real staging table (not TEMP) is created with a unique suffix
        assert re.search(
            r"CREATE TABLE `users_staging_[0-9a-f]{8}` LIKE `users`",
            executed[0],
        )
        assert "CREATE TEMP" not in executed[0]
        assert re.search(
            r"DROP TABLE IF EXISTS `users_staging_[0-9a-f]{8}`",
            executed[-1],
        )

    def test_merge_with_schema_dataset(self):
        engine, conn = _make_mock_engine()
        DatabaseWriter._merge_bigquery(
            engine,
            self._make_df(),
            "users",
            ["id"],
            ["name"],
            schema="my_dataset",
        )
        merge = [s for s in _executed_sql(conn) if "MERGE INTO" in s][0]
        assert "MERGE INTO `my_dataset.users` AS target" in merge
        assert "`my_dataset.users_staging_" in merge

    def test_composite_key_conditions(self):
        engine, conn = _make_mock_engine()
        df = pd.DataFrame({"id": [1], "name": ["A"], "value": [9]})
        DatabaseWriter._merge_bigquery(engine, df, "users", ["id", "name"], ["value"])
        merge = [s for s in _executed_sql(conn) if "MERGE INTO" in s][0]
        assert ("ON target.id = source.id AND target.name = source.name") in merge

    def test_no_update_columns_omits_when_matched(self):
        engine, conn = _make_mock_engine()
        DatabaseWriter._merge_bigquery(engine, self._make_df(), "users", ["id"], [])
        merge = [s for s in _executed_sql(conn) if "MERGE INTO" in s][0]
        assert "WHEN MATCHED" not in merge
        assert "WHEN NOT MATCHED THEN INSERT" in merge

    def test_cleanup_runs_on_error(self):
        engine, conn = _make_mock_engine()
        ok_result = MagicMock()
        ok_result.rowcount = 1

        def explode_on_merge(stmt, params=None):
            if "MERGE INTO" in str(stmt):
                raise RuntimeError("merge failed")
            return ok_result

        conn.execute.side_effect = explode_on_merge
        with pytest.raises(RuntimeError, match="merge failed"):
            DatabaseWriter._merge_bigquery(
                engine, self._make_df(), "users", ["id"], ["name"]
            )
        assert any("DROP TABLE IF EXISTS" in s for s in _executed_sql(conn))


# ---------------------------------------------------------------------------
# Merge dispatch by engine dialect name
# ---------------------------------------------------------------------------


class TestMergeDispatch:
    """Tests that merge() routes to the right per-dialect method."""

    def _make_df(self):
        return pd.DataFrame({"id": [1], "name": ["Alice"]})

    def _dispatch_with_dialect(self, dialect_name, method_name):
        writer = DatabaseWriter()
        engine = sqlalchemy.create_engine("sqlite:///:memory:")
        with patch.object(
            type(engine.dialect), "name", new_callable=PropertyMock
        ) as mock_dialect:
            mock_dialect.return_value = dialect_name
            with patch.object(
                DatabaseWriter, method_name, return_value=7
            ) as mock_merge:
                rows = writer.merge(
                    data=self._make_df(),
                    destination=engine,
                    table_name="users",
                    key_columns=["id"],
                )
        assert rows == 7
        mock_merge.assert_called_once()
        return mock_merge

    def test_dispatch_snowflake(self):
        mock_merge = self._dispatch_with_dialect("snowflake", "_merge_snowflake")
        args = mock_merge.call_args.args
        assert args[2] == "users"
        assert args[3] == ["id"]
        assert args[4] == ["name"]

    def test_dispatch_bigquery(self):
        mock_merge = self._dispatch_with_dialect("bigquery", "_merge_bigquery")
        args = mock_merge.call_args.args
        assert args[2] == "users"
        assert args[3] == ["id"]
        assert args[4] == ["name"]

    def test_unknown_dialect_falls_back_to_generic(self):
        self._dispatch_with_dialect("oracle", "_merge_generic")


# ---------------------------------------------------------------------------
# URL detection
# ---------------------------------------------------------------------------


class TestUrlDetection:
    """Tests for snowflake:// and bigquery:// connection URL handling."""

    SNOWFLAKE_URL = "snowflake://user:pass@account/db/schema"
    BIGQUERY_URL = "bigquery://my-project/my_dataset"

    @pytest.mark.parametrize("url", [SNOWFLAKE_URL, BIGQUERY_URL])
    def test_writer_resolves_warehouse_urls(self, url):
        fake_engine = MagicMock()
        with patch(
            "simpleetl.formats.database.get_engine",
            return_value=fake_engine,
        ) as mock_get_engine:
            engine = DatabaseWriter._resolve_engine(url)
        assert engine is fake_engine
        config = mock_get_engine.call_args.args[0]
        assert config.url == url

    @pytest.mark.parametrize("url", [SNOWFLAKE_URL, BIGQUERY_URL])
    def test_reader_resolves_warehouse_urls(self, url):
        fake_engine = MagicMock()
        with patch(
            "simpleetl.formats.database.get_engine",
            return_value=fake_engine,
        ) as mock_get_engine:
            engine = DatabaseReader._resolve_engine(url)
        assert engine is fake_engine
        config = mock_get_engine.call_args.args[0]
        assert config.url == url

    def test_non_url_string_still_raises(self):
        with pytest.raises(ValueError, match="must be a connection URL"):
            DatabaseWriter._resolve_engine("just_a_table_name")

    def test_error_message_lists_warehouse_prefixes(self):
        with pytest.raises(ValueError, match=r"snowflake://.*bigquery://"):
            DatabaseReader._resolve_engine("not-a-url")


# ---------------------------------------------------------------------------
# SQLDialect enum
# ---------------------------------------------------------------------------


class TestSQLDialectEnum:
    """Tests for the new SQLDialect members."""

    def test_snowflake_member(self):
        assert SQLDialect.SNOWFLAKE.value == "snowflake"

    def test_bigquery_member(self):
        assert SQLDialect.BIGQUERY.value == "bigquery"


# ---------------------------------------------------------------------------
# Snowflake DDL generation
# ---------------------------------------------------------------------------


class TestSnowflakeDdl:
    """Tests for generate_ddl with the snowflake dialect."""

    def test_scalar_types(self):
        schema = Schema(
            columns=[
                ColumnDef("id", "int64", nullable=False),
                ColumnDef("score", "float64"),
                ColumnDef("name", "string"),
                ColumnDef("active", "bool"),
                ColumnDef("created", "datetime64[ns]"),
                ColumnDef("born", "date"),
            ]
        )
        ddl = generate_ddl(schema, "users", dialect="snowflake")
        assert "id NUMBER NOT NULL" in ddl
        assert "score FLOAT" in ddl
        assert "name VARCHAR" in ddl
        assert "active BOOLEAN" in ddl
        assert "created TIMESTAMP_NTZ" in ddl
        assert "born DATE" in ddl

    def test_table_name_and_if_not_exists(self):
        schema = Schema(columns=[ColumnDef("id", "int64")])
        ddl = generate_ddl(schema, "analytics.users", dialect="snowflake")
        assert "CREATE TABLE IF NOT EXISTS analytics.users" in ddl

    def test_no_if_not_exists(self):
        schema = Schema(columns=[ColumnDef("id", "int64")])
        ddl = generate_ddl(schema, "users", dialect="snowflake", if_not_exists=False)
        assert "IF NOT EXISTS" not in ddl
        assert "CREATE TABLE users" in ddl

    def test_nullability(self):
        schema = Schema(
            columns=[
                ColumnDef("id", "int64", nullable=False),
                ColumnDef("name", "string", nullable=True),
            ]
        )
        ddl = generate_ddl(schema, "t", dialect="snowflake")
        assert "id NUMBER NOT NULL" in ddl
        assert "name VARCHAR NOT NULL" not in ddl

    def test_struct_maps_to_variant(self):
        col = ColumnDef(
            name="data",
            dtype="struct<a:int64>",
            struct_type=StructType(fields=[FieldDef("a", "int64")]),
        )
        schema = Schema(columns=[col])
        ddl = generate_ddl(schema, "t", dialect="snowflake")
        assert "data VARIANT" in ddl

    def test_array_maps_to_variant(self):
        col = ColumnDef(
            name="tags",
            dtype="array<string>",
            array_type=ArrayType(element_type="string"),
        )
        schema = Schema(columns=[col])
        ddl = generate_ddl(schema, "t", dialect="snowflake")
        assert "tags VARIANT" in ddl

    def test_map_maps_to_variant(self):
        col = ColumnDef(
            name="meta",
            dtype="map<string,int64>",
            map_type=MapType(key_type="string", value_type="int64"),
        )
        schema = Schema(columns=[col])
        ddl = generate_ddl(schema, "t", dialect="snowflake")
        assert "meta VARIANT" in ddl

    def test_unknown_type_falls_back_to_varchar(self):
        schema = Schema(columns=[ColumnDef("x", "custom_type")])
        ddl = generate_ddl(schema, "t", dialect="snowflake")
        assert "x VARCHAR" in ddl
        assert "TEXT" not in ddl


# ---------------------------------------------------------------------------
# BigQuery DDL generation
# ---------------------------------------------------------------------------


class TestBigQueryDdl:
    """Tests for generate_ddl with the bigquery dialect."""

    def test_scalar_types(self):
        schema = Schema(
            columns=[
                ColumnDef("id", "int64", nullable=False),
                ColumnDef("score", "float64"),
                ColumnDef("name", "string"),
                ColumnDef("active", "bool"),
                ColumnDef("created", "datetime64[ns]"),
                ColumnDef("born", "date"),
            ]
        )
        ddl = generate_ddl(schema, "users", dialect="bigquery")
        assert "id INT64 NOT NULL" in ddl
        assert "score FLOAT64" in ddl
        assert "name STRING" in ddl
        assert "active BOOL" in ddl
        assert "created TIMESTAMP" in ddl
        assert "born DATE" in ddl

    def test_no_varchar_in_bigquery_ddl(self):
        schema = Schema(
            columns=[
                ColumnDef("a", "object"),
                ColumnDef("b", "string"),
                ColumnDef("c", "category"),
            ]
        )
        ddl = generate_ddl(schema, "t", dialect="bigquery")
        assert "VARCHAR" not in ddl
        assert "TEXT" not in ddl
        assert ddl.count("STRING") == 3

    def test_table_name_and_if_not_exists(self):
        schema = Schema(columns=[ColumnDef("id", "int64")])
        ddl = generate_ddl(schema, "my_dataset.users", dialect="bigquery")
        assert "CREATE TABLE IF NOT EXISTS my_dataset.users" in ddl

    def test_nullability(self):
        schema = Schema(
            columns=[
                ColumnDef("id", "int64", nullable=False),
                ColumnDef("name", "string", nullable=True),
            ]
        )
        ddl = generate_ddl(schema, "t", dialect="bigquery")
        assert "id INT64 NOT NULL" in ddl
        assert "name STRING NOT NULL" not in ddl

    def test_struct_maps_to_json(self):
        col = ColumnDef(
            name="data",
            dtype="struct<a:int64>",
            struct_type=StructType(fields=[FieldDef("a", "int64")]),
        )
        schema = Schema(columns=[col])
        ddl = generate_ddl(schema, "t", dialect="bigquery")
        assert "data JSON" in ddl
        assert "JSONB" not in ddl

    def test_array_maps_to_json(self):
        col = ColumnDef(
            name="tags",
            dtype="array<string>",
            array_type=ArrayType(element_type="string"),
        )
        schema = Schema(columns=[col])
        ddl = generate_ddl(schema, "t", dialect="bigquery")
        assert "tags JSON" in ddl

    def test_map_maps_to_json(self):
        col = ColumnDef(
            name="meta",
            dtype="map<string,int64>",
            map_type=MapType(key_type="string", value_type="int64"),
        )
        schema = Schema(columns=[col])
        ddl = generate_ddl(schema, "t", dialect="bigquery")
        assert "meta JSON" in ddl

    def test_unknown_type_falls_back_to_string(self):
        schema = Schema(columns=[ColumnDef("x", "custom_type")])
        ddl = generate_ddl(schema, "t", dialect="bigquery")
        assert "x STRING" in ddl
