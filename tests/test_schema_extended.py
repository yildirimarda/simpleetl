"""
Extended tests for schema management (schema.py).

Covers: Schema.diff(), Schema.evolve(), Schema.merge(), Schema.validate(),
generate_ddl(), ColumnDef, SchemaDiff, SQLDialect, SchemaValidationError,
and edge cases.
"""

import pytest
import pandas as pd
import json

from simpleetl.core.schema import (
    Schema,
    ColumnDef,
    SchemaDiff,
    SchemaValidationError,
    SQLDialect,
    generate_ddl,
)


# ---------------------------------------------------------------------------
# ColumnDef tests
# ---------------------------------------------------------------------------


class TestColumnDef:
    def test_basic_creation(self):
        col = ColumnDef(name="id", dtype="int64")
        assert col.name == "id"
        assert col.dtype == "int64"
        assert col.nullable is True
        assert col.default is None
        assert col.description == ""

    def test_full_creation(self):
        col = ColumnDef(
            name="email",
            dtype="object",
            nullable=False,
            default="unknown",
            description="User email address",
        )
        assert col.nullable is False
        assert col.default == "unknown"
        assert col.description == "User email address"

    def test_to_dict(self):
        col = ColumnDef(name="age", dtype="int64", nullable=True)
        d = col.to_dict()
        assert d == {
            "name": "age",
            "dtype": "int64",
            "nullable": True,
            "default": None,
            "description": "",
        }

    def test_from_dict(self):
        data = {
            "name": "score",
            "dtype": "float64",
            "nullable": False,
            "default": 0.0,
            "description": "Test score",
        }
        col = ColumnDef.from_dict(data)
        assert col.name == "score"
        assert col.dtype == "float64"
        assert col.nullable is False
        assert col.default == 0.0
        assert col.description == "Test score"

    def test_from_dict_defaults(self):
        data = {"name": "x", "dtype": "int64"}
        col = ColumnDef.from_dict(data)
        assert col.nullable is True
        assert col.default is None
        assert col.description == ""


# ---------------------------------------------------------------------------
# SchemaDiff tests
# ---------------------------------------------------------------------------


class TestSchemaDiff:
    def test_no_changes(self):
        diff = SchemaDiff()
        assert diff.has_changes is False

    def test_added_columns(self):
        diff = SchemaDiff(added_columns=["new_col"])
        assert diff.has_changes is True

    def test_removed_columns(self):
        diff = SchemaDiff(removed_columns=["old_col"])
        assert diff.has_changes is True

    def test_type_changes(self):
        diff = SchemaDiff(type_changes={"col": {"old": "int32", "new": "int64"}})
        assert diff.has_changes is True

    def test_nullability_changes(self):
        diff = SchemaDiff(
            nullability_changes={"col": {"old": True, "new": False}}
        )
        assert diff.has_changes is True

    def test_to_dict(self):
        diff = SchemaDiff(
            added_columns=["a"],
            removed_columns=["b"],
            type_changes={"c": {"old": "int32", "new": "int64"}},
            nullability_changes={"d": {"old": True, "new": False}},
        )
        d = diff.to_dict()
        assert d["added_columns"] == ["a"]
        assert d["removed_columns"] == ["b"]
        assert d["type_changes"] == {"c": {"old": "int32", "new": "int64"}}
        assert d["nullability_changes"] == {"d": {"old": True, "new": False}}


# ---------------------------------------------------------------------------
# Schema.from_dataframe
# ---------------------------------------------------------------------------


class TestSchemaFromDataFrame:
    def test_basic_inference(self):
        df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        schema = Schema.from_dataframe(df)
        assert len(schema) == 2
        assert schema.column_names == ["id", "name"]

    def test_nullability_inference(self):
        df = pd.DataFrame({"a": [1, None, 3], "b": [4, 5, 6]})
        schema = Schema.from_dataframe(df)
        col_a = schema.get_column("a")
        col_b = schema.get_column("b")
        assert col_a.nullable is True
        assert col_b.nullable is False

    def test_nullable_override(self):
        df = pd.DataFrame({"a": [1, 2, 3]})
        schema = Schema.from_dataframe(df, nullable={"a": True})
        assert schema.get_column("a").nullable is True

    def test_descriptions(self):
        df = pd.DataFrame({"x": [1]})
        schema = Schema.from_dataframe(
            df, descriptions={"x": "The X column"}
        )
        assert schema.get_column("x").description == "The X column"

    def test_metadata(self):
        df = pd.DataFrame({"x": [1]})
        schema = Schema.from_dataframe(df, metadata={"source": "test"})
        assert schema.metadata["source"] == "test"

    def test_empty_dataframe(self):
        df = pd.DataFrame()
        schema = Schema.from_dataframe(df)
        assert len(schema) == 0


# ---------------------------------------------------------------------------
# Schema.from_dict
# ---------------------------------------------------------------------------


class TestSchemaFromDict:
    def test_basic(self):
        data = {
            "columns": [
                {"name": "id", "dtype": "int64", "nullable": False},
                {"name": "name", "dtype": "object"},
            ]
        }
        schema = Schema.from_dict(data)
        assert len(schema) == 2
        assert schema.column_names == ["id", "name"]

    def test_empty_columns(self):
        schema = Schema.from_dict({"columns": []})
        assert len(schema) == 0

    def test_with_metadata(self):
        data = {
            "columns": [{"name": "x", "dtype": "int64"}],
            "metadata": {"version": "1.0"},
        }
        schema = Schema.from_dict(data)
        assert schema.metadata["version"] == "1.0"


# ---------------------------------------------------------------------------
# Schema serialization
# ---------------------------------------------------------------------------


class TestSchemaSerialization:
    def test_to_dict(self):
        schema = Schema(columns=[ColumnDef("id", "int64")])
        d = schema.to_dict()
        assert "columns" in d
        assert d["columns"][0]["name"] == "id"

    def test_to_json(self):
        schema = Schema(
            columns=[ColumnDef("id", "int64")], metadata={"k": "v"}
        )
        j = schema.to_json()
        parsed = json.loads(j)
        assert parsed["columns"][0]["name"] == "id"
        assert parsed["metadata"]["k"] == "v"

    def test_round_trip(self):
        original = Schema(
            columns=[
                ColumnDef("a", "int64", nullable=False),
                ColumnDef("b", "object", description="test"),
            ],
            metadata={"source": "unit_test"},
        )
        restored = Schema.from_dict(original.to_dict())
        assert restored == original


# ---------------------------------------------------------------------------
# Schema.validate
# ---------------------------------------------------------------------------


class TestSchemaValidate:
    def test_valid(self):
        schema = Schema(
            columns=[ColumnDef("id", "int64"), ColumnDef("name", "object")]
        )
        df = pd.DataFrame({"id": [1], "name": ["a"]})
        schema.validate(df)  # Should not raise

    def test_missing_columns(self):
        schema = Schema(
            columns=[ColumnDef("id", "int64"), ColumnDef("missing", "object")]
        )
        df = pd.DataFrame({"id": [1]})
        with pytest.raises(SchemaValidationError):
            schema.validate(df)

    def test_extra_columns(self):
        schema = Schema(columns=[ColumnDef("id", "int64")])
        df = pd.DataFrame({"id": [1], "extra": [2]})
        with pytest.raises(SchemaValidationError):
            schema.validate(df)

    def test_strict_nullability(self):
        schema = Schema(
            columns=[ColumnDef("id", "int64", nullable=False)]
        )
        df = pd.DataFrame({"id": [1, None]})
        with pytest.raises(SchemaValidationError):
            schema.validate(df, strict_nullability=True)

    def test_strict_types(self):
        schema = Schema(columns=[ColumnDef("id", "float64")])
        df = pd.DataFrame({"id": [1]})
        with pytest.raises(SchemaValidationError):
            schema.validate(df, strict_types=True)

    def test_strict_types_match(self):
        schema = Schema(columns=[ColumnDef("id", "int64")])
        df = pd.DataFrame({"id": [1, 2, 3]})
        schema.validate(df, strict_types=True)  # Should not raise

    def test_validation_errors_list(self):
        schema = Schema(
            columns=[ColumnDef("x", "int64", nullable=False)]
        )
        df = pd.DataFrame({"x": [1, None]})
        with pytest.raises(SchemaValidationError) as exc_info:
            schema.validate(df, strict_nullability=True)
        assert len(exc_info.value.errors) > 0


# ---------------------------------------------------------------------------
# Schema.diff
# ---------------------------------------------------------------------------

    def test_schema_diff_added_columns(self):
        old = Schema(columns=[ColumnDef("a", "int64")])
        new = Schema(
            columns=[ColumnDef("a", "int64"), ColumnDef("b", "object")]
        )
        diff = old.diff(new)
        assert diff.added_columns == ["b"]
        assert diff.removed_columns == []

    def test_schema_diff_removed_columns(self):
        old = Schema(
            columns=[ColumnDef("a", "int64"), ColumnDef("b", "object")]
        )
        new = Schema(columns=[ColumnDef("a", "int64")])
        diff = old.diff(new)
        assert diff.removed_columns == ["b"]
        assert diff.added_columns == []

    def test_schema_diff_type_change(self):
        old = Schema(columns=[ColumnDef("x", "int32")])
        new = Schema(columns=[ColumnDef("x", "int64")])
        diff = old.diff(new)
        assert "x" in diff.type_changes
        assert diff.type_changes["x"]["old"] == "int32"
        assert diff.type_changes["x"]["new"] == "int64"

    def test_schema_diff_nullability_change(self):
        old = Schema(columns=[ColumnDef("x", "int64", nullable=True)])
        new = Schema(columns=[ColumnDef("x", "int64", nullable=False)])
        diff = old.diff(new)
        assert "x" in diff.nullability_changes

    def test_identical_schemas(self):
        s = Schema(columns=[ColumnDef("a", "int64"), ColumnDef("b", "object")])
        diff = s.diff(s)
        assert diff.has_changes is False

    def test_multiple_changes(self):
        old = Schema(
            columns=[
                ColumnDef("a", "int64"),
                ColumnDef("b", "int32"),
                ColumnDef("c", "object"),
            ]
        )
        new = Schema(
            columns=[
                ColumnDef("a", "int64"),
                ColumnDef("b", "float64"),
                ColumnDef("d", "object"),
            ]
        )
        diff = old.diff(new)
        assert "d" in diff.added_columns
        assert "c" in diff.removed_columns
        assert "b" in diff.type_changes


# ---------------------------------------------------------------------------
# Schema.evolve
# ---------------------------------------------------------------------------


class TestSchemaEvolve:
    def test_add_columns(self):
        old = Schema(columns=[ColumnDef("a", "int64")])
        new = Schema(
            columns=[ColumnDef("a", "int64"), ColumnDef("b", "object")]
        )
        evolved = old.evolve(new)
        assert len(evolved) == 2
        assert "b" in evolved.column_names

    def test_remove_columns(self):
        old = Schema(
            columns=[ColumnDef("a", "int64"), ColumnDef("b", "object")]
        )
        new = Schema(columns=[ColumnDef("a", "int64")])
        evolved = old.evolve(new)
        assert len(evolved) == 1

    def test_type_changes_allowed(self):
        old = Schema(columns=[ColumnDef("x", "int32")])
        new = Schema(columns=[ColumnDef("x", "int64")])
        evolved = old.evolve(new, allow_type_changes=True)
        assert evolved.get_column("x").dtype == "int64"

    def test_type_changes_disallowed(self):
        old = Schema(columns=[ColumnDef("x", "int32")])
        new = Schema(columns=[ColumnDef("x", "int64")])
        evolved = old.evolve(new, allow_type_changes=False)
        assert evolved.get_column("x").dtype == "int32"

    def test_nullability_changes(self):
        old = Schema(columns=[ColumnDef("x", "int64", nullable=True)])
        new = Schema(columns=[ColumnDef("x", "int64", nullable=False)])
        evolved = old.evolve(new, allow_nullability_changes=True)
        assert evolved.get_column("x").nullable is False

    def test_evolve_preserves_order(self):
        old = Schema(
            columns=[
                ColumnDef("a", "int64"),
                ColumnDef("b", "object"),
            ]
        )
        new = Schema(
            columns=[
                ColumnDef("a", "int64"),
                ColumnDef("b", "object"),
                ColumnDef("c", "float64"),
            ]
        )
        evolved = old.evolve(new)
        assert evolved.column_names == ["a", "b", "c"]


# ---------------------------------------------------------------------------
# Schema.merge
# ---------------------------------------------------------------------------


class TestSchemaMerge:
    def test_merge_disjoint(self):
        s1 = Schema(columns=[ColumnDef("a", "int64")])
        s2 = Schema(columns=[ColumnDef("b", "object")])
        merged = s1.merge(s2)
        assert len(merged) == 2
        assert merged.column_names == ["a", "b"]

    def test_merge_overlapping_self_wins(self):
        s1 = Schema(columns=[ColumnDef("x", "int64")])
        s2 = Schema(columns=[ColumnDef("x", "float64")])
        merged = s1.merge(s2)
        assert merged.get_column("x").dtype == "int64"

    def test_merge_preserves_order(self):
        s1 = Schema(columns=[ColumnDef("a", "int64"), ColumnDef("b", "object")])
        s2 = Schema(columns=[ColumnDef("c", "float64")])
        merged = s1.merge(s2)
        assert merged.column_names == ["a", "b", "c"]

    def test_merge_metadata(self):
        s1 = Schema(columns=[ColumnDef("a", "int64")], metadata={"k1": "v1"})
        s2 = Schema(columns=[ColumnDef("b", "object")], metadata={"k2": "v2"})
        merged = s1.merge(s2)
        assert merged.metadata["k1"] == "v1"
        assert merged.metadata["k2"] == "v2"


# ---------------------------------------------------------------------------
# Schema dunder methods
# ---------------------------------------------------------------------------


class TestSchemaDunder:
    def test_repr(self):
        schema = Schema(columns=[ColumnDef("a", "int64"), ColumnDef("b", "object")])
        r = repr(schema)
        assert "Schema" in r
        assert "a" in r

    def test_equality(self):
        s1 = Schema(columns=[ColumnDef("a", "int64")])
        s2 = Schema(columns=[ColumnDef("a", "int64")])
        assert s1 == s2

    def test_inequality(self):
        s1 = Schema(columns=[ColumnDef("a", "int64")])
        s2 = Schema(columns=[ColumnDef("b", "int64")])
        assert s1 != s2

    def test_len(self):
        schema = Schema(
            columns=[ColumnDef("a", "int64"), ColumnDef("b", "object")]
        )
        assert len(schema) == 2

    def test_get_column_none(self):
        schema = Schema(columns=[ColumnDef("a", "int64")])
        assert schema.get_column("nonexistent") is None


# ---------------------------------------------------------------------------
# SQLDialect tests
# ---------------------------------------------------------------------------


class TestSQLDialect:
    def test_values(self):
        assert SQLDialect.POSTGRESQL == "postgresql"
        assert SQLDialect.MYSQL == "mysql"
        assert SQLDialect.SQLITE == "sqlite"

    def test_membership(self):
        assert "postgresql" in [d.value for d in SQLDialect]


# ---------------------------------------------------------------------------
# DDL generation
# ---------------------------------------------------------------------------


class TestGenerateDdl:
    def test_postgresql_basic(self):
        schema = Schema(
            columns=[
                ColumnDef("id", "int64", nullable=False),
                ColumnDef("name", "object"),
            ]
        )
        ddl = generate_ddl(schema, "users", dialect="postgresql")
        assert "CREATE TABLE" in ddl
        assert "users" in ddl
        assert "BIGINT" in ddl
        assert "NOT NULL" in ddl
        assert "TEXT" in ddl

    def test_mysql_basic(self):
        schema = Schema(
            columns=[ColumnDef("id", "int64", nullable=False)]
        )
        ddl = generate_ddl(schema, "users", dialect="mysql")
        assert "CREATE TABLE" in ddl
        assert "BIGINT" in ddl

    def test_sqlite_basic(self):
        schema = Schema(
            columns=[ColumnDef("id", "int64", nullable=False)]
        )
        ddl = generate_ddl(schema, "users", dialect="sqlite")
        assert "CREATE TABLE" in ddl
        assert "INTEGER" in ddl

    def test_if_not_exists(self):
        schema = Schema(columns=[ColumnDef("id", "int64")])
        ddl = generate_ddl(schema, "t", dialect="postgresql")
        assert "IF NOT EXISTS" in ddl

    def test_no_if_not_exists_mysql(self):
        schema = Schema(columns=[ColumnDef("id", "int64")])
        ddl = generate_ddl(schema, "t", dialect="mysql", if_not_exists=True)
        # MySQL doesn't support IF NOT EXISTS for CREATE TABLE in the same way
        assert "CREATE TABLE" in ddl

    def test_invalid_dialect(self):
        schema = Schema(columns=[ColumnDef("id", "int64")])
        with pytest.raises(ValueError, match="Unsupported dialect"):
            generate_ddl(schema, "t", dialect="oracle")

    def test_default_value(self):
        schema = Schema(
            columns=[ColumnDef("status", "object", default="active")]
        )
        ddl = generate_ddl(schema, "t")
        assert "'active'" in ddl

    def test_default_numeric(self):
        schema = Schema(columns=[ColumnDef("count", "int64", default=0)])
        ddl = generate_ddl(schema, "t")
        assert "DEFAULT 0" in ddl

    def test_all_types_postgresql(self):
        """Test all dtype mappings for PostgreSQL."""
        schema = Schema(
            columns=[
                ColumnDef("a", "int64"),
                ColumnDef("b", "int32"),
                ColumnDef("c", "int16"),
                ColumnDef("d", "int8"),
                ColumnDef("e", "float64"),
                ColumnDef("f", "float32"),
                ColumnDef("g", "bool"),
                ColumnDef("h", "object"),
                ColumnDef("i", "datetime64[ns]"),
            ]
        )
        ddl = generate_ddl(schema, "t", dialect="postgresql")
        assert "BIGINT" in ddl
        assert "INTEGER" in ddl
        assert "SMALLINT" in ddl
        assert "DOUBLE PRECISION" in ddl
        assert "REAL" in ddl
        assert "BOOLEAN" in ddl
        assert "TIMESTAMP" in ddl

    def test_all_types_mysql(self):
        schema = Schema(
            columns=[
                ColumnDef("a", "int64"),
                ColumnDef("b", "float64"),
                ColumnDef("c", "datetime64[ns]"),
            ]
        )
        ddl = generate_ddl(schema, "t", dialect="mysql")
        assert "BIGINT" in ddl
        assert "DOUBLE" in ddl
        assert "DATETIME" in ddl

    def test_unknown_type_fallback(self):
        schema = Schema(columns=[ColumnDef("x", "custom_type")])
        ddl = generate_ddl(schema, "t")
        assert "TEXT" in ddl  # Falls back to TEXT

    def test_datetime_with_timezone(self):
        schema = Schema(
            columns=[ColumnDef("ts", "datetime64[ns, UTC]")]
        )
        ddl = generate_ddl(schema, "t", dialect="postgresql")
        assert "TIMESTAMPTZ" in ddl
