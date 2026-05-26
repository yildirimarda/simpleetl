"""
Tests for nested/complex type schema support.

Covers: FieldDef, StructType, ArrayType, MapType, ColumnDef nested
properties, Schema inference from DataFrames, DDL generation for nested
types, and schema diff with nested types.
"""

import json

import pandas as pd

from simpleetl.core.schema import (
    ArrayType,
    ColumnDef,
    FieldDef,
    MapType,
    Schema,
    StructType,
    generate_ddl,
)


# ---------------------------------------------------------------------------
# FieldDef tests
# ---------------------------------------------------------------------------


class TestFieldDef:
    def test_creation(self):
        f = FieldDef(name="age", dtype="int64", nullable=True)
        assert f.name == "age"
        assert f.dtype == "int64"
        assert f.nullable is True

    def test_creation_nullable_default(self):
        f = FieldDef(name="x", dtype="string")
        assert f.nullable is True

    def test_creation_not_nullable(self):
        f = FieldDef(name="id", dtype="int64", nullable=False)
        assert f.nullable is False

    def test_to_dict(self):
        f = FieldDef(name="score", dtype="float64", nullable=False)
        d = f.to_dict()
        assert d == {"name": "score", "dtype": "float64", "nullable": False}

    def test_from_dict(self):
        data = {"name": "x", "dtype": "int64", "nullable": True}
        f = FieldDef.from_dict(data)
        assert f.name == "x"
        assert f.dtype == "int64"
        assert f.nullable is True

    def test_from_dict_default_nullable(self):
        data = {"name": "x", "dtype": "string"}
        f = FieldDef.from_dict(data)
        assert f.nullable is True

    def test_round_trip(self):
        original = FieldDef(name="val", dtype="float64", nullable=False)
        restored = FieldDef.from_dict(original.to_dict())
        assert restored == original


# ---------------------------------------------------------------------------
# StructType tests
# ---------------------------------------------------------------------------


class TestStructType:
    def test_creation(self):
        fields = [FieldDef("a", "int64"), FieldDef("b", "string")]
        st = StructType(fields=fields)
        assert len(st.fields) == 2
        assert st.fields[0].name == "a"

    def test_dtype_property(self):
        fields = [FieldDef("x", "int64"), FieldDef("y", "string")]
        st = StructType(fields=fields)
        assert st.dtype == "struct<x:int64,y:string>"

    def test_dtype_single_field(self):
        st = StructType(fields=[FieldDef("a", "int64")])
        assert st.dtype == "struct<a:int64>"

    def test_to_dict(self):
        fields = [FieldDef("a", "int64")]
        st = StructType(fields=fields)
        d = st.to_dict()
        assert d == {"type": "struct", "fields": [{"name": "a", "dtype": "int64", "nullable": True}]}

    def test_from_dict(self):
        data = {
            "type": "struct",
            "fields": [{"name": "a", "dtype": "float64", "nullable": False}],
        }
        st = StructType.from_dict(data)
        assert len(st.fields) == 1
        assert st.fields[0].name == "a"
        assert st.fields[0].dtype == "float64"

    def test_from_dict_multiple_fields(self):
        data = {
            "type": "struct",
            "fields": [
                {"name": "x", "dtype": "int64"},
                {"name": "y", "dtype": "string"},
            ],
        }
        st = StructType.from_dict(data)
        assert len(st.fields) == 2

    def test_round_trip(self):
        original = StructType(
            fields=[FieldDef("a", "int64"), FieldDef("b", "string")]
        )
        restored = StructType.from_dict(original.to_dict())
        assert restored.dtype == original.dtype
        assert len(restored.fields) == len(original.fields)

    def test_merge_adds_new_fields(self):
        s1 = StructType(fields=[FieldDef("a", "int64")])
        s2 = StructType(fields=[FieldDef("a", "int64"), FieldDef("b", "string")])
        merged = s1.merge(s2)
        assert len(merged.fields) == 2
        names = [f.name for f in merged.fields]
        assert names == ["a", "b"]

    def test_merge_preserves_order(self):
        s1 = StructType(fields=[FieldDef("a", "int64")])
        s2 = StructType(fields=[FieldDef("c", "string"), FieldDef("b", "float64")])
        merged = s1.merge(s2)
        names = [f.name for f in merged.fields]
        assert names == ["a", "c", "b"]

    def test_merge_no_duplicates(self):
        s1 = StructType(fields=[FieldDef("a", "int64"), FieldDef("b", "string")])
        s2 = StructType(fields=[FieldDef("a", "int64"), FieldDef("b", "string")])
        merged = s1.merge(s2)
        assert len(merged.fields) == 2

    def test_merge_empty_other(self):
        s1 = StructType(fields=[FieldDef("a", "int64")])
        s2 = StructType(fields=[])
        merged = s1.merge(s2)
        assert len(merged.fields) == 1


# ---------------------------------------------------------------------------
# ArrayType tests
# ---------------------------------------------------------------------------


class TestArrayType:
    def test_creation(self):
        at = ArrayType(element_type="int64")
        assert at.element_type == "int64"

    def test_dtype_property(self):
        at = ArrayType(element_type="string")
        assert at.dtype == "array<string>"

    def test_dtype_numeric(self):
        at = ArrayType(element_type="float64")
        assert at.dtype == "array<float64>"

    def test_to_dict(self):
        at = ArrayType(element_type="int64")
        d = at.to_dict()
        assert d == {"type": "array", "element_type": "int64"}

    def test_from_dict(self):
        data = {"type": "array", "element_type": "string"}
        at = ArrayType.from_dict(data)
        assert at.element_type == "string"

    def test_round_trip(self):
        original = ArrayType(element_type="float64")
        restored = ArrayType.from_dict(original.to_dict())
        assert restored.dtype == original.dtype


# ---------------------------------------------------------------------------
# MapType tests
# ---------------------------------------------------------------------------


class TestMapType:
    def test_creation(self):
        mt = MapType(key_type="string", value_type="int64")
        assert mt.key_type == "string"
        assert mt.value_type == "int64"

    def test_dtype_property(self):
        mt = MapType(key_type="string", value_type="float64")
        assert mt.dtype == "map<string,float64>"

    def test_dtype_simple(self):
        mt = MapType(key_type="int64", value_type="int64")
        assert mt.dtype == "map<int64,int64>"

    def test_to_dict(self):
        mt = MapType(key_type="string", value_type="int64")
        d = mt.to_dict()
        assert d == {
            "type": "map",
            "key_type": "string",
            "value_type": "int64",
        }

    def test_from_dict(self):
        data = {"type": "map", "key_type": "string", "value_type": "float64"}
        mt = MapType.from_dict(data)
        assert mt.key_type == "string"
        assert mt.value_type == "float64"

    def test_round_trip(self):
        original = MapType(key_type="string", value_type="int64")
        restored = MapType.from_dict(original.to_dict())
        assert restored.dtype == original.dtype


# ---------------------------------------------------------------------------
# ColumnDef nested type tests
# ---------------------------------------------------------------------------


class TestColumnDefNested:
    def test_is_nested_false_for_simple(self):
        col = ColumnDef(name="id", dtype="int64")
        assert col.is_nested is False

    def test_is_nested_with_struct(self):
        col = ColumnDef(
            name="data",
            dtype="struct<a:int64>",
            struct_type=StructType(fields=[FieldDef("a", "int64")]),
        )
        assert col.is_nested is True

    def test_is_nested_with_array(self):
        col = ColumnDef(
            name="tags",
            dtype="array<string>",
            array_type=ArrayType(element_type="string"),
        )
        assert col.is_nested is True

    def test_is_nested_with_map(self):
        col = ColumnDef(
            name="meta",
            dtype="map<string,string>",
            map_type=MapType(key_type="string", value_type="string"),
        )
        assert col.is_nested is True

    def test_to_dict_includes_struct_type(self):
        col = ColumnDef(
            name="data",
            dtype="struct<a:int64>",
            struct_type=StructType(fields=[FieldDef("a", "int64")]),
        )
        d = col.to_dict()
        assert "struct_type" in d
        assert d["struct_type"]["type"] == "struct"

    def test_to_dict_includes_array_type(self):
        col = ColumnDef(
            name="tags",
            dtype="array<string>",
            array_type=ArrayType(element_type="string"),
        )
        d = col.to_dict()
        assert "array_type" in d
        assert d["array_type"]["type"] == "array"

    def test_to_dict_includes_map_type(self):
        col = ColumnDef(
            name="meta",
            dtype="map<string,int64>",
            map_type=MapType(key_type="string", value_type="int64"),
        )
        d = col.to_dict()
        assert "map_type" in d
        assert d["map_type"]["type"] == "map"

    def test_from_dict_restores_struct_type(self):
        data = {
            "name": "data",
            "dtype": "struct<a:int64>",
            "struct_type": {
                "type": "struct",
                "fields": [{"name": "a", "dtype": "int64"}],
            },
        }
        col = ColumnDef.from_dict(data)
        assert col.struct_type is not None
        assert col.struct_type.dtype == "struct<a:int64>"

    def test_from_dict_restores_array_type(self):
        data = {
            "name": "tags",
            "dtype": "array<string>",
            "array_type": {"type": "array", "element_type": "string"},
        }
        col = ColumnDef.from_dict(data)
        assert col.array_type is not None
        assert col.array_type.dtype == "array<string>"

    def test_from_dict_restores_map_type(self):
        data = {
            "name": "meta",
            "dtype": "map<string,int64>",
            "map_type": {
                "type": "map",
                "key_type": "string",
                "value_type": "int64",
            },
        }
        col = ColumnDef.from_dict(data)
        assert col.map_type is not None
        assert col.map_type.dtype == "map<string,int64>"

    def test_round_trip_with_all_nested_types(self):
        col = ColumnDef(
            name="mixed",
            dtype="struct<a:int64>",
            nullable=False,
            description="test col",
            struct_type=StructType(fields=[FieldDef("a", "int64")]),
            array_type=ArrayType(element_type="string"),
            map_type=MapType(key_type="string", value_type="int64"),
        )
        d = col.to_dict()
        restored = ColumnDef.from_dict(d)
        assert restored.name == "mixed"
        assert restored.nullable is False
        assert restored.description == "test col"
        assert restored.struct_type is not None
        assert restored.array_type is not None
        assert restored.map_type is not None


# ---------------------------------------------------------------------------
# Schema nested type inference
# ---------------------------------------------------------------------------


class TestSchemaNestedInference:
    def test_infer_struct_from_dict_column(self):
        df = pd.DataFrame(
            {
                "id": [1, 2],
                "meta": [
                    {"city": "NY", "zip": 10001},
                    {"city": "LA", "zip": 90001},
                ],
            }
        )
        schema = Schema.from_dataframe(df)
        meta_col = schema.get_column("meta")
        assert meta_col is not None
        assert meta_col.struct_type is not None
        assert meta_col.is_nested is True
        field_names = [f.name for f in meta_col.struct_type.fields]
        assert "city" in field_names
        assert "zip" in field_names

    def test_infer_array_from_list_column(self):
        df = pd.DataFrame(
            {
                "id": [1, 2],
                "tags": [["a", "b"], ["c"]],
            }
        )
        schema = Schema.from_dataframe(df)
        tags_col = schema.get_column("tags")
        assert tags_col is not None
        assert tags_col.array_type is not None
        assert tags_col.is_nested is True
        assert tags_col.array_type.element_type == "string"

    def test_infer_array_of_ints(self):
        df = pd.DataFrame(
            {
                "id": [1],
                "scores": [[10, 20, 30]],
            }
        )
        schema = Schema.from_dataframe(df)
        scores_col = schema.get_column("scores")
        assert scores_col.array_type is not None
        assert scores_col.array_type.element_type == "int64"

    def test_mixed_columns_with_nested(self):
        df = pd.DataFrame(
            {
                "id": [1],
                "name": ["Alice"],
                "meta": [{"key": "val"}],
            }
        )
        schema = Schema.from_dataframe(df)
        assert len(schema) == 3
        assert schema.get_column("id").is_nested is False
        assert schema.get_column("name").is_nested is False
        assert schema.get_column("meta").is_nested is True


# ---------------------------------------------------------------------------
# DDL generation for nested types
# ---------------------------------------------------------------------------


class TestSchemaNestedDDL:
    def test_struct_postgresql(self):
        col = ColumnDef(
            name="data",
            dtype="struct<a:int64>",
            struct_type=StructType(fields=[FieldDef("a", "int64")]),
        )
        schema = Schema(columns=[ColumnDef("id", "int64"), col])
        ddl = generate_ddl(schema, "t", dialect="postgresql")
        assert "JSONB" in ddl

    def test_struct_mysql(self):
        col = ColumnDef(
            name="data",
            dtype="struct<a:int64>",
            struct_type=StructType(fields=[FieldDef("a", "int64")]),
        )
        schema = Schema(columns=[col])
        ddl = generate_ddl(schema, "t", dialect="mysql")
        assert "JSON" in ddl
        assert "JSONB" not in ddl

    def test_struct_sqlite(self):
        col = ColumnDef(
            name="data",
            dtype="struct<a:int64>",
            struct_type=StructType(fields=[FieldDef("a", "int64")]),
        )
        schema = Schema(columns=[col])
        ddl = generate_ddl(schema, "t", dialect="sqlite")
        assert "JSON" in ddl

    def test_array_postgresql(self):
        col = ColumnDef(
            name="tags",
            dtype="array<string>",
            array_type=ArrayType(element_type="string"),
        )
        schema = Schema(columns=[col])
        ddl = generate_ddl(schema, "t", dialect="postgresql")
        assert "string[]" in ddl

    def test_array_mysql(self):
        col = ColumnDef(
            name="tags",
            dtype="array<string>",
            array_type=ArrayType(element_type="string"),
        )
        schema = Schema(columns=[col])
        ddl = generate_ddl(schema, "t", dialect="mysql")
        assert "JSON" in ddl

    def test_array_sqlite(self):
        col = ColumnDef(
            name="tags",
            dtype="array<string>",
            array_type=ArrayType(element_type="string"),
        )
        schema = Schema(columns=[col])
        ddl = generate_ddl(schema, "t", dialect="sqlite")
        assert "JSON" in ddl

    def test_map_postgresql(self):
        col = ColumnDef(
            name="meta",
            dtype="map<string,int64>",
            map_type=MapType(key_type="string", value_type="int64"),
        )
        schema = Schema(columns=[col])
        ddl = generate_ddl(schema, "t", dialect="postgresql")
        assert "JSONB" in ddl

    def test_map_mysql(self):
        col = ColumnDef(
            name="meta",
            dtype="map<string,int64>",
            map_type=MapType(key_type="string", value_type="int64"),
        )
        schema = Schema(columns=[col])
        ddl = generate_ddl(schema, "t", dialect="mysql")
        assert "JSON" in ddl

    def test_map_sqlite(self):
        col = ColumnDef(
            name="meta",
            dtype="map<string,int64>",
            map_type=MapType(key_type="string", value_type="int64"),
        )
        schema = Schema(columns=[col])
        ddl = generate_ddl(schema, "t", dialect="sqlite")
        assert "JSON" in ddl

    def test_mixed_nested_and_simple_ddl(self):
        schema = Schema(
            columns=[
                ColumnDef("id", "int64", nullable=False),
                ColumnDef(
                    name="data",
                    dtype="struct<a:int64>",
                    struct_type=StructType(fields=[FieldDef("a", "int64")]),
                ),
                ColumnDef("name", "object"),
            ]
        )
        ddl = generate_ddl(schema, "t", dialect="postgresql")
        assert "BIGINT" in ddl
        assert "NOT NULL" in ddl
        assert "JSONB" in ddl
        assert "TEXT" in ddl


# ---------------------------------------------------------------------------
# Schema diff with nested types
# ---------------------------------------------------------------------------


class TestSchemaDiffNested:
    def test_nested_type_change_detected(self):
        old = Schema(
            columns=[
                ColumnDef(
                    name="data",
                    dtype="struct<a:int64>",
                    struct_type=StructType(fields=[FieldDef("a", "int64")]),
                ),
            ]
        )
        new = Schema(
            columns=[
                ColumnDef(
                    name="data",
                    dtype="struct<a:string>",
                    struct_type=StructType(fields=[FieldDef("a", "string")]),
                ),
            ]
        )
        diff = old.diff(new)
        assert "data" in diff.type_changes

    def test_nested_type_no_change(self):
        col = ColumnDef(
            name="data",
            dtype="struct<a:int64>",
            struct_type=StructType(fields=[FieldDef("a", "int64")]),
        )
        s = Schema(columns=[col])
        diff = s.diff(s)
        assert diff.has_changes is False


# ---------------------------------------------------------------------------
# Schema serialization round-trip with nested types
# ---------------------------------------------------------------------------


class TestSchemaNestedSerialization:
    def test_round_trip_with_struct(self):
        schema = Schema(
            columns=[
                ColumnDef("id", "int64", nullable=False),
                ColumnDef(
                    name="meta",
                    dtype="struct<city:string,zip:int64>",
                    struct_type=StructType(
                        fields=[
                            FieldDef("city", "string"),
                            FieldDef("zip", "int64"),
                        ]
                    ),
                ),
            ],
            metadata={"version": "2"},
        )
        d = schema.to_dict()
        j = json.dumps(d)
        parsed = json.loads(j)
        restored = Schema.from_dict(parsed)
        assert restored.column_names == schema.column_names
        assert restored.metadata["version"] == "2"
        meta_col = restored.get_column("meta")
        assert meta_col is not None
        assert meta_col.struct_type is not None
        assert len(meta_col.struct_type.fields) == 2
