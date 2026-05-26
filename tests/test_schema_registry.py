"""
Tests for schema_registry.py.

Covers: FileSchemaRegistry (register, get, get_latest, list_versions, list_schemas),
SchemaRegistry abstract class, error handling.
"""

import pytest
import tempfile
from pathlib import Path

from simpleetl.core.schema import Schema, ColumnDef
from simpleetl.core.schema_registry import (
    FileSchemaRegistry,
    SchemaRegistry,
)


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield d


@pytest.fixture
def registry(tmp_dir):
    return FileSchemaRegistry(tmp_dir)


@pytest.fixture
def sample_schema():
    return Schema(
        columns=[
            ColumnDef("id", "int64", nullable=False),
            ColumnDef("name", "object"),
        ]
    )


class TestFileSchemaRegistryInit:
    def test_creates_directory(self, tmp_dir):
        path = Path(tmp_dir) / "new_subdir"
        FileSchemaRegistry(str(path))
        assert path.exists()

    def test_existing_directory(self, tmp_dir):
        reg = FileSchemaRegistry(tmp_dir)
        assert reg._base_dir == Path(tmp_dir)


class TestRegisterSchema:
    def test_register(self, registry, sample_schema):
        registry.register_schema("users", 1, sample_schema)
        path = registry._schema_path("users", 1)
        assert path.exists()

    def test_register_multiple_versions(self, registry, sample_schema):
        registry.register_schema("users", 1, sample_schema)
        registry.register_schema("users", 2, sample_schema)
        versions = registry.list_versions("users")
        assert versions == [1, 2]

    def test_register_invalid_version(self, registry, sample_schema):
        with pytest.raises(ValueError, match="positive integer"):
            registry.register_schema("users", 0, sample_schema)

    def test_register_negative_version(self, registry, sample_schema):
        with pytest.raises(ValueError, match="positive integer"):
            registry.register_schema("users", -1, sample_schema)

    def test_register_overwrite(self, registry, sample_schema):
        registry.register_schema("users", 1, sample_schema)
        new_schema = Schema(columns=[ColumnDef("email", "object")])
        registry.register_schema("users", 1, new_schema)
        restored = registry.get_schema("users", 1)
        assert restored.column_names == ["email"]


class TestGetSchema:
    def test_get_existing(self, registry, sample_schema):
        registry.register_schema("users", 1, sample_schema)
        restored = registry.get_schema("users", 1)
        assert restored.column_names == ["id", "name"]

    def test_get_nonexistent_name(self, registry):
        with pytest.raises(KeyError, match="not found"):
            registry.get_schema("nonexistent", 1)

    def test_get_nonexistent_version(self, registry, sample_schema):
        registry.register_schema("users", 1, sample_schema)
        with pytest.raises(KeyError, match="not found"):
            registry.get_schema("users", 99)


class TestGetLatestSchema:
    def test_get_latest(self, registry, sample_schema):
        registry.register_schema("users", 1, sample_schema)
        registry.register_schema("users", 3, sample_schema)
        registry.register_schema("users", 2, sample_schema)
        latest = registry.get_latest_schema("users")
        assert latest == sample_schema

    def test_get_latest_nonexistent(self, registry):
        with pytest.raises(KeyError, match="No versions found"):
            registry.get_latest_schema("nonexistent")


class TestListVersions:
    def test_list_versions(self, registry, sample_schema):
        registry.register_schema("users", 1, sample_schema)
        registry.register_schema("users", 3, sample_schema)
        registry.register_schema("users", 2, sample_schema)
        versions = registry.list_versions("users")
        assert versions == [1, 2, 3]

    def test_list_versions_nonexistent(self, registry):
        with pytest.raises(KeyError, match="not found"):
            registry.list_versions("nonexistent")

    def test_list_versions_empty_dir(self, registry, tmp_dir):
        # Create an empty schema directory
        (Path(tmp_dir) / "empty_schema").mkdir()
        versions = registry.list_versions("empty_schema")
        assert versions == []


class TestListSchemas:
    def test_list_schemas(self, registry, sample_schema):
        registry.register_schema("users", 1, sample_schema)
        registry.register_schema("orders", 1, sample_schema)
        schemas = registry.list_schemas()
        assert schemas == ["orders", "users"]

    def test_list_schemas_empty(self, registry):
        assert registry.list_schemas() == []


class TestSchemaRegistryAbstract:
    def test_cannot_instantiate(self):
        with pytest.raises(TypeError):
            SchemaRegistry()

    def test_subclass_without_implementation(self):
        class IncompleteRegistry(SchemaRegistry):
            pass

        with pytest.raises(TypeError):
            IncompleteRegistry()


class TestRoundTrip:
    def test_complex_schema_round_trip(self, registry):
        schema = Schema(
            columns=[
                ColumnDef("id", "int64", nullable=False, description="PK"),
                ColumnDef("name", "object", default="unknown"),
                ColumnDef("score", "float64"),
                ColumnDef("active", "bool", nullable=False),
                ColumnDef("created", "datetime64[ns]"),
            ],
            metadata={"source": "test_db", "version": "2.0"},
        )
        registry.register_schema("test_table", 1, schema)
        restored = registry.get_schema("test_table", 1)
        assert len(restored) == 5
        assert restored.get_column("id").nullable is False
        assert restored.get_column("name").default == "unknown"
        assert restored.metadata["source"] == "test_db"
