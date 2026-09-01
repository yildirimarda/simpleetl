"""Tests that new public classes are exported from simpleetl.__init__."""

import simpleetl


def test_parallel_classes_exported():
    assert simpleetl.ParallelReader is not None
    assert simpleetl.ParallelWriter is not None
    assert simpleetl.PartitionStrategy is not None
    assert simpleetl.LazyTransformation is not None


def test_parallel_functions_exported():
    assert callable(simpleetl.parallel_read)
    assert callable(simpleetl.parallel_write)


def test_table_exported():
    assert simpleetl.Table is not None


def test_schema_registry_classes_exported():
    assert simpleetl.SchemaRegistry is not None
    assert simpleetl.FileSchemaRegistry is not None
