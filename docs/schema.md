# Nested Type Schema Support

SimpleETL supports nested and complex data types in schemas, enabling you to
work with semi-structured data such as JSON records, arrays, and key-value maps.

## Overview

The schema module provides four type classes:

| Class | Description | Example dtype |
|-------|-------------|---------------|
| `FieldDef` | A field within a struct | — |
| `StructType` | Nested record/struct | `struct<name:string,age:int64>` |
| `ArrayType` | Array/list type | `array<string>` |
| `MapType` | Key-value map type | `map<string,int64>` |

## Quick Start

```python
from simpleetl.core.schema import (
    Schema,
    ColumnDef,
    StructType,
    FieldDef,
    ArrayType,
    MapType,
    SQLDialect,
)

# Define a schema with nested types
address_struct = StructType(fields=[
    FieldDef(name="street", dtype="string"),
    FieldDef(name="city", dtype="string"),
    FieldDef(name="zip", dtype="string"),
])

schema = Schema(columns=[
    ColumnDef(name="id", dtype="int64"),
    ColumnDef(name="name", dtype="string"),
    ColumnDef(name="address", dtype="struct<street:string,city:string,zip:string>",
              struct_type=address_struct),
    ColumnDef(name="tags", dtype="array<string>",
              array_type=ArrayType(element_type="string")),
    ColumnDef(name="metadata", dtype="map<string,string>",
              map_type=MapType(key_type="string", value_type="string")),
])
```

## StructType

A `StructType` represents a nested record with named fields:

```python
from simpleetl.core.schema import StructType, FieldDef

# Define a struct type
address = StructType(fields=[
    FieldDef(name="street", dtype="string"),
    FieldDef(name="city", dtype="string"),
    FieldDef(name="zip", dtype="string", nullable=False),
])

print(address.dtype)
# Output: struct<street:string,city:string,zip:string>

# Serialization
data = address.to_dict()
# {"type": "struct", "fields": [{"name": "street", "dtype": "string", ...}, ...]}

restored = StructType.from_dict(data)

# Merge structs (additive — adds new fields)
base = StructType(fields=[FieldDef("a", "int64")])
extended = StructType(fields=[FieldDef("a", "int64"), FieldDef("b", "string")])
merged = base.merge(extended)
```

## ArrayType

An `ArrayType` represents a list of elements:

```python
from simpleetl.core.schema import ArrayType

tags = ArrayType(element_type="string")
print(tags.dtype)
# Output: array<string>

# Serialization
data = tags.to_dict()
# {"type": "array", "element_type": "string"}

restored = ArrayType.from_dict(data)
```

## MapType

A `MapType` represents a key-value mapping:

```python
from simpleetl.core.schema import MapType

metadata = MapType(key_type="string", value_type="string")
print(metadata.dtype)
# Output: map<string,string>

# Serialization
data = metadata.to_dict()
# {"type": "map", "key_type": "string", "value_type": "string"}

restored = MapType.from_dict(data)
```

## FieldDef

A `FieldDef` represents a field within a `StructType`:

```python
from simpleetl.core.schema import FieldDef

field = FieldDef(name="age", dtype="int64", nullable=False)
print(field.to_dict())
# {"name": "age", "dtype": "int64", "nullable": False}

restored = FieldDef.from_dict({"name": "age", "dtype": "int64"})
```

## ColumnDef with Nested Types

`ColumnDef` supports three optional nested type fields:

```python
col = ColumnDef(
    name="address",
    dtype="struct<street:string,city:string>",
    struct_type=address_struct,
)

print(col.is_nested)  # True

# Serialization includes nested type
data = col.to_dict()
# {
#   "name": "address",
#   "dtype": "struct<street:string,city:string>",
#   "nullable": true,
#   ...
#   "struct_type": {"type": "struct", "fields": [...]}
# }

# Restore from serialized data
restored_col = ColumnDef.from_dict(data)
```

## Schema Inference from DataFrames

`Schema.from_dataframe()` automatically detects nested types:

```python
import pandas as pd
from simpleetl.core.schema import Schema

# A DataFrame with nested data
df = pd.DataFrame({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "address": [
        {"street": "123 Main St", "city": "NYC", "zip": "10001"},
        {"street": "456 Oak Ave", "city": "LA", "zip": "90001"},
        {"street": "789 Pine Rd", "city": "Chicago", "zip": "60601"},
    ],
    "tags": [["admin", "user"], ["user"], ["admin"]],
    "metadata": [{"key": "val"}, {"key": "val"}, {"key": "val"}],
})

schema = Schema.from_dataframe(df)

# Dict columns are inferred as StructType
print(schema.columns["address"].dtype)
# Output: struct<street:string,city:string,zip:string>

# List columns are inferred as ArrayType
print(schema.columns["tags"].dtype)
# Output: array<string>
```

## DDL Generation for Nested Types

The `generate_ddl()` function maps nested types to SQL types based on the
dialect:

### PostgreSQL

| SimpleETL Type | SQL Type |
|----------------|----------|
| `StructType` | `JSONB` |
| `ArrayType` | `element_type[]` (e.g., `TEXT[]`) |
| `MapType` | `JSONB` |

### MySQL

| SimpleETL Type | SQL Type |
|----------------|----------|
| `StructType` | `JSON` |
| `ArrayType` | `JSON` |
| `MapType` | `JSON` |

### SQLite

| SimpleETL Type | SQL Type |
|----------------|----------|
| `StructType` | `JSON` |
| `ArrayType` | `JSON` |
| `MapType` | `JSON` |

### Example

```python
from simpleetl.core.schema import generate_ddl, SQLDialect

schema = Schema(columns=[...])

ddl = generate_dl(schema, dialect=SQLDialect.POSTGRESQL)
print(ddl)
# CREATE TABLE users (
#   id BIGINT,
#   address JSONB,
#   tags TEXT[],
#   metadata JSONB
# );
```

## Schema Diff with Nested Types

Schema evolution detects changes in nested types:

```python
from simpleetl.core.schema import SchemaDiff

old_schema = Schema.from_dataframe(df_v1)
new_schema = Schema.from_dataframe(df_v2)

diff = Schema.diff(old_schema, new_schema)
print(diff.type_changes)
# {"address": {"old": "struct<street:string,city:string>",
#               "new": "struct<street:string,city:string,zip:string>"}}
```
