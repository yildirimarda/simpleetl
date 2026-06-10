# Apache Iceberg Format

SimpleETL v1.2 supports [Apache Iceberg](https://iceberg.apache.org/)
tables via [pyiceberg](https://py.iceberg.apache.org/) — pure Python, no
Spark required.

## Installation

```bash
pip install simpleetl[iceberg]
```

## Source String Format

Configs and the format factory address Iceberg tables with an
`iceberg://` URI:

```
iceberg:///path/to/warehouse?table=namespace.table
```

A bare table name (no namespace) defaults to the `default` namespace.

## Quick Start

### Zero-Infra Local Warehouse

Without further configuration, SimpleETL creates a local SQLite-backed
catalog (`catalog.db`) inside the warehouse directory:

```python
from simpleetl import IcebergReader, IcebergWriter

writer = IcebergWriter("/data/warehouse", table="sales.orders")
writer.write(df, mode="append")        # append | overwrite | error

reader = IcebergReader("/data/warehouse", table="sales.orders")
df = reader.read()
```

### Read Features

```python
# Column projection and row filtering (pushed down to the scan)
df = reader.read(columns=["id", "amount"], row_filter="amount > 100")

# Snapshot time travel
df_old = reader.read(snapshot_id=some_snapshot_id)

# Streaming in chunks
for chunk in reader.read_chunks(chunk_size=10_000):
    process(chunk)
```

Note: pyiceberg scans don't stream natively; `read_chunks()` materializes
the scan to Arrow first and then yields slices.

### External Catalogs (REST, Glue, Hive)

Pass an explicit `catalog_config` to route through
`pyiceberg.catalog.load_catalog`:

```python
reader = IcebergReader(
    "sales.orders",
    catalog_name="prod",
    catalog_config={
        "type": "rest",
        "uri": "https://iceberg-catalog.example.com",
    },
)
```

## Write Modes

| Mode | Behavior |
|------|----------|
| `append` | Add rows to the table (created automatically if absent) |
| `overwrite` | Replace the table contents |
| `error` | Raise if the table already exists |

Namespaces and tables are created automatically from the DataFrame's
Arrow schema when missing.

## Limitations

- `partition_by` is not supported on write: pyiceberg requires partition
  specs keyed by Iceberg field IDs. Create partitioned tables with
  pyiceberg directly; SimpleETL appends respect the existing spec.
- The SQLite catalog scopes tables by catalog name (default
  `simpleetl`).
