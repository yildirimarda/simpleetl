# SimpleETL

[![Version](https://img.shields.io/badge/version-1.0.0-blue.svg)](https://github.com/yourusername/simpleetl)
[![Python](https://img.shields.io/badge/python-3.9%2B-green.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-yellow.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-1540%20passed-brightgreen.svg)]()
[![Coverage](https://img.shields.io/badge/coverage-94%25-brightgreen.svg)]()

A **production-grade** ETL framework for Python. Designed to run on **local**, **AWS Glue**, **Databricks**, and **Azure Synapse** platforms. Supports all major data formats with a focus on simplicity, readability, and observability.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Core Concepts](#core-concepts)
  - [ETL Job Lifecycle](#etl-job-lifecycle)
  - [Configuration](#configuration)
  - [Format System](#format-system)
  - [Transformations](#transformations)
  - [Hooks System](#hooks-system)
- [Advanced Features](#advanced-features)
  - [Incremental Loading](#incremental-loading)
  - [Streaming & Chunked Processing](#streaming--chunked-processing)
  - [DAG Orchestration](#dag-orchestration)
  - [Schema Management](#schema-management)
  - [Nested Types](#nested-types)
  - [Data Lineage & Observability](#data-lineage--observability)
  - [Per-Record Provenance](#per-record-provenance)
  - [Alerting](#alerting)
  - [OpenLineage Integration](#openlineage-integration)
  - [Data Quality](#data-quality)
  - [Security](#security)
- [Platform Support](#platform-support)
- [Performance Benchmarks](#performance-benchmarks)
- [Documentation](#documentation)
- [Running Tests](#running-tests)
- [Project Structure](#project-structure)
- [License](#license)
- [Contributing](#contributing)

---

## Features

- **Simple and Clean API**: Intuitive job lifecycle — `extract()`, `transform()`, `load()`
- **Multi-Platform**: Runs locally, on AWS Glue, Databricks, and Azure Synapse with auto-detection
- **Multiple Formats**: CSV, JSON, Parquet, Avro, ORC, XML, Excel, and JDBC databases
- **Cloud Storage**: S3, GCS, ABFS via fsspec with unified path handling
- **Incremental Loading**: Watermark-based delta loading with checkpoint/resume
- **Streaming & Chunked**: Process datasets larger than memory with chunked I/O
- **DAG Orchestration**: Define job dependencies with topological scheduling
- **Data Lineage**: Full lineage tracking from source to destination with OpenLineage export
- **Per-Record Provenance**: Trace transformation history for individual records
- **Alerting**: Rule-based alerts via webhooks, Slack, and email
- **Schema Management**: Inference, evolution, nested types, and DDL generation
- **Security**: Secrets management (AWS, Azure, Vault), audit logging, RBAC, PII masking
- **Data Quality**: Schema validation, null checks, duplicate detection, value ranges
- **Plugin System**: Extensible via hooks and entry_points-based discovery
- **Production Ready**: Docker, Kubernetes, Prometheus metrics, structured logging
- **Lightweight Core**: Only 6 core dependencies; cloud/spark/db as optional extras
- **High Quality**: 1540 tests, 94% coverage, ruff + mypy clean

## Installation

### Prerequisites

- Python 3.9+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

### Install from Source

```bash
git clone https://github.com/yourusername/simpleetl.git
cd simpleetl

# Install core (recommended)
uv sync

# Or install with all optional dependencies
uv sync --all-extras

# Or install specific extras
uv sync --extra cloud     # S3, GCS, ABFS support
uv sync --extra aws       # AWS Glue, Secrets Manager
uv sync --extra spark     # PySpark (Databricks, Synapse)
uv sync --extra databases # PostgreSQL, MySQL, SQL Server drivers
uv sync --extra secrets   # HashiCorp Vault, Azure Key Vault
uv sync --extra monitoring # Prometheus metrics
```

### Install from PyPI (when available)

```bash
pip install simpleetl

# Or with extras
pip install "simpleetl[all]"
```

### Verify Installation

```bash
uv run python -m simpleetl --version
# Output: simpleetl 1.0.0
```

## Quick Start

### 1. Create a Configuration File (`job.yaml`)

```yaml
name: quickstart_job
description: "Filter users by age and write to Parquet"
platform: local
input_format: csv
output_format: parquet
log_level: INFO
params:
  input_path: data/users.csv
  output_path: data/adults.parquet
  filter_column: age
  filter_min_value: 18
```

### 2. Define Your Job

```python
from simpleetl.core.job import ETLJob
from simpleetl.formats import FormatFactory

class QuickstartJob(ETLJob):
    def extract(self):
        reader = FormatFactory.get_reader(self.config.params["input_path"])
        return reader.read(self.config.params["input_path"])

    def transform(self, data):
        col = self.config.params.get("filter_column", "age")
        min_val = self.config.params.get("filter_min_value", 0)
        return data[data[col] >= min_val]

    def load(self, data):
        writer = FormatFactory.get_writer(self.config.params["output_path"])
        writer.write(data, self.config.params["output_path"])

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    job = QuickstartJob("job.yaml")
    job.run_with_error_handling()
```

### 3. Run

```bash
uv run python my_job.py

# Or via CLI
uv run simpleetl --config job.yaml
```

### Alternatively: Use the Top-Level API

```python
import simpleetl

# Read, filter, and write in one line
df = simpleetl.read("data/users.csv")
filtered = df[df["age"] >= 18]
simpleetl.write(filtered, "data/adults.parquet")
```

---

## Core Concepts

### ETL Job Lifecycle

Every SimpleETL job follows the `extract → transform → load` lifecycle:

```python
from simpleetl.core.job import ETLJob

class MyJob(ETLJob):
    def extract(self):
        # Read data from source — returns a DataFrame
        return self.reader.read("source.csv")

    def transform(self, data):
        # Transform the data — must return a DataFrame
        return data[data["status"] == "active"]

    def load(self, data):
        # Write data to destination — no return value
        self.writer.write(data, "output.parquet")

    def run(self):
        # Optional: override for custom lifecycle control
        data = self.extract()
        data = self.transform(data)
        self.load(data)
```

Jobs emit **hooks** at each phase (`pre_extract`, `post_extract`, `pre_transform`, `post_transform`, `pre_load`, `post_load`), enabling automatic lineage tracking, metrics collection, and provenance recording.

### Configuration

Jobs are configured via YAML or JSON files validated by Pydantic:

```yaml
name: my_job
platform: local                    # local | glue | databricks | synapse
input_format: csv                  # csv | json | parquet | avro | orc | xml | excel | database
output_format: parquet
max_retries: 3                     # Retry count on failure
retry_delay: 1.0                   # Initial delay (exponential backoff applied)
log_level: INFO
incremental: true                  # Enable watermark-based incremental loading
incremental_column: updated_at     # Column for watermark tracking
incremental_strategy: watermark    # watermark | cdc
database:
  url: "${DB_URL}"                 # Env var interpolation
  pool_size: 5
  ssl_mode: require
openlineage_url: "http://marquez:5000/api/v1/lineage"
params:
  input_path: s3://bucket/data.csv
  bucket: my-bucket
```

**Features:**
- **Environment variable interpolation**: `${VAR}`, `${VAR:-default}`
- **Auto-loading from env prefix**: Set `env_prefix: "ETL_"` to auto-load `ETL_BATCH_SIZE` into `params.batch_size`
- **Secrets resolution**: `${secrets://my-secret/key}` resolved via AWS, Azure, or Vault

```python
from simpleetl.core.config import load_config, ETLJobConfig

# Load from file
config = load_config("job.yaml")

# Or create programmatically
config = ETLJobConfig(
    name="my_job",
    input_format="csv",
    output_format="parquet",
    params={"input_path": "data.csv"},
    env_prefix="ETL_",  # Auto-loads ETL_* env vars into params
)
```

### Format System

SimpleETL uses a plugin-based format system with auto-detection:

```python
from simpleetl.formats import FormatFactory

# Auto-detect from file extension
reader = FormatFactory.get_reader("data/input.parquet")
writer = FormatFactory.get_writer("data/output.json")

# Check supported formats
formats = FormatFactory.supported_formats()

# Read/write cloud paths seamlessly
reader = FormatFactory.get_reader("s3://bucket/data.csv")
reader = FormatFactory.get_writer("gs://bucket/output.parquet")
reader = FormatFactory.get_writer("abfss://container@account.dfs.core.windows.net/data.avro")
```

**Supported Formats:**

| Format | Reader | Writer | Notes |
|--------|--------|--------|-------|
| CSV | ✅ | ✅ | Chunked I/O, compression (gzip, snappy) |
| JSON | ✅ | ✅ | Line-delimited and array modes |
| Parquet | ✅ | ✅ | Column pruning, row group filtering |
| Avro | ✅ | ✅ | fastavro + pyarrow fallback |
| ORC | ✅ | ✅ | Stream-based PyArrow |
| XML | ✅ | ✅ | xmltodict-based |
| Excel | ✅ | ✅ | openpyxl-based |
| Database | ✅ | ✅ | SQLAlchemy, chunked reads, UPSERT |
| Glue Catalog | ✅ | — | Schema inference from AWS Glue |

### Transformations

Built-in reusable transformation functions:

```python
from simpleetl.transformations import (
    filter_data, map_values, aggregate_data,
    join_data, union_data, deduplicate_data,
    window_functions, string_operations, date_operations,
    transform_chain
)

# Filter
filtered = filter_data(df, column="age", min_value=18, max_value=65)

# Map values
mapped = map_values(df, "status", {"A": "Active", "I": "Inactive"})
mapped = map_values(df, "name", lambda x: x.upper())

# Join
joined = join_data(df1, df2, on="id", how="inner")

# Aggregate
agg = aggregate_data(df, groupby=["city"], agg={"revenue": "sum", "id": "count"})

# Window functions
ranked = window_functions(df, func="rank", order_by="score", partition_by="city")

# String operations
cleaned = string_operations(df, column="email", operation="lower")
matched = string_operations(df, column="phone", operation="regex_extract", pattern=r"\d{3}-\d{4}")

# Date operations
df = date_operations(df, column="created_at", operation="extract", extract_field="year")
df = date_operations(df, column="created_at", operation="trunc", trunc_unit="month")

# Chain multiple transformations
result = transform_chain(df, [
    lambda d: filter_data(d, column="age", min_value=18),
    lambda d: map_values(d, "name", lambda x: x.strip()),
    lambda d: deduplicate_data(d, subset=["id"]),
])
```

### Hooks System

Hooks intercept the ETL lifecycle at pre/post phases:

```python
from simpleetl.core.hooks import Hook, HookContext, POST_TRANSFORM

class CustomHook(Hook):
    name = "custom"
    priority = 10  # Lower = earlier execution

    def execute(self, context: HookContext):
        if context.phase == POST_TRANSFORM:
            print(f"Transformed {len(context.data)} rows")

job.add_hook(CustomHook())
```

**Built-in hooks:**
- `LineageHook` — automatic data lineage event recording
- `MetricsHook` — Prometheus metrics at each phase
- `ProvenanceHook` — per-record provenance tracking
- `CheckpointHook` — automatic checkpoint saving

---

## Advanced Features

### Incremental Loading

Process only new or changed records using watermark-based delta loading:

```yaml
# config.yaml
incremental: true
incremental_column: updated_at
incremental_strategy: watermark
watermark_store: file  # file | database
```

```python
class IncrementalJob(ETLJob):
    def extract(self, **kwargs):
        return self.reader.read(
            self.config.params["input_path"],
            incremental_column=self.config.incremental_column,
            watermark_value=self.get_high_watermark(),
        )
```

Watermarks are automatically tracked between job runs. Checkpoint/resume is supported for long-running jobs.

### Streaming & Chunked Processsing

Handle datasets larger than available memory:

```python
# Chunked read — processes data in batches
reader = FormatFactory.get_reader("large_file.parquet")
for chunk in reader.read_chunks("large_file.parquet", chunk_size=10_000):
    process(chunk)

# Chunked write — streams to output without loading all into memory
writer = FormatFactory.get_writer("output.parquet")
with writer.open("output.parquet") as w:
    for batch in generate_batches():
        w.write_batch(batch)
```

All formats support chunked I/O. Benchmark results show chunked JSON reads reduce memory by 95% at 500K rows.

### DAG Orchestration

Define job dependencies as a directed acyclic graph:

```yaml
# pipeline.yaml
name: etl_pipeline
jobs:
  - name: extract_users
    job_class: my_jobs.ExtractUsers
    config_path: configs/extract_users.yaml
  - name: extract_orders
    job_class: my_jobs.ExtractOrders
    config_path: configs/extract_orders.yaml
  - name: join_data
    job_class: my_jobs.JoinData
    config_path: configs/join.yaml
    depends_on: [extract_users, extract_orders]
  - name: load_warehouse
    job_class: my_jobs.LoadWarehouse
    config_path: configs/load.yaml
    depends_on: [join_data]
```

```python
from simpleetl.core.dag import DAG

dag = DAG.from_yaml("pipeline.yaml")      # or DAG.from_dict(...)
dag.validate()                             # Check for cycles, missing deps
plan = dag.get_execution_plan()            # View execution order
dag.run()                                  # Execute with dependency resolution
```

Features: topological sort, parallel group detection, fan-out/fan-in patterns, cycle detection.

### Schema Management

```python
from simpleetl.core.schema import Schema, ColumnDef, SchemaDiff, SQLDialect

# Define schema
schema = Schema(columns=[
    ColumnDef(name="id", dtype="int64", nullable=False),
    ColumnDef(name="name", dtype="string"),
    ColumnDef(name="email", dtype="string"),
])

# Infer from DataFrame
df = pd.read_csv("data.csv")
schema = Schema.from_dataframe(df)

# Schema evolution
old_schema = Schema.from_dataframe(df_v1)
new_schema = Schema.from_dataframe(df_v2)
diff = Schema.diff(old_schema, new_schema)
print(diff.added_columns)    # ["phone"]
print(diff.removed_columns)  # ["fax"]
print(diff.type_changes)     # {"age": {"old": "int32", "new": "int64"}}

# DDL generation
ddl = schema.generate_ddl(table_name="users", dialect=SQLDialect.POSTGRESQL)
# CREATE TABLE users (id BIGINT NOT NULL, name TEXT, email TEXT);
```

### Nested Types

Support for structs, arrays, and maps:

```python
from simpleetl.core.schema import (
    StructType, ArrayType, MapType, FieldDef, ColumnDef
)

# Define nested columns
address = StructType(fields=[
    FieldDef("street", "string"),
    FieldDef("city", "string"),
    FieldDef("zip", "string"),
])

schema = Schema(columns=[
    ColumnDef(name="id", dtype="int64"),
    ColumnDef(name="address", dtype="struct<street:string,city:string,zip:string>",
              struct_type=address),
    ColumnDef(name="tags", dtype="array<string>",
              array_type=ArrayType("string")),
    ColumnDef(name="metadata", dtype="map<string,string>",
              map_type=MapType("string", "string")),
])

# Auto-infer nested types from DataFrame
df = pd.DataFrame({
    "id": [1, 2],
    "address": [{"street": "123 Main St", "city": "NYC", "zip": "10001"},
                {"street": "456 Oak Ave", "city": "LA", "zip": "90001"}],
    "tags": [["admin", "user"], ["user"]],
})
schema = Schema.from_dataframe(df)

# Dialect-aware DDL generation
schema.generate_ddl("users", dialect=SQLDialect.POSTGRESQL)
# CREATE TABLE users (id BIGINT, address JSONB, tags TEXT[], metadata JSONB);
schema.generate_ddl("users", dialect=SQLDialect.MYSQL)
# CREATE TABLE users (id BIGINT, address JSON, tags JSON, metadata JSON);
```

### Data Lineage & Observability

Track the full data flow from source to destination:

```python
from simpleetl.core.lineage import LineageTracker, get_lineage_tracker

# Automatic via LineageHook (added by default)
tracker = get_lineage_tracker()

# Query lineage
events = tracker.get_events(job_name="my_job")
lineage = tracker.get_lineage("my_job")
print(lineage["phases"])               # ["post_extract", "post_transform", "post_load"]
print(lineage["total_rows_processed"]) # 150000
print(lineage["total_duration_seconds"]) # 45.2

# Summary
summary = tracker.summary()
# {"total_events": 3, "total_rows_processed": 150000, "jobs": ["my_job"]}

# Persist to file
tracker.to_file("/var/lineage/events.jsonl")

# Or use file-backed store
from simpleetl.core.lineage import FileLineageStore
store = FileLineageStore("/var/lineage/events.jsonl")
```

### Per-Record Provenance

Trace individual record transformations:

```python
from simpleetl.core.lineage import ProvenanceHook

# Add provenance hook to job
hook = ProvenanceHook(record_id_column="user_id")
job.add_hook(hook)
job.run()

# Query provenance for a specific record
history = job.lineage_tracker.get_provenance("user_42")
# ["post_extract", "post_transform:filter_age", "post_load"]

# Get all provenance data
all_prov = job.lineage_tracker.get_all_provenance()
```

### Alerting

Rule-based alerting with pluggable channels:

```python
from simpleetl.core.lineage import (
    AlertRule, AlertManager,
    WebhookChannel, SlackChannel, EmailChannel
)

# Configure channels
webhook = WebhookChannel(url="https://hooks.example.com/alerts")
slack = SlackChannel(webhook_url="https://hooks.slack.com/services/...")
email = EmailChannel(recipients=["team@example.com"])

# Define rules
rules = [
    AlertRule(
        name="low_row_count",
        condition=lambda ctx: ctx.get("row_count", 0) < 1000,
        severity="critical",
        message_template="Row count dropped to {row_count}",
        channel_instances=[webhook, slack],
    ),
    AlertRule(
        name="slow_job",
        condition=lambda ctx: ctx.get("duration_seconds", 0) > 3600,
        severity="warning",
        message_template="Job took {duration_seconds}s (limit: 3600s)",
        channel_instances=[email],
    ),
]

# Check alerts after job execution
manager = AlertManager()
for rule in rules:
    manager.add_rule(rule)

results = manager.check_and_dispatch({
    "row_count": 500,
    "duration_seconds": 4000,
    "job_name": "my_job",
})
```

### OpenLineage Integration

Export lineage to OpenLineage-compatible systems (Marquez, DataHub, OpenMetadata):

```yaml
# config.yaml
openlineage_url: "http://marquez:5000/api/v1/lineage"
openlineage_namespace: "production"
```

```python
from simpleetl.core.lineage import configure_openlineage, get_lineage_tracker

# Configure OpenLineage
configure_openlineage(
    url="http://marquez:5000/api/v1/lineage",
    namespace="production",
)

# Run job (lineage events are recorded automatically)
job.run()

# Emit to OpenLineage endpoint
tracker = get_lineage_tracker()
emitted = tracker.emit_openlineage(config.openlineage_url)
print(f"Emitted {emitted}/{len(tracker.get_events())} events")
```

Supports the full OpenLineage RunEvent schema including inputs, outputs, schema facets, and custom facets.

### Data Quality

```python
from simpleetl.core.quality import DataQualityReport

report = DataQualityReport(raise_on_failure=False)
report.validate_schema(df, required_columns=["id", "name", "email"])
report.check_nulls(df, columns=["id", "name"], threshold=0.01)
report.check_duplicates(df, columns=["id"])
report.check_value_range(df, column="age", min_value=0, max_value=150)

print(report.summary())
report.raise_on_failures()  # Raises if any checks failed
```

### Security

```python
from simpleetl.core.secrets import AWSSecretsProvider, AzureKeyVaultProvider
from simpleetl.core.security import AuditLogger, RBACPolicy, Role

# Secrets management
provider = AWSSecretsProvider(region_name="us-east-1")
config = load_config("job.yaml", secrets_provider=provider)

# Audit logging
audit = AuditLogger()
audit.log_access("read", "s3://bucket/data.csv", user="etl_user")
audit.log_change("transform", "applied filter: age > 18", user="etl_user")

# RBAC
admin = Role("admin", permissions=["read", "write", "execute", "configure"])
analyst = Role("analyst", permissions=["read"])

policy = RBACPolicy()
policy.add_role("alice", admin)
policy.add_role("bob", analyst)
policy.can("alice", "write")   # True
policy.can("bob", "write")     # False
```

---

## Platform Support

| Platform | Status | Notes |
|----------|--------|-------|
| **Local** | ✅ | Runs with pandas on any OS |
| **AWS Glue** | ✅ | boto3 integration, S3, Glue Catalog, DynamicFrame |
| **Databricks** | ✅ | Spark session, DBFS, Delta Lake |
| **Azure Synapse** | ✅ | Synapse Spark, ABFS, Synapse SDK |
| **Docker** | ✅ | Dockerfile + docker-compose.yml included |
| **Kubernetes** | ✅ | Manifests in `k8s/` |

### Running on AWS Glue

```python
from simpleetl.platforms.glue import GluePlatformRunner
from awsglue.context import GlueContext

runner = GluePlatformRunner(glue_context=GlueContext)
runner.run_job(job)
```

### Running on Databricks

```python
from simpleetl.platforms.databricks import DatabricksPlatformRunner

runner = DatabricksPlatformRunner(spark=spark)
runner.run_job(job)
```

---

## Performance Benchmarks

All benchmarks run on Python 3.12 / macOS / M-series.

### Read/Write (100K rows)

| Format | Write + Read |
|--------|-------------|
| CSV | ~85ms |
| JSON | ~85ms |
| Parquet | ~85ms |

### Transformations (1M rows)

| Operation | Time |
|-----------|------|
| filter_data | ~15ms |
| map_values (callable) | ~81ms |
| union_data | ~1.4ms |
| window_functions | Partition-based (scales independently) |

### Streaming Memory (500K rows)

| Format | Memory (Chunked vs Full) |
|--------|-------------------------|
| JSON | 29MB vs 649MB (95% reduction) |
| CSV | 260MB vs 649MB (60% reduction) |
| Parquet | 2MB (dominant at all sizes) |

### DAG Operations (100 nodes)

| Operation | Time |
|-----------|------|
| Topological sort | ~18μs |
| Parallel group detection | ~50μs |
| validate() | ~30μs |

Run benchmarks yourself:

```bash
uv run python -m benchmarks.benchmark_read_write
uv run python -m benchmarks.benchmark_transformations
uv run python -m benchmarks.benchmark_streaming
uv run python -m benchmarks.benchmark_dag
```

---

## Documentation

| Document | Description |
|----------|-------------|
| [Getting Started](docs/getting-started.md) | Installation, first job, configuration reference |
| [API Reference](docs/api-reference.md) | Complete API documentation for all modules |
| [Platforms](docs/platforms.md) | Deployment guides for all supported platforms |
| [Development](docs/development.md) | Developer guide: structure, testing, contributing |
| [Performance](docs/performance.md) | Detailed benchmark results and analysis |
| [Security](docs/security.md) | Security best practices and audit guide |
| [OpenLineage](docs/openlineage.md) | OpenLineage integration guide |
| [Provenance](docs/provenance.md) | Per-record provenance tracking guide |
| [Alerting](docs/alerting.md) | Alerting integration guide |
| [Schema](docs/schema.md) | Nested type support guide |

---

## Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage report
uv run pytest --cov=simpleetl --cov-report=term-missing

# Run specific test module
uv run pytest tests/test_lineage.py -v

# Run benchmarks
uv run python -m pytest tests/ -q  # 1546 tests, 0 failures
```

---

## Project Structure

```
simpleetl/
├── src/simpleetl/
│   ├── __init__.py              # Public API (SimpleETL, read, write)
│   ├── __main__.py              # python -m simpleetl entry point
│   ├── cli.py                   # CLI (argparse)
│   ├── transformations.py       # Reusable transformation functions
│   ├── core/
│   │   ├── __init__.py
│   │   ├── job.py               # ETLJob base class
│   │   ├── config.py            # ETLJobConfig, load_config, env var resolution
│   │   ├── hooks.py             # Hook base, HookContext, phase constants
│   │   ├── dag.py               # DAG orchestration
│   │   ├── schema.py            # Schema, ColumnDef, nested types, DDL
│   │   ├── schema_registry.py   # Schema registry interface
│   │   ├── lineage.py           # LineageTracker, OpenLineage, provenance, alerting
│   │   ├── connection.py        # Database connection pooling
│   │   ├── secrets.py           # AWS, Azure, Vault, env secrets providers
│   │   ├── security.py          # AuditLogger, RBAC, PII masking, encryption
│   │   ├── checkpoint.py        # Checkpoint/resume support
│   │   ├── incremental.py       # Watermark-based incremental loading
│   │   ├── dlq.py               # Dead letter queue
│   │   ├── quality.py           # Data quality checks
│   │   ├── metrics.py           # Prometheus metrics
│   │   ├── health.py            # Health/readiness endpoints
│   │   ├── logger.py            # Structured logging
│   │   ├── parallel.py          # Multi-process/thread processing
│   │   ├── plugins.py           # Plugin discovery via entry_points
│   │   ├── schedule.py          # Cron scheduling
│   │   └── filesystem.py        # Unified filesystem abstraction
│   ├── formats/
│   │   ├── __init__.py
│   │   ├── base.py              # BaseReader, BaseWriter
│   │   ├── factory.py           # FormatFactory with auto-detection
│   │   ├── csv.py               # CSV reader/writer
│   │   ├── json.py              # JSON reader/writer
│   │   ├── parquet.py           # Parquet reader/writer
│   │   ├── avro.py              # Avro reader/writer (fastavro + pyarrow)
│   │   ├── orc.py               # ORC reader/writer
│   │   ├── xml.py               # XML reader/writer
│   │   ├── excel.py             # Excel reader/writer
│   │   ├── database.py          # Database reader/writer (SQLAlchemy)
│   │   └── glue_catalog.py      # AWS Glue Data Catalog reader
│   └── platforms/
│       ├── __init__.py
│       ├── base.py              # Platform abstract base
│       ├── detector.py          # Auto-detect runtime platform
│       ├── local.py             # Local platform runner
│       ├── glue.py              # AWS Glue platform runner
│       ├── databricks.py        # Databricks platform runner
│       └── synapse.py           # Azure Synapse platform runner
├── tests/                       # 1540 tests across 41 modules
├── benchmarks/                  # Performance benchmark suite
├── docs/                        # Documentation (10 guides)
├── examples/                    # Example ETL jobs and configurations
├── configs/                     # Example environment configurations
├── k8s/                         # Kubernetes manifests
├── Dockerfile
├── docker-compose.yml
├── pyproject.toml
└── README.md
```

---

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome!

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Run tests: `uv run pytest -q`
4. Run linting: `uv run ruff check src/ tests/`
5. Run type checking: `uv run mypy src/`
6. Commit your changes (`git commit -m 'Add some amazing feature'`)
7. Push to the branch (`git push origin feature/amazing-feature`)
8. Open a Pull Request
