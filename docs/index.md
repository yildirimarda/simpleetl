# SimpleETL Documentation

**Version 0.2.0**

A lightweight, professional ETL (Extract, Transform, Load) framework for Python. SimpleETL supports local execution, AWS Glue, Databricks, and Azure Synapse platforms with multiple data formats including CSV, JSON, Parquet, Avro, ORC, XML, Excel, and databases.

## Features

- **Multiple Data Formats** -- Read and write CSV, JSON, Parquet, Avro, ORC, XML, Excel, and SQL databases
- **Multi-Platform** -- Run jobs locally or on AWS Glue, Databricks, and Azure Synapse
- **Data Quality** -- Built-in schema validation, null checks, duplicate detection, value range checks, and data profiling
- **Declarative Quality Rules** -- Config-driven expectations (`not_null`, `unique`, ranges, regex, expressions) enforced automatically
- **Schema Drift Detection** -- Automatic schema comparison between runs with fail/warn/evolve policies
- **Lakehouse Formats** -- Delta Lake and Apache Iceberg without Spark
- **OpenTelemetry Tracing** -- Per-phase spans for distributed tracing backends
- **Polars Interop** -- Zero-copy bridges and Polars-accelerated CSV/Parquet IO
- **Kafka** -- Topic consumption into DataFrames; rows produced as JSON messages
- **Cloud Warehouses** -- Snowflake and BigQuery with native MERGE upserts
- **Metrics & Monitoring** -- Prometheus-compatible metrics with HTTP health/readiness endpoints
- **Structured Logging** -- JSON-formatted structured logging with job lifecycle events
- **Configuration-Driven** -- YAML or JSON configuration files with Pydantic validation
- **CLI** -- Command-line interface for job execution, format listing, and platform detection
- **Retry Logic** -- Configurable retry with exponential backoff
- **Docker & Kubernetes** -- Ready-to-use Docker and Kubernetes deployment manifests

## Quick Links

| Document | Description |
|---|---|
| [Getting Started](getting-started.md) | Installation, first ETL job, configuration reference |
| [API Reference](api-reference.md) | Complete API documentation for all modules |
| [Platforms](platforms.md) | Deployment guides for all supported platforms |
| [Development](development.md) | Developer guide: project structure, testing, contributing |
| [Performance](performance.md) | Benchmark results and analysis |
| [Security](security.md) | Security best practices and audit guide |
| [OpenLineage](openlineage.md) | OpenLineage integration guide |
| [Provenance](provenance.md) | Per-record provenance tracking guide |
| [Alerting](alerting.md) | Alerting integration guide |
| [Schema](schema.md) | Nested type support guide |
| [Quality Rules](quality_rules.md) | Declarative data quality rules guide |
| [Schema Drift](schema_drift.md) | Schema drift detection guide |
| [Iceberg](iceberg.md) | Apache Iceberg format guide |
| [Tracing](tracing.md) | OpenTelemetry tracing guide |
| [Polars](polars.md) | Polars interop and IO acceleration guide |
| [Kafka](kafka.md) | Kafka source/sink guide |
| [Warehouses](warehouses.md) | Snowflake & BigQuery dialect guide |

## Quick Example

```python
from simpleetl.core.job import ETLJob
from simpleetl.formats.csv import CSVReader, CSVWriter

class MyETLJob(ETLJob):
    def __init__(self, config):
        super().__init__(config)
        self.reader = CSVReader()
        self.writer = CSVWriter()

    def extract(self):
        return self.reader.read(self.config.params["input_path"])

    def transform(self, data):
        return data[data["age"] >= 18]

    def load(self, data):
        self.writer.write(data, self.config.params["output_path"])

    def run(self):
        data = self.extract()
        data = self.transform(data)
        self.load(data)

# Run the job
job = MyETLJob("config.yaml")
job.run_with_error_handling()
```

## Project Structure

```
simpleetl/
  src/simpleetl/          # Main source code
    core/                 # Core framework (job, config, metrics, health, quality, logger)
    formats/              # Data format readers and writers
    platforms/            # Platform-specific runners
    transformations.py    # Reusable transformation functions
    cli.py                # Command-line interface
  tests/                  # Unit and integration tests
  examples/               # Example ETL jobs and configurations
  configs/                # Example environment configurations
  docs/                   # Documentation (this directory)
  k8s/                    # Kubernetes manifests
```

## License

This project is licensed under the terms specified in the LICENSE file.
