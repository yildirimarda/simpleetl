# SimpleETL Documentation

**Version 1.0.0**

A lightweight, professional ETL (Extract, Transform, Load) framework for Python. SimpleETL supports local execution, AWS Glue, Databricks, and Azure Synapse platforms with multiple data formats including CSV, JSON, Parquet, Avro, ORC, XML, Excel, and databases.

## Features

- **Multiple Data Formats** -- Read and write CSV, JSON, Parquet, Avro, ORC, XML, Excel, and SQL databases
- **Multi-Platform** -- Run jobs locally or on AWS Glue, Databricks, and Azure Synapse
- **Data Quality** -- Built-in schema validation, null checks, duplicate detection, value range checks, and data profiling
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
