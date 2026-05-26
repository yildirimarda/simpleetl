# Development Guide

This guide covers the project structure, testing practices, and contribution guidelines for SimpleETL.

## Project Structure

```
simpleetl/
  src/simpleetl/              # Main source code
    __init__.py               # Package init, exports __version__
    __main__.py               # Entry point for `python -m simpleetl`
    cli.py                    # Command-line interface
    core/                     # Core framework components
      __init__.py             # Exports: ETLJobConfig, ETLJob, MetricsCollector, etc.
      config.py               # ETLJobConfig model, load_config(), save_config()
      job.py                  # ETLJob abstract base class
      logger.py               # StructuredLogger, JSONFormatter, get_logger()
      metrics.py              # MetricsCollector, TimerContext, get_metrics()
      health.py               # HealthServer, HealthHandler, start_health_server()
      quality.py              # Data quality functions and DataQualityReport
    formats/                  # Data format readers and writers
      __init__.py             # Exports all readers, writers, and FormatFactory
      base.py                 # DataReader and DataWriter abstract base classes
      csv.py                  # CSVReader, CSVWriter
      json.py                 # JSONReader, JSONWriter
      parquet.py              # ParquetReader, ParquetWriter
      avro.py                 # AvroReader, AvroWriter
      orc.py                  # OrcReader, OrcWriter
      xml.py                  # XMLReader, XMLWriter
      excel.py                # ExcelReader, ExcelWriter
      database.py             # DatabaseReader, DatabaseWriter
      factory.py              # FormatFactory for auto-detection
    platforms/                # Platform-specific runners
      __init__.py             # Exports all runners and detection functions
      base.py                 # PlatformRunner abstract base class
      local.py                # LocalPlatformRunner
      glue.py                 # GluePlatformRunner
      databricks.py           # DatabricksPlatformRunner
      synapse.py              # SynapsePlatformRunner
      detector.py             # Platform detection utilities
    transformations.py        # Reusable transformation functions
  tests/                      # Unit and integration tests
    test_cli.py               # CLI tests
    test_config.py            # Configuration tests
    test_formats.py           # Format reader/writer tests
    test_health.py            # Health server tests
    test_job.py               # ETLJob tests
    test_logger.py            # Logger tests
    test_main.py              # __main__.py tests
    test_metrics.py           # Metrics tests
    test_platform_detector.py # Platform detection tests
    test_platforms.py         # Platform runner tests
    test_quality.py           # Data quality tests
    test_transformations.py   # Transformation function tests
  examples/                   # Example ETL jobs and data
    example_job.py            # Full example with metrics and logging
    jobs/example_csv_job.py   # Simple CSV-to-CSV job
    configs/example_job.yaml  # Example job configuration
    sample_job_config.yaml    # Sample job configuration
    sample_customers.csv      # Sample input data
    input/                    # Additional sample input data
    output/                   # Generated output data
  configs/                    # Environment configurations
    dev.yaml                  # Development environment
    staging.yaml              # Staging environment
    prod.yaml                 # Production environment
    logging.yaml              # Logging configuration
  k8s/                        # Kubernetes manifests
    namespace.yaml
    configmap.yaml
    service-account.yaml
    deployment.yaml
    service.yaml
    kustomization.yaml
  docs/                       # Documentation (this directory)
  Dockerfile                  # Docker build configuration
  docker-compose.yml          # Docker Compose configuration
  pyproject.toml              # Project metadata and dependencies
  main.py                     # Main entry point script
```

## Technology Stack

| Component | Technology |
|---|---|
| Data Processing | pandas, pyarrow |
| Configuration | Pydantic, PyYAML |
| Metrics | prometheus-client |
| Logging | Python logging (JSON format) |
| Database | SQLAlchemy |
| Serialization | fastavro, xmltodict, openpyxl |
| Testing | pytest, pytest-cov |
| Linting | ruff |
| Formatting | black |
| Package Management | uv |

## Setting Up the Development Environment

### Prerequisites

- Python 3.9+
- [uv](https://docs.astral.sh/uv/) package manager

### Setup

```bash
# Clone the repository
git clone <repository-url>
cd simpleetl

# Install all dependencies including dev
uv sync --group dev

# Verify setup
uv run python -m simpleetl --version
```

## Testing

### Running Tests

```bash
# Run all tests
uv run pytest tests/ -v

# Run with coverage report
uv run pytest tests/ -v --cov=src/simpleetl --cov-report=term-missing

# Run a specific test file
uv run pytest tests/test_quality.py -v

# Run a specific test
uv run pytest tests/test_quality.py::test_validate_schema -v
```

### Test Coverage

The project targets 95%+ test coverage. Check coverage with:

```bash
uv run pytest tests/ --cov=src/simpleetl --cov-report=term-missing --cov-fail-under=95
```

### Test Structure

Each module in `src/simpleetl/` has a corresponding test file in `tests/`:

| Source Module | Test File |
|---|---|
| `core/config.py` | `test_config.py` |
| `core/job.py` | `test_job.py` |
| `core/logger.py` | `test_logger.py` |
| `core/metrics.py` | `test_metrics.py` |
| `core/health.py` | `test_health.py` |
| `core/quality.py` | `test_quality.py` |
| `formats/` | `test_formats.py` |
| `platforms/` | `test_platforms.py`, `test_platform_detector.py` |
| `transformations.py` | `test_transformations.py` |
| `cli.py` | `test_cli.py` |
| `__main__.py` | `test_main.py` |

### Writing Tests

Follow these conventions:

- Use pytest fixtures for shared test data
- Test both success and failure paths
- Use descriptive test function names: `test_<function>_<scenario>`
- Aim for isolated, independent tests

```python
import pytest
import pandas as pd
from simpleetl.transformations import filter_data

def test_filter_data_by_column_range():
    df = pd.DataFrame({"age": [15, 25, 35, 45]})
    result = filter_data(df, column="age", min_value=20, max_value=40)
    assert len(result) == 2
    assert list(result["age"]) == [25, 35]

def test_filter_data_missing_column_raises():
    df = pd.DataFrame({"name": ["Alice", "Bob"]})
    with pytest.raises(ValueError, match="Column 'age' not found"):
        filter_data(df, column="age", min_value=18)
```

## Code Style

### Linting

```bash
# Run ruff linter
uv run ruff check src/ tests/

# Auto-fix issues
uv run ruff check --fix src/ tests/
```

### Formatting

```bash
# Format with black
uv run black src/ tests/

# Sort imports with isort
uv run isort src/ tests/
```

### Type Checking

```bash
uv run mypy src/simpleetl/
```

### Style Guidelines

- Follow PEP 8
- Use type hints for all function signatures
- Write docstrings for all public classes and methods (Google style)
- Keep lines to a maximum of 88 characters
- All code comments and documentation in English

## Contributing

### Workflow

1. **Create a branch:**

   ```bash
   git checkout -b feature/my-new-feature
   # or
   git checkout -b fix/my-bug-fix
   ```

2. **Make changes** following the code style guidelines.

3. **Write tests** for all new functionality.

4. **Run tests and linting:**

   ```bash
   uv run pytest tests/ -v --cov=src/simpleetl --cov-fail-under=95
   uv run ruff check src/ tests/
   uv run black --check src/ tests/
   ```

5. **Commit with a clear message:**

   ```bash
   git commit -m "Add support for Delta Lake format"
   ```

   Focus on the "why" in commit messages, not just the "what".

6. **Create a pull request** with:
   - Description of changes
   - Tests covering the changes
   - Updated documentation if needed

7. **All CI checks must pass** before merging.

### Adding a New Data Format

1. Create a new file in `src/simpleetl/formats/` (e.g., `delta.py`)
2. Implement `DataReader` and `DataWriter` subclasses
3. Register the format in `FormatFactory.FORMAT_MAP`
4. Export the new classes in `formats/__init__.py`
5. Add tests in `tests/test_formats.py`
6. Update documentation

### Adding a New Platform

1. Create a new file in `src/simpleetl/platforms/` (e.g., `spark.py`)
2. Implement a `PlatformRunner` subclass
3. Add a detection function in `detector.py`
4. Export in `platforms/__init__.py`
5. Add tests in `tests/test_platforms.py`
6. Update documentation

## Release Process

1. Update the version in `pyproject.toml` and `src/simpleetl/__init__.py`
2. Update the changelog
3. Create a git tag: `git tag v0.2.0`
4. Push the tag: `git push origin v0.2.0`
5. Build and publish: `uv build && uv publish`

## CI/CD

The project uses GitHub Actions for continuous integration. The pipeline:

1. Runs on every push and pull request
2. Installs dependencies with uv
3. Runs linting (ruff, black)
4. Runs type checking (mypy)
5. Runs tests with coverage (pytest, pytest-cov)
6. Fails if coverage is below 95%

## Getting Help

- Check the [API Reference](api-reference.md) for detailed API documentation
- Review the [examples](../examples/) directory for working examples
- Open an issue on the repository for bugs or feature requests
