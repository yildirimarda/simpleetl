# Getting Started

## Installation

### Prerequisites

- Python 3.9 or higher
- [uv](https://docs.astral.sh/uv/) package manager (recommended) or pip

### Install from Source

```bash
# Clone the repository
git clone <repository-url>
cd simpleetl

# Install with uv (recommended)
uv sync

# Or install with pip
pip install -e .
```

### Install with Dev Dependencies

```bash
uv sync --group dev
```

### Verify Installation

```bash
python -m simpleetl --version
# Output: simpleetl 1.0.0
```

## Your First ETL Job

### Step 1: Create a Configuration File

Create a YAML configuration file for your job:

```yaml
# my_job.yaml
name: my_first_job
description: "My first ETL job"
platform: local
input_format: csv
output_format: csv
log_level: INFO
max_retries: 3
retry_delay: 1.0
params:
  input_path: data/input.csv
  output_path: data/output.csv
  filter_column: age
  filter_min_value: 18
```

### Step 2: Create the Job Class

```python
# my_job.py
from simpleetl.core.job import ETLJob
from simpleetl.formats.csv import CSVReader, CSVWriter

class MyFirstJob(ETLJob):
    def __init__(self, config):
        super().__init__(config)
        self.reader = CSVReader()
        self.writer = CSVWriter()

    def extract(self):
        return self.reader.read(self.config.params["input_path"])

    def transform(self, data):
        col = self.config.params.get("filter_column", "age")
        min_val = self.config.params.get("filter_min_value", 0)
        return data[data[col] >= min_val]

    def load(self, data):
        self.writer.write(data, self.config.params["output_path"])

    def run(self):
        data = self.extract()
        data = self.transform(data)
        self.load(data)
```

### Step 3: Run the Job

**Option A: Run from Python**

```python
job = MyFirstJob("my_job.yaml")
job.run_with_error_handling()
```

**Option B: Run from CLI**

Add a `job_class` parameter to your config:

```yaml
params:
  job_class: my_job.MyFirstJob
  # ... other params
```

Then execute:

```bash
python -m simpleetl --config my_job.yaml
```

**Option C: Dry Run (validate config only)**

```bash
python -m simpleetl --config my_job.yaml --dry-run
```

## Using the Format Factory

Instead of importing specific readers/writers, use `FormatFactory` to auto-detect formats:

```python
from simpleetl.formats import FormatFactory

# Auto-detect based on file extension
reader = FormatFactory.get_reader("data/input.parquet")
writer = FormatFactory.get_writer("data/output.json")

data = reader.read("data/input.parquet")
writer.write(data, "data/output.json")
```

## Using Built-in Transformations

```python
from simpleetl.transformations import filter_data, map_values, aggregate_data

# Filter by column value
filtered = filter_data(df, column="age", min_value=18, max_value=65)

# Map values using a dictionary
mapped = map_values(df, "country", {"USA": "United States", "UK": "United Kingdom"})

# Map values using a function
mapped = map_values(df, "name", lambda x: x.upper())

# Aggregate data
result = aggregate_data(
    df,
    groupby=["country"],
    agg={"revenue": "sum", "customer_id": "count"}
)
```

## Adding Data Quality Checks

```python
from simpleetl.core.quality import DataQualityReport

report = DataQualityReport(raise_on_failure=False)

report.validate_schema(df, required_columns=["id", "name", "email"])
report.check_nulls(df, columns=["id", "name"], threshold=0.01)
report.check_duplicates(df, columns=["id"])
report.check_value_range(df, column="age", min_value=0, max_value=150)

# Check results
print(report.summary())

# Raise if any checks failed
report.raise_on_failures()
```

## Configuration Reference

### ETLJobConfig Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | `str` | required | Unique job name |
| `description` | `str \| None` | `None` | Human-readable description |
| `platform` | `str` | `"local"` | Target platform: `local`, `glue`, `databricks`, `synapse` |
| `input_format` | `str` | required | Input data format: `csv`, `json`, `parquet`, `avro`, `orc`, `xml`, `excel`, `database` |
| `output_format` | `str` | required | Output data format |
| `max_retries` | `int` | `0` | Maximum retry attempts on failure |
| `retry_delay` | `float` | `1.0` | Initial delay between retries in seconds (exponential backoff applied) |
| `log_level` | `str` | `"INFO"` | Logging level: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` |
| `params` | `Dict[str, Any]` | `{}` | Additional job-specific parameters |

### Environment Configurations

The `configs/` directory provides example environment configurations:

- `configs/dev.yaml` -- Development environment with debug logging and local SQLite
- `configs/staging.yaml` -- Staging environment with PostgreSQL and moderate retry settings
- `configs/prod.yaml` -- Production environment with full monitoring, alerting, and security
- `configs/logging.yaml` -- Logging configuration with JSON formatters and rotating file handlers

### CLI Reference

```bash
# Run a job
python -m simpleetl --config path/to/config.yaml

# Override platform
python -m simpleetl --config config.yaml --platform glue

# Dry run (validate config only)
python -m simpleetl --config config.yaml --dry-run

# List supported formats
python -m simpleetl --list-formats

# Detect current platform
python -m simpleetl --detect-platform

# Show version
python -m simpleetl --version
```

## Troubleshooting

### Common Installation Issues

**"No module named simpleetl"**
Make sure you installed the package in editable mode:
```bash
uv sync
# or
pip install -e .
```
If using `uv run`, prefix commands with `uv run` (e.g., `uv run python my_job.py`) so the virtual environment is activated.

**Python version errors**
SimpleETL requires Python 3.9+. Check your version:
```bash
python --version
```
Use `uv python` to install and manage Python versions.

---

### Configuration Issues

**`FileNotFoundError` when loading config**
Paths in config files are resolved relative to the *current working directory*, not the config file's location. Run your project from the repository root:
```bash
cd /path/to/simpleetl
uv run simpleetl --config my_job.yaml
```

**`EnvVarResolutionError: Environment variable 'X' is not set`**
The config uses `${VAR}` syntax for environment variables. Either set the variable or provide a default:
```yaml
password: ${DB_PASSWORD:-default_value}
```

**`ValidationError` on config load**
Run a dry-run validation to see exactly which fields are missing or invalid:
```bash
uv run simpleetl --config my_job.yaml --dry-run
```

---

### Job Execution Issues

**Job class not found (`ImportError` / `ModuleNotFoundError`)**
Ensure the module is importable. For custom jobs in the `examples/` directory:
```bash
# Run from the project root so Python can resolve imports
cd /path/to/simpleetl
uv run python -m examples.example_job
```
Check that `job_class` in your config or DAG YAML uses the fully qualified dotted path:
```yaml
params:
  job_class: examples.example_job.SampleETLJob
```

**DAG node fails with "No job_class or config_path; skipping execution"**
Every DAG node needs either a `job_class` (dotted Python path) or a `config_path` that includes a `job_class` in its params. Verify all nodes have at least one of these:
```yaml
jobs:
  - name: my_node
    job_class: "examples.example_job.SampleETLJob"
    config_path: "configs/my_node.yaml"
```

**S3 / GCS / ABFS paths fail at runtime**
Cloud storage support requires additional fsspec backends:
```bash
uv add s3fs       # Amazon S3
uv add gcsfs      # Google Cloud Storage
uv add adlfs      # Azure Blob / ADLS Gen2
```
Authentication uses the standard SDK mechanisms (environment variables,
IAM roles, service account keys). See `examples/cloud_storage_job.py` for
details.

**`ModuleNotFoundError: No module named 'prometheus_client'`**
The metrics module depends on `prometheus_client`. Install it explicitly:
```bash
uv add prometheus-client
```

---

### Debugging Tips

1. **Enable debug logging** in your config:
   ```yaml
   log_level: DEBUG
   ```

2. **Validate a DAG's execution plan** without running it:
   ```python
   from simpleetl.core.dag import DAG
   dag = DAG.from_yaml("examples/dags/example_dag.yaml")
   plan = dag.get_execution_plan()
   print(plan)
   ```

3. **Use `--dry-run`** to validate configs without executing jobs:
   ```bash
   uv run simpleetl --config my_job.yaml --dry-run
   ```

4. **Check the platform detection** if jobs behave differently on cloud:
   ```bash
   uv run simpleetl --detect-platform
   ```

---

## Next Topics

- [API Reference](api-reference.md) -- Complete API documentation
- [Platforms](platforms.md) -- Deploy to AWS Glue, Databricks, Azure Synapse, Docker, Kubernetes
- [Development](development.md) -- Testing, contributing, project structure
