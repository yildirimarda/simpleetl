# API Reference

Complete API documentation for SimpleETL v1.0.0.

---

## Core Module

### `ETLJobConfig`

```python
from simpleetl.core.config import ETLJobConfig
```

Pydantic model for ETL job configuration.

**Fields:**

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | `str` | required | Unique job name |
| `description` | `str \| None` | `None` | Human-readable description |
| `platform` | `str` | `"local"` | Target platform |
| `input_format` | `str` | required | Input data format |
| `output_format` | `str` | required | Output data format |
| `max_retries` | `int` | `0` | Maximum retry attempts |
| `retry_delay` | `float` | `1.0` | Initial retry delay in seconds |
| `log_level` | `str` | `"INFO"` | Logging level |
| `params` | `Dict[str, Any]` | `{}` | Additional parameters |

**Example:**

```python
config = ETLJobConfig(
    name="my_job",
    platform="local",
    input_format="csv",
    output_format="parquet",
    max_retries=3,
    retry_delay=1.0,
    log_level="INFO",
    params={"input_path": "data.csv"}
)
```

---

### `load_config`

```python
from simpleetl.core.config import load_config

config = load_config("path/to/config.yaml")
```

Load and validate ETL job configuration from a YAML or JSON file.

**Args:**
- `config_path` (`str | Path`): Path to the configuration file.

**Returns:** `ETLJobConfig` instance.

**Raises:**
- `FileNotFoundError` -- If the config file does not exist.
- `ValidationError` -- If the configuration is invalid.
- `ValueError` -- If the file format is not supported.

---

### `save_config`

```python
from simpleetl.core.config import save_config

save_config(config, "path/to/output.yaml")
```

Save ETL job configuration to a YAML or JSON file.

**Args:**
- `config` (`ETLJobConfig`): Configuration to save.
- `config_path` (`str | Path`): Output file path.

---

### `ETLJob`

```python
from simpleetl.core.job import ETLJob
```

Abstract base class for all ETL jobs.

**Constructor:**

```python
def __init__(self, config: ETLJobConfig | str | Dict[str, Any]):
```

Accepts an `ETLJobConfig` instance, a path to a config file, or a dictionary.

**Methods:**

#### `run()` (abstract)

```python
@abstractmethod
def run(self) -> None:
```

Execute the ETL job. Must be implemented by subclasses.

#### `extract()`

```python
def extract(self) -> Any:
```

Extract data from the source. Override in subclasses. Default returns `None`.

#### `transform()`

```python
def transform(self, data: Any) -> Any:
```

Transform the extracted data. Override in subclasses. Default returns data unchanged.

#### `load()`

```python
def load(self, data: Any) -> None:
```

Load the transformed data to the destination. Override in subclasses.

#### `run_with_error_handling()`

```python
def run_with_error_handling(self) -> None:
```

Run the ETL job with error handling, logging, and retry with exponential backoff. Uses `max_retries` and `retry_delay` from the configuration.

**Example:**

```python
class MyJob(ETLJob):
    def __init__(self, config):
        super().__init__(config)

    def run(self):
        data = self.extract()
        data = self.transform(data)
        self.load(data)

job = MyJob("config.yaml")
job.run_with_error_handling()
```

---

## Formats Module

### Base Classes

#### `DataReader`

```python
from simpleetl.formats.base import DataReader
```

Abstract base class for data readers.

```python
@abstractmethod
def read(self, source: Any, **kwargs) -> pd.DataFrame:
```

#### `DataWriter`

```python
from simpleetl.formats.base import DataWriter
```

Abstract base class for data writers.

```python
@abstractmethod
def write(self, data: pd.DataFrame, destination: Any, **kwargs) -> None:
```

---

### CSV

```python
from simpleetl.formats.csv import CSVReader, CSVWriter
```

**CSVReader.read(source, \*\*kwargs)** -- Reads a CSV file into a DataFrame. Passes extra kwargs to `pandas.read_csv`.

**CSVWriter.write(data, destination, \*\*kwargs)** -- Writes a DataFrame to CSV. Defaults to `index=False`.

```python
reader = CSVReader()
df = reader.read("data/input.csv", sep=",")

writer = CSVWriter()
writer.write(df, "data/output.csv")
```

---

### JSON

```python
from simpleetl.formats.json import JSONReader, JSONWriter
```

**JSONReader.read(source, \*\*kwargs)** -- Reads JSON from a file path or JSON string.

**JSONWriter.write(data, destination, \*\*kwargs)** -- Writes DataFrame to JSON. Defaults to `orient='records'` and `lines=True`. Use `destination='-'` for stdout.

```python
reader = JSONReader()
df = reader.read("data/input.json")

writer = JSONWriter()
writer.write(df, "data/output.json")
```

---

### Parquet

```python
from simpleetl.formats.parquet import ParquetReader, ParquetWriter
```

**ParquetReader.read(source, \*\*kwargs)** -- Reads a Parquet file. Defaults to `engine='pyarrow'`.

**ParquetWriter.write(data, destination, \*\*kwargs)** -- Writes DataFrame to Parquet. Defaults to `engine='pyarrow'` and `compression='snappy'`.

```python
reader = ParquetReader()
df = reader.read("data/input.parquet")

writer = ParquetWriter()
writer.write(df, "data/output.parquet")
```

---

### Avro

```python
from simpleetl.formats.avro import AvroReader, AvroWriter
```

**AvroReader.read(source, \*\*kwargs)** -- Reads an Avro file using `fastavro`.

**AvroWriter.write(data, destination, \*\*kwargs)** -- Writes DataFrame to Avro. Supports a `schema` kwarg; infers schema from the DataFrame if not provided.

```python
reader = AvroReader()
df = reader.read("data/input.avro")

writer = AvroWriter()
writer.write(df, "data/output.avro")
```

---

### ORC

```python
from simpleetl.formats.orc import OrcReader, OrcWriter
```

**OrcReader.read(source, \*\*kwargs)** -- Reads an ORC file using `pyarrow.orc`.

**OrcWriter.write(data, destination, \*\*kwargs)** -- Writes DataFrame to ORC. Defaults to `compression='snappy'`.

```python
reader = OrcReader()
df = reader.read("data/input.orc")

writer = OrcWriter()
writer.write(df, "data/output.orc")
```

---

### XML

```python
from simpleetl.formats.xml import XMLReader, XMLWriter
```

**XMLReader.read(source, \*\*kwargs)** -- Reads XML from a file path or XML string. Supports `root_element` kwarg to specify which element to extract.

**XMLWriter.write(data, destination, \*\*kwargs)** -- Writes DataFrame to XML. Supports `root_element` (default: `'data'`) and `record_element` (default: `'record'`) kwargs.

```python
reader = XMLReader()
df = reader.read("data/input.xml", root_element="items")

writer = XMLWriter()
writer.write(df, "data/output.xml")
```

---

### Excel

```python
from simpleetl.formats.excel import ExcelReader, ExcelWriter
```

**ExcelReader.read(source, \*\*kwargs)** -- Reads an Excel file (.xlsx, .xls). Supports `sheet_name` kwarg (default: `0`).

**ExcelWriter.write(data, destination, \*\*kwargs)** -- Writes DataFrame to Excel. Supports `sheet_name` kwarg (default: `'Sheet1'`). Can accept a dict of DataFrames to write multiple sheets.

```python
reader = ExcelReader()
df = reader.read("data/input.xlsx", sheet_name="Sheet1")

writer = ExcelWriter()
writer.write(df, "data/output.xlsx")
```

---

### Database

```python
from simpleetl.formats.database import DatabaseReader, DatabaseWriter
```

**DatabaseReader.read(source, \*\*kwargs)** -- Reads from a database using SQLAlchemy. Accepts a connection string or engine. Requires `sql` kwarg when using an engine directly.

**DatabaseWriter.write(data, destination, \*\*kwargs)** -- Writes DataFrame to a database. Supports `table_name` (default: `'data'`), `if_exists` (default: `'fail'`), and `index` (default: `False`) kwargs.

```python
reader = DatabaseReader()
df = reader.read("postgresql://user:pass@localhost/db", sql="SELECT * FROM customers")

writer = DatabaseWriter()
writer.write(df, "postgresql://user:pass@localhost/db", table_name="results", if_exists="replace")
```

---

### FormatFactory

```python
from simpleetl.formats import FormatFactory
```

Factory class for auto-detecting and creating appropriate readers/writers.

#### `get_reader(source, **kwargs) -> DataReader`

Returns the appropriate reader for a source based on file extension or database connection string.

```python
reader = FormatFactory.get_reader("data.csv")        # Returns CSVReader
reader = FormatFactory.get_reader("data.parquet")    # Returns ParquetReader
reader = FormatFactory.get_reader("postgresql://...") # Returns DatabaseReader
```

#### `get_writer(destination, **kwargs) -> DataWriter`

Returns the appropriate writer for a destination.

```python
writer = FormatFactory.get_writer("output.json")      # Returns JSONWriter
writer = FormatFactory.get_writer("output.xlsx")      # Returns ExcelWriter
```

#### `detect_format(source) -> Dict[str, Any]`

Detects the format from a source path or connection string.

```python
info = FormatFactory.detect_format("data.parquet")
# Returns: {'format': 'parquet', 'extension': '.parquet', 'reader': ..., 'writer': ..., 'mime_type': 'application/octet-stream'}
```

#### `supported_formats() -> Dict[str, str]`

Returns a dictionary mapping format names to their extensions.

```python
formats = FormatFactory.supported_formats()
# Returns: {'csv': '.csv', 'json': '.json', 'parquet': '.parquet', ...}
```

---

## Platforms Module

### `PlatformRunner`

```python
from simpleetl.platforms.base import PlatformRunner
```

Abstract base class for platform runners.

```python
@abstractmethod
def run_job(self, job: ETLJob) -> None:
```

---

### `LocalPlatformRunner`

```python
from simpleetl.platforms.local import LocalPlatformRunner
```

Runs ETL jobs locally using pandas.

```python
runner = LocalPlatformRunner()
runner.run_job(job)  # Calls job.run_with_error_handling()
```

---

### `GluePlatformRunner`

```python
from simpleetl.platforms.glue import GluePlatformRunner
```

Runs ETL jobs on AWS Glue. Detects Glue environment via the `AWS_EXECUTION_ENV` environment variable. Falls back to local execution with a warning if not in a Glue environment.

```python
runner = GluePlatformRunner()
runner.run_job(job)
```

---

### `DatabricksPlatformRunner`

```python
from simpleetl.platforms.databricks import DatabricksPlatformRunner
```

Runs ETL jobs on Databricks. Detects Databricks environment via the `DATABRICKS_RUNTIME_VERSION` environment variable. Falls back to local execution with a warning if not in a Databricks environment.

```python
runner = DatabricksPlatformRunner()
runner.run_job(job)
```

---

### `SynapsePlatformRunner`

```python
from simpleetl.platforms.synapse import SynapsePlatformRunner
```

Runs ETL jobs on Azure Synapse. Detects Synapse environment via the `AZURE_SYNAPSE_SPARK_POOL_NAME` environment variable. Falls back to local execution with a warning if not in a Synapse environment.

```python
runner = SynapsePlatformRunner()
runner.run_job(job)
```

---

### Platform Detection Functions

```python
from simpleetl.platforms.detector import (
    is_aws_glue,
    is_databricks,
    is_azure_synapse,
    get_current_platform,
    get_platform_info,
)
```

| Function | Returns | Description |
|---|---|---|
| `is_aws_glue()` | `bool` | True if `AWS_EXECUTION_ENV` starts with `AWS_Glue` |
| `is_databricks()` | `bool` | True if `DATABRICKS_RUNTIME_VERSION` is set |
| `is_azure_synapse()` | `bool` | True if `AZURE_SYNAPSE_SPARK_POOL_NAME` is set |
| `get_current_platform()` | `str` | One of `'glue'`, `'databricks'`, `'synapse'`, `'local'` |
| `get_platform_info()` | `dict` | Detailed platform info including system and safe env vars |

---

## Transformations Module

```python
from simpleetl.transformations import filter_data, map_values, aggregate_data
```

### `filter_data`

```python
filter_data(
    df: pd.DataFrame,
    filter_func: Optional[Callable[[pd.Series], bool]] = None,
    column: Optional[str] = None,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
) -> pd.DataFrame
```

Filter a DataFrame based on conditions.

**Args:**
- `df` -- Input DataFrame
- `filter_func` -- Function that takes a row Series and returns boolean
- `column` -- Column name to filter on (used with min/max)
- `min_value` -- Minimum value (inclusive)
- `max_value` -- Maximum value (inclusive)

**Returns:** Filtered DataFrame.

```python
# Filter by column range
result = filter_data(df, column="age", min_value=18, max_value=65)

# Filter by custom function
result = filter_data(df, filter_func=lambda row: row["email"] != "N/A")
```

---

### `map_values`

```python
map_values(
    df: pd.DataFrame,
    column: str,
    mapping: Union[Dict[Any, Any], Callable[[Any], Any]],
) -> pd.DataFrame
```

Map values in a column using a dictionary or function.

**Args:**
- `df` -- Input DataFrame
- `column` -- Column name to map
- `mapping` -- Dict or callable for value mapping

**Returns:** DataFrame with mapped column.

```python
# Map with dictionary
result = map_values(df, "country", {"USA": "United States", "UK": "United Kingdom"})

# Map with function
result = map_values(df, "name", lambda x: x.upper())
```

---

### `aggregate_data`

```python
aggregate_data(
    df: pd.DataFrame,
    groupby: Union[str, List[str]],
    agg: Dict[str, Union[str, List[str], Callable]],
) -> pd.DataFrame
```

Aggregate data using groupby and aggregation specifications.

**Args:**
- `df` -- Input DataFrame
- `groupby` -- Column name(s) to group by
- `agg` -- Aggregation dict: keys are column names, values are agg specs (e.g., `'sum'`, `'mean'`, `['min', 'max']`, or a callable)

**Returns:** Aggregated DataFrame with flattened column names.

```python
result = aggregate_data(
    df,
    groupby=["country", "age_group"],
    agg={"customer_id": "count", "revenue": ["sum", "mean"]}
)
```

---

## Quality Module

```python
from simpleetl.core.quality import (
    DataQualityError,
    validate_schema,
    check_nulls,
    check_duplicates,
    check_value_range,
    check_unique_values,
    profile_data,
    CheckResult,
    DataQualityReport,
)
```

### `DataQualityError`

Exception raised when a data quality check fails.

**Attributes:**
- `check_name` (`str`): Name of the failed check.
- `details` (`Dict[str, Any]`): Additional details about the failure.

---

### `validate_schema`

```python
validate_schema(
    df: pd.DataFrame,
    required_columns: List[str],
    column_types: Optional[Dict[str, str]] = None,
) -> bool
```

Validate that a DataFrame has required columns and optional type checks.

**Raises:** `DataQualityError` if columns are missing or types do not match.

```python
validate_schema(df, required_columns=["id", "name"], column_types={"id": "int64"})
```

---

### `check_nulls`

```python
check_nulls(
    df: pd.DataFrame,
    columns: Optional[List[str]] = None,
    threshold: float = 0.0,
) -> Dict[str, float]
```

Check for null values. Returns a dictionary mapping column name to null fraction.

**Raises:** `DataQualityError` if null fraction exceeds threshold.

```python
null_fractions = check_nulls(df, columns=["id", "name"], threshold=0.01)
```

---

### `check_duplicates`

```python
check_duplicates(
    df: pd.DataFrame,
    columns: Optional[List[str]] = None,
    threshold: float = 0.0,
) -> int
```

Check for duplicate rows. Returns the number of duplicate rows found.

**Raises:** `DataQualityError` if duplicate fraction exceeds threshold.

```python
dup_count = check_duplicates(df, columns=["id"], threshold=0.0)
```

---

### `check_value_range`

```python
check_value_range(
    df: pd.DataFrame,
    column: str,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
) -> bool
```

Check that column values are within the specified range.

**Raises:** `DataQualityError` if values are outside range.

```python
check_value_range(df, column="age", min_value=0, max_value=150)
```

---

### `check_unique_values`

```python
check_unique_values(
    df: pd.DataFrame,
    column: str,
    expected_count: Optional[int] = None,
) -> int
```

Check the number of unique values in a column.

**Raises:** `DataQualityError` if `expected_count` does not match.

```python
count = check_unique_values(df, column="country", expected_count=50)
```

---

### `profile_data`

```python
profile_data(df: pd.DataFrame) -> Dict[str, Any]
```

Return basic profiling information about a DataFrame.

**Returns:** Dictionary with `row_count`, `column_count`, `null_counts`, `dtypes`, and `memory_usage_bytes`.

```python
profile = profile_data(df)
# {'row_count': 1000, 'column_count': 10, 'null_counts': {...}, 'dtypes': {...}, 'memory_usage_bytes': 80000}
```

---

### `DataQualityReport`

```python
report = DataQualityReport(raise_on_failure: bool = True)
```

Collects multiple quality check results.

**Methods:**

| Method | Description |
|---|---|
| `validate_schema(df, required_columns, column_types=None)` | Run schema validation |
| `check_nulls(df, columns=None, threshold=0.0)` | Check for null values |
| `check_duplicates(df, columns=None, threshold=0.0)` | Check for duplicates |
| `check_value_range(df, column, min_value=None, max_value=None)` | Check value range |
| `check_unique_values(df, column, expected_count=None)` | Check unique values |
| `add_check(name, passed, details=None, error=None)` | Add a manual check result |
| `raise_on_failures()` | Raise `DataQualityError` if any checks failed |
| `summary()` | Return a summary dict of all results |

**Properties:**

| Property | Description |
|---|---|
| `results` | List of all `CheckResult` objects |
| `passed` | `True` if all checks passed |
| `failed_checks` | List of failed `CheckResult` objects |

```python
report = DataQualityReport(raise_on_failure=False)
report.validate_schema(df, required_columns=["id", "name"])
report.check_nulls(df, threshold=0.01)
report.check_duplicates(df)

if not report.passed:
    print(report.summary())
    report.raise_on_failures()
```

---

## Metrics Module

```python
from simpleetl.core.metrics import MetricsCollector, get_metrics, job_timer
```

### `MetricsCollector`

```python
collector = MetricsCollector(registry=None)
```

Metrics collector with Prometheus compatibility.

**Default Metrics:**

| Metric | Type | Description |
|---|---|---|
| `etl_jobs_total` | Counter | Total ETL jobs executed |
| `etl_jobs_failed` | Counter | Total failed ETL jobs |
| `etl_active_jobs` | Gauge | Currently active ETL jobs |
| `etl_last_job_timestamp` | Gauge | Timestamp of last job execution |
| `etl_job_duration_seconds` | Histogram | Job execution duration |
| `etl_records_processed_total` | Histogram | Total records processed |
| `etl_read_duration_seconds` | Histogram | Data read duration |
| `etl_transform_duration_seconds` | Histogram | Transformation duration |
| `etl_write_duration_seconds` | Histogram | Data write duration |

**Methods:**

| Method | Description |
|---|---|
| `counter(name, description, labelnames=None)` | Get or create a Counter |
| `gauge(name, description, labelnames=None)` | Get or create a Gauge |
| `histogram(name, description, labelnames=None, buckets=None)` | Get or create a Histogram |
| `inc_counter(name, value=1.0, labels=None)` | Increment a counter |
| `set_gauge(name, value, labels=None)` | Set a gauge value |
| `observe_histogram(name, value, labels=None)` | Observe a histogram value |
| `time_function(name, labels=None)` | Decorator to time a function |
| `context_timer(name, labels=None)` | Context manager for timing |
| `get_metrics(output_format='text')` | Get metrics as text (Prometheus) or JSON |
| `export_to_file(filepath, format='text')` | Export metrics to a file |

**Example:**

```python
metrics = get_metrics()

# Increment counters
metrics.inc_counter('etl_jobs_total')
metrics.inc_counter('etl_records_extracted', 1000)

# Set gauges
metrics.set_gauge('etl_active_jobs', 1)

# Time with context manager
with metrics.context_timer('etl_job_duration_seconds'):
    run_etl_job()

# Time with decorator
@metrics.time_function('etl_job_duration_seconds')
def my_job():
    pass
```

---

### `job_timer`

```python
from simpleetl.core.metrics import job_timer

@job_timer()
def my_function():
    pass
```

Convenience decorator that times function execution using the `etl_job_duration_seconds` histogram.

---

### `TimerContext`

```python
from simpleetl.core.metrics import get_metrics

with get_metrics().context_timer('etl_job_duration_seconds'):
    # Code to time
    pass
```

Context manager for timing operations. Automatically records duration to the specified histogram on exit.

---

## Health Module

```python
from simpleetl.core.health import HealthServer, start_health_server
```

### `HealthServer`

```python
server = HealthServer(port=8000)
```

HTTP server for health checks and metrics.

**Endpoints:**

| Endpoint | Description |
|---|---|
| `GET /health` | Liveness probe -- returns `{"status":"ok"}` |
| `GET /ready` | Readiness probe -- returns `{"status":"ready"}` |
| `GET /metrics` | Prometheus metrics endpoint |

**Methods:**

| Method | Description |
|---|---|
| `start()` | Start the server in a background thread |
| `stop()` | Stop the server |

```python
server = HealthServer(port=8000)
server.start()
# Server runs in background thread
server.stop()
```

---

### `start_health_server`

```python
server = start_health_server(port=8000)
```

Start the global health server. Returns a `HealthServer` instance.

---

## Logger Module

```python
from simpleetl.core.logger import StructuredLogger, get_logger
```

### `StructuredLogger`

```python
logger = StructuredLogger(name='simpleetl', level='INFO')
```

Structured logger with JSON formatting.

**Methods:**

| Method | Description |
|---|---|
| `debug(message, **kwargs)` | Log debug message |
| `info(message, **kwargs)` | Log info message |
| `warning(message, **kwargs)` | Log warning message |
| `error(message, **kwargs)` | Log error message |
| `critical(message, **kwargs)` | Log critical message |
| `log_job_start(job_name, job_id, **kwargs)` | Log job start event |
| `log_job_complete(job_name, job_id, duration, **kwargs)` | Log job completion event |
| `log_job_error(job_name, job_id, error, **kwargs)` | Log job error event |
| `log_data_read(source, record_count, **kwargs)` | Log data read event |
| `log_data_write(destination, record_count, **kwargs)` | Log data write event |

```python
logger = get_logger(__name__)
logger.info("Processing data", record_count=1000)
logger.log_job_start("my_job", "job_123")
logger.log_job_complete("my_job", "job_123", 5.2)
```

---

### `get_logger`

```python
logger = get_logger(name='simpleetl')
```

Get a `StructuredLogger` instance with the given name.
