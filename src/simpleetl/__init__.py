"""
SimpleETL: A lightweight, professional ETL framework.

Supports local, AWS Glue, Databricks, and Azure Synapse platforms.
Handles CSV, JSON, Parquet, Avro, ORC, XML, Excel, and database formats.

Quick Start:
    import simpleetl

    # Run a job from config
    simpleetl.run_job("config.yaml")

    # Or use the ETLJob base class
    class MyJob(simpleetl.ETLJob):
        def run(self):
            data = self.extract()
            transformed = self.transform(data)
            self.load(transformed)

    job = MyJob({"name": "my_job", "input_format": "csv", "output_format": "parquet"})
    job.run_with_error_handling()
"""

__version__ = "1.0.0"
__author__ = "SimpleETL Contributors"

# -- Core ETL ----------------------------------------------------------------

from simpleetl.core import (
    ETLJob,
    ETLJobConfig,
    load_config,
    save_config,
    resolve_env_vars,
    EnvVarResolutionError,
)

# -- Errors ------------------------------------------------------------------

from simpleetl.core.errors import (
    ETLError,
    ExtractError,
    TransformError,
    LoadError,
    PartialFailureError,
    ErrorClassification,
    classify_error,
)

# -- Formats -----------------------------------------------------------------

from simpleetl.formats import (
    DataReader,
    DataWriter,
    CSVReader,
    CSVWriter,
    JSONReader,
    JSONWriter,
    ParquetReader,
    ParquetWriter,
    AvroReader,
    AvroWriter,
    OrcReader,
    OrcWriter,
    XMLReader,
    XMLWriter,
    ExcelReader,
    ExcelWriter,
    DatabaseReader,
    DatabaseWriter,
    FormatFactory,
)

# -- Schema ------------------------------------------------------------------

from simpleetl.core.schema import (
    Schema,
    ColumnDef,
    SchemaDiff,
    SchemaValidationError,
    SQLDialect,
    generate_ddl,
    StructType,
    ArrayType,
    MapType,
    FieldDef,
)

# -- Incremental -------------------------------------------------------------

from simpleetl.core.incremental import (
    Watermark,
    WatermarkStore,
    FileWatermarkStore,
    DatabaseWatermarkStore,
    WatermarkManager,
)

# -- Quality -----------------------------------------------------------------

from simpleetl.core.quality import (
    DataQualityError,
    DataQualityReport,
    CheckResult,
    validate_schema,
    check_nulls,
    check_duplicates,
    check_value_range,
    check_unique_values,
    profile_data,
)

# -- Connection --------------------------------------------------------------

from simpleetl.core.connection import (
    ConnectionConfig,
    ConnectionPool,
    get_engine,
    get_connection,
    dispose_engine,
    dispose_all,
)

# -- Checkpoint --------------------------------------------------------------

from simpleetl.core.checkpoint import (
    Checkpoint,
    CheckpointManager,
    CheckpointStore,
    InMemoryCheckpointStore,
    FileCheckpointStore,
)

# -- DLQ ---------------------------------------------------------------------

from simpleetl.core.dlq import (
    DLQEntry,
    DeadLetterQueue,
)

# -- Hooks -------------------------------------------------------------------

from simpleetl.core.hooks import (
    Hook,
    HookContext,
    HookRegistry,
    LoggingHook,
    MetricsHook,
    QualityCheckHook,
    register_hook,
    execute_hooks,
    get_hook_registry,
    PRE_EXTRACT,
    POST_EXTRACT,
    PRE_TRANSFORM,
    POST_TRANSFORM,
    PRE_LOAD,
    POST_LOAD,
    ON_ERROR,
    ON_COMPLETE,
)

# -- Plugins -----------------------------------------------------------------

from simpleetl.core.plugins import (
    Plugin,
    FormatPlugin,
    PluginRegistry,
    register_plugin,
    get_plugin,
    list_plugins,
    register_format,
    get_format_registry,
)

# -- DAG ---------------------------------------------------------------------

from simpleetl.core.dag import (
    DAG,
    DAGResult,
    DAGRunner,
    JobNode,
    NodeResult,
    NodeStatus,
    DAGCycleError,
    DAGMissingDependencyError,
)

# -- Schedule ----------------------------------------------------------------

from simpleetl.core.schedule import (
    CronExpression,
    Schedule,
)

# -- Secrets -----------------------------------------------------------------

from simpleetl.core.secrets import (
    SecretsProvider,
    EnvSecretsProvider,
    AwsSecretsManagerProvider,
    AzureKeyVaultProvider,
    HashiCorpVaultProvider,
    SecretNotFoundError,
    resolve_secrets,
)

# -- Lineage & Observability -------------------------------------------------

from simpleetl.core.lineage import (
    LineageEvent,
    LineageTracker,
    ProvenanceTracker,
    ProvenanceHook,
    LineageHook,
    FileLineageStore,
    OpenLineageConverter,
    DataFreshnessTracker,
    AlertRule,
    AlertChannel,
    WebhookChannel,
    SlackChannel,
    EmailChannel,
    AlertManager,
    get_lineage_tracker,
    configure_lineage_persistence,
    get_file_lineage_store,
    get_freshness_tracker,
    get_alert_manager,
    create_lineage_hook,
    configure_openlineage,
    get_openlineage_converter,
)

# -- Filesystem --------------------------------------------------------------

from simpleetl.core.filesystem import (
    get_filesystem,
    is_cloud_path,
    split_path,
    get_cloud_type,
)

# -- Logging & Metrics -------------------------------------------------------

from simpleetl.core.logger import (
    StructuredLogger,
    get_logger,
)

from simpleetl.core.metrics import (
    MetricsCollector,
    get_metrics,
)

# -- Security ------------------------------------------------------------------

from simpleetl.core.security import (
    detect_pii_columns,
    detect_pii_values,
    mask_pii,
    ColumnEncryptor,
    AuditLogger,
    RBACPolicy,
    apply_rbac_filter,
    mask_email,
    mask_phone,
    mask_credit_card,
)

# -- Health ------------------------------------------------------------------

from simpleetl.core.health import (
    HealthServer,
    start_health_server,
)

# -- Transformations ---------------------------------------------------------

from simpleetl.transformations import (
    filter_data,
    map_values,
    aggregate_data,
    join_data,
    union_data,
    deduplicate_data,
    with_column,
    rename_columns,
    select_columns,
    drop_columns,
    fill_na,
    drop_na,
    sort_data,
    limit_rows,
    sample_data,
    distinct_data,
    cast_columns,
    when_otherwise,
    add_computed_column,
    group_by_aggregate_data,
    pivot_data,
    unpivot_data,
    transform_chain,
    TransformationChain,
    chain,
)

# -- Platforms ---------------------------------------------------------------

from simpleetl.platforms import (
    PlatformRunner,
    LocalPlatformRunner,
    GluePlatformRunner,
    DatabricksPlatformRunner,
    SynapsePlatformRunner,
    get_current_platform,
    get_platform_info,
)

# ============================================================================
# Top-level convenience functions
# ============================================================================


def run_job(config_path: str, platform_override: str | None = None) -> None:
    """Run an ETL job from a configuration file.

    Convenience function that wraps ``simpleetl.cli.run_job``.

    Args:
        config_path: Path to the ETL job configuration file (YAML or JSON).
        platform_override: Optional platform override (e.g., 'local', 'glue').

    Example:
        >>> simpleetl.run_job("jobs/daily_extract.yaml")
    """
    from simpleetl.cli import run_job as _run_job

    _run_job(config_path, platform_override)


def run_dag(
    dag_config_path: str,
    max_parallel: int = 1,
    fail_fast: bool = True,
) -> None:
    """Load and execute a DAG from a YAML configuration file.

    Args:
        dag_config_path: Path to the DAG YAML file.
        max_parallel: Maximum number of concurrent jobs.
        fail_fast: If True, stop on first failure.

    Example:
        >>> simpleetl.run_dag("dags/pipeline.yaml", max_parallel=4)
    """
    from simpleetl.cli import run_dag as _run_dag

    _run_dag(dag_config_path, max_parallel, fail_fast)


def read(source: str, format: str = "auto", **kwargs):
    """Read data from a source using the appropriate reader.

    Args:
        source: Data source (file path, URL, connection string).
        format: Format name (e.g., 'csv', 'parquet', 'json').
            If 'auto', detects from file extension.
        **kwargs: Additional format-specific arguments.

    Returns:
        pandas DataFrame

    Example:
        >>> df = simpleetl.read("data/sales.csv")
        >>> df = simpleetl.read("s3://bucket/data.parquet")
    """
    from simpleetl.formats import FormatFactory

    if format == "auto":
        format = _detect_format(source)

    reader = FormatFactory.get_reader(format)
    return reader.read(source, **kwargs)


def write(df, destination: str, format: str = "auto", **kwargs) -> None:
    """Write a DataFrame to a destination using the appropriate writer.

    Args:
        df: pandas DataFrame to write.
        destination: Output destination (file path, URL, connection string).
        format: Format name. If 'auto', detects from file extension.
        **kwargs: Additional format-specific arguments.

    Example:
        >>> simpleetl.write(df, "output/result.parquet")
        >>> simpleetl.write(df, "postgresql://localhost/db", table_name="results")
    """
    from simpleetl.formats import FormatFactory

    if format == "auto":
        format = _detect_format(destination)

    writer = FormatFactory.get_writer(format)
    writer.write(df, destination, **kwargs)


def _detect_format(path: str) -> str:
    """Detect format from file extension or connection string."""
    from simpleetl.formats import FormatFactory

    info = FormatFactory.detect_format(path)
    fmt = str(info.get("format", ""))
    if fmt and fmt != "unknown":
        return fmt
    raise ValueError(
        f"Cannot auto-detect format for '{path}'. "
        "Please specify format explicitly."
    )


__all__ = [
    # Version
    "__version__",
    # Convenience functions
    "run_job",
    "run_dag",
    "read",
    "write",
    # Core
    "ETLJob",
    "ETLJobConfig",
    "load_config",
    "save_config",
    "resolve_env_vars",
    "EnvVarResolutionError",
    # Errors
    "ETLError",
    "ExtractError",
    "TransformError",
    "LoadError",
    "PartialFailureError",
    "ErrorClassification",
    "classify_error",
    # Formats
    "DataReader",
    "DataWriter",
    "CSVReader",
    "CSVWriter",
    "JSONReader",
    "JSONWriter",
    "ParquetReader",
    "ParquetWriter",
    "AvroReader",
    "AvroWriter",
    "OrcReader",
    "OrcWriter",
    "XMLReader",
    "XMLWriter",
    "ExcelReader",
    "ExcelWriter",
    "DatabaseReader",
    "DatabaseWriter",
    "FormatFactory",
    # Schema
    "Schema",
    "ColumnDef",
    "SchemaDiff",
    "SchemaValidationError",
    "SQLDialect",
    "generate_ddl",
    "StructType",
    "ArrayType",
    "MapType",
    "FieldDef",
    # Incremental
    "Watermark",
    "WatermarkStore",
    "FileWatermarkStore",
    "DatabaseWatermarkStore",
    "WatermarkManager",
    # Quality
    "DataQualityError",
    "DataQualityReport",
    "CheckResult",
    "validate_schema",
    "check_nulls",
    "check_duplicates",
    "check_value_range",
    "check_unique_values",
    "profile_data",
    # Connection
    "ConnectionConfig",
    "ConnectionPool",
    "get_engine",
    "get_connection",
    "dispose_engine",
    "dispose_all",
    # Checkpoint
    "Checkpoint",
    "CheckpointManager",
    "CheckpointStore",
    "InMemoryCheckpointStore",
    "FileCheckpointStore",
    # DLQ
    "DLQEntry",
    "DeadLetterQueue",
    # Hooks
    "Hook",
    "HookContext",
    "HookRegistry",
    "LoggingHook",
    "MetricsHook",
    "QualityCheckHook",
    "register_hook",
    "execute_hooks",
    "get_hook_registry",
    "PRE_EXTRACT",
    "POST_EXTRACT",
    "PRE_TRANSFORM",
    "POST_TRANSFORM",
    "PRE_LOAD",
    "POST_LOAD",
    "ON_ERROR",
    "ON_COMPLETE",
    # Plugins
    "Plugin",
    "FormatPlugin",
    "PluginRegistry",
    "register_plugin",
    "get_plugin",
    "list_plugins",
    "register_format",
    "get_format_registry",
    # DAG
    "DAG",
    "DAGResult",
    "DAGRunner",
    "JobNode",
    "NodeResult",
    "NodeStatus",
    "DAGCycleError",
    "DAGMissingDependencyError",
    # Schedule
    "CronExpression",
    "Schedule",
    # Secrets
    "SecretsProvider",
    "EnvSecretsProvider",
    "AwsSecretsManagerProvider",
    "AzureKeyVaultProvider",
    "HashiCorpVaultProvider",
    "SecretNotFoundError",
    "resolve_secrets",
    # Lineage & Observability
    "LineageEvent",
    "LineageTracker",
    "ProvenanceTracker",
    "ProvenanceHook",
    "LineageHook",
    "FileLineageStore",
    "OpenLineageConverter",
    "DataFreshnessTracker",
    "AlertRule",
    "AlertChannel",
    "WebhookChannel",
    "SlackChannel",
    "EmailChannel",
    "AlertManager",
    "get_lineage_tracker",
    "configure_lineage_persistence",
    "get_file_lineage_store",
    "get_freshness_tracker",
    "get_alert_manager",
    "create_lineage_hook",
    "configure_openlineage",
    "get_openlineage_converter",
    # Filesystem
    "get_filesystem",
    "is_cloud_path",
    "split_path",
    "get_cloud_type",
    # Logging & Metrics
    "StructuredLogger",
    "get_logger",
    "MetricsCollector",
    "get_metrics",
    # Health
    "HealthServer",
    "start_health_server",
    # Security
    "detect_pii_columns",
    "detect_pii_values",
    "mask_pii",
    "ColumnEncryptor",
    "AuditLogger",
    "RBACPolicy",
    "apply_rbac_filter",
    "mask_email",
    "mask_phone",
    "mask_credit_card",
    # Transformations
    "filter_data",
    "map_values",
    "aggregate_data",
    "join_data",
    "union_data",
    "deduplicate_data",
    "with_column",
    "rename_columns",
    "select_columns",
    "drop_columns",
    "fill_na",
    "drop_na",
    "sort_data",
    "limit_rows",
    "sample_data",
    "distinct_data",
    "cast_columns",
    "when_otherwise",
    "add_computed_column",
    "group_by_aggregate_data",
    "pivot_data",
    "unpivot_data",
    "transform_chain",
    "TransformationChain",
    "chain",
    # Platforms
    "PlatformRunner",
    "LocalPlatformRunner",
    "GluePlatformRunner",
    "DatabricksPlatformRunner",
    "SynapsePlatformRunner",
    "get_current_platform",
    "get_platform_info",
]
