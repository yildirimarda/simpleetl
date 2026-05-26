"""Core ETL framework components."""

from .config import (
    ETLJobConfig,
    EnvVarResolutionError,
    load_config,
    resolve_env_vars,
    save_config,
)
from .job import ETLJob
from .logger import StructuredLogger, get_logger
from .metrics import MetricsCollector, get_metrics
from .health import HealthServer, start_health_server
from .filesystem import (
    get_filesystem,
    is_cloud_path,
    split_path,
    get_cloud_type,
)
from .secrets import (
    AwsSecretsManagerProvider,
    AzureKeyVaultProvider,
    EnvSecretsProvider,
    HashiCorpVaultProvider,
    SecretNotFoundError,
    SecretsProvider,
    resolve_secrets,
)
from .errors import (
    ETLError,
    ExtractError,
    TransformError,
    LoadError,
    PartialFailureError,
    ErrorClassification,
    classify_error,
)
from .checkpoint import (
    Checkpoint,
    CheckpointManager,
    CheckpointStore,
    InMemoryCheckpointStore,
    FileCheckpointStore,
)
from .dlq import (
    DLQEntry,
    DeadLetterQueue,
)
from .connection import (
    ConnectionConfig,
    ConnectionPool,
    dispose_all,
    dispose_engine,
    get_connection,
    get_engine,
)
from .dag import DAG, DAGResult, DAGRunner, JobNode, NodeResult, NodeStatus
from .schedule import CronExpression, Schedule

__all__ = [
    'ETLJobConfig',
    'EnvVarResolutionError',
    'load_config',
    'resolve_env_vars',
    'save_config',
    'ETLJob',
    'StructuredLogger',
    'get_logger',
    'MetricsCollector',
    'get_metrics',
    'HealthServer',
    'start_health_server',
    'get_filesystem',
    'is_cloud_path',
    'split_path',
    'get_cloud_type',
    'ConnectionConfig',
    'ConnectionPool',
    'get_engine',
    'get_connection',
    'dispose_engine',
    'dispose_all',
    'SecretsProvider',
    'EnvSecretsProvider',
    'AwsSecretsManagerProvider',
    'AzureKeyVaultProvider',
    'HashiCorpVaultProvider',
    'SecretNotFoundError',
    'resolve_secrets',
    'ETLError',
    'ExtractError',
    'TransformError',
    'LoadError',
    'PartialFailureError',
    'ErrorClassification',
    'classify_error',
    'Checkpoint',
    'CheckpointManager',
    'CheckpointStore',
    'InMemoryCheckpointStore',
    'FileCheckpointStore',
    'DLQEntry',
    'DeadLetterQueue',
    'DAG',
    'DAGResult',
    'DAGRunner',
    'JobNode',
    'NodeResult',
    'NodeStatus',
    'CronExpression',
    'Schedule',
]
