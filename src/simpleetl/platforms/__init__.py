"""Platform-specific ETL job runners."""

from .base import PlatformRunner
from .local import LocalPlatformRunner
from .glue import GluePlatformRunner
from .databricks import DatabricksPlatformRunner
from .synapse import SynapsePlatformRunner
from .detector import (
    is_aws_glue,
    is_databricks,
    is_azure_synapse,
    get_current_platform,
    get_platform_info,
)

__all__ = [
    'PlatformRunner',
    'LocalPlatformRunner',
    'GluePlatformRunner',
    'DatabricksPlatformRunner',
    'SynapsePlatformRunner',
    'is_aws_glue',
    'is_databricks',
    'is_azure_synapse',
    'get_current_platform',
    'get_platform_info',
]
