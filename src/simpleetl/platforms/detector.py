"""
Platform detection utilities.
"""

import os
import platform


def is_aws_glue() -> bool:
    """
    Detect if the code is running in an AWS Glue environment.

    Returns:
        bool: True if running in AWS Glue, False otherwise.
    """
    # AWS Glue sets the AWS_EXECUTION_ENV environment variable to a value starting with 'AWS_Glue'
    # See: https://docs.aws.amazon.com/glue/latest/dg/monitoring-continuous-logging.html
    aws_exec_env = os.environ.get('AWS_EXECUTION_ENV') or ''
    return aws_exec_env.startswith('AWS_Glue')


def is_databricks() -> bool:
    """
    Detect if the code is running in a Databricks environment.

    Returns:
        bool: True if running in Databricks, False otherwise.
    """
    # Databricks sets the DATABRICKS_RUNTIME_VERSION environment variable
    # See: https://docs.databricks.com/dev-tools/env-vars.html
    return bool(os.environ.get('DATABRICKS_RUNTIME_VERSION'))


def is_azure_synapse() -> bool:
    """
    Detect if the code is running in an Azure Synapse environment.

    Returns:
        bool: True if running in Azure Synapse, False otherwise.
    """
    # Azure Synapse Spark pools set the AZURE_SYNAPSE_SPARK_POOL_NAME environment variable
    # Note: This is not officially documented but observed in environment variables
    return bool(os.environ.get('AZURE_SYNAPSE_SPARK_POOL_NAME'))


def get_current_platform() -> str:
    """
    Get the current platform as a string.

    Returns:
        str: One of 'glue', 'databricks', 'synapse', 'local', or 'unknown'.
    """
    if is_aws_glue():
        return 'glue'
    elif is_databricks():
        return 'databricks'
    elif is_azure_synapse():
        return 'synapse'
    else:
        # Default to local if none of the above
        return 'local'


def get_platform_info() -> dict:
    """
    Get detailed information about the current platform.

    Returns:
        dict: A dictionary containing platform detection results and system information.
              Only safe, non-sensitive environment variables are included.
    """
    safe_env_prefixes = (
        'AWS_EXECUTION_ENV',
        'DATABRICKS_RUNTIME_VERSION',
        'AZURE_SYNAPSE_SPARK_POOL_NAME',
        'ENVIRONMENT',
        'LOG_LEVEL',
        'PYTHONPATH',
        'HOME',
        'PATH',
        'LANG',
        'LC_ALL',
    )
    safe_env = {
        k: v for k, v in os.environ.items()
        if any(k.startswith(prefix) for prefix in safe_env_prefixes)
    }
    return {
        'platform': get_current_platform(),
        'is_glue': is_aws_glue(),
        'is_databricks': is_databricks(),
        'is_synapse': is_azure_synapse(),
        'system': platform.system(),
        'python_version': platform.python_version(),
        'environment': safe_env,
    }