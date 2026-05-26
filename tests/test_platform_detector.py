"""
Tests for the platform detection utilities.
"""

import os
from unittest.mock import patch
from simpleetl.platforms.detector import (
    is_aws_glue,
    is_databricks,
    is_azure_synapse,
    get_current_platform,
    get_platform_info
)


def test_is_aws_glue_true():
    """Test is_aws_glue returns True in Glue environment."""
    with patch.dict(os.environ, {'AWS_EXECUTION_ENV': 'AWS_Glue'}):
        assert is_aws_glue() is True


def test_is_aws_glue_false():
    """Test is_aws_glue returns False in non-Glue environment."""
    with patch.dict(os.environ, {'AWS_EXECUTION_ENV': 'SomethingElse'}):
        assert is_aws_glue() is False


def test_is_aws_glue_empty():
    """Test is_aws_glue returns False when AWS_EXECUTION_ENV is empty."""
    with patch.dict(os.environ, {'AWS_EXECUTION_ENV': ''}):
        assert is_aws_glue() is False


def test_is_databricks_true():
    """Test is_databricks returns True in Databricks environment."""
    with patch.dict(os.environ, {'DATABRICKS_RUNTIME_VERSION': '10.4.x-scala2.12'}):
        assert is_databricks() is True


def test_is_databricks_false():
    """Test is_databricks returns False in non-Databricks environment."""
    with patch.dict(os.environ, {}, clear=True):
        assert is_databricks() is False


def test_is_azure_synapse_true():
    """Test is_azure_synapse returns True in Synapse environment."""
    with patch.dict(os.environ, {'AZURE_SYNAPSE_SPARK_POOL_NAME': 'my-pool'}):
        assert is_azure_synapse() is True


def test_is_azure_synapse_false():
    """Test is_azure_synapse returns False in non-Synapse environment."""
    with patch.dict(os.environ, {}, clear=True):
        assert is_azure_synapse() is False


def test_get_current_platform_glue():
    """Test get_current_platform returns 'glue' in Glue environment."""
    with patch.dict(os.environ, {'AWS_EXECUTION_ENV': 'AWS_Glue'}):
        assert get_current_platform() == 'glue'


def test_get_current_platform_databricks():
    """Test get_current_platform returns 'databricks' in Databricks environment."""
    with patch.dict(os.environ, {'DATABRICKS_RUNTIME_VERSION': '10.4.x-scala2.12'}):
        assert get_current_platform() == 'databricks'


def test_get_current_platform_synapse():
    """Test get_current_platform returns 'synapse' in Synapse environment."""
    with patch.dict(os.environ, {'AZURE_SYNAPSE_SPARK_POOL_NAME': 'my-pool'}):
        assert get_current_platform() == 'synapse'


def test_get_current_platform_local():
    """Test get_current_platform returns 'local' in local environment."""
    with patch.dict(os.environ, {}, clear=True):
        assert get_current_platform() == 'local'


def test_get_platform_info():
    """Test get_platform_info returns expected dictionary."""
    test_env = {
        'AWS_EXECUTION_ENV': 'AWS_Glue',
        'ENVIRONMENT': 'production',
        'SECRET_KEY': 'should-not-appear',
    }

    with patch.dict(os.environ, test_env, clear=True):
        info = get_platform_info()
        assert info['platform'] == 'glue'
        assert info['is_glue'] is True
        assert info['is_databricks'] is False
        assert info['is_synapse'] is False
        assert 'system' in info
        assert 'python_version' in info
        assert 'environment' in info
        # Safe env vars are included
        assert info['environment']['AWS_EXECUTION_ENV'] == 'AWS_Glue'
        assert info['environment']['ENVIRONMENT'] == 'production'
        # Sensitive vars like SECRET_KEY must NOT be included
        assert 'SECRET_KEY' not in info['environment']