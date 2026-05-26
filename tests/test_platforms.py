"""
Tests for the platform-specific runners.
"""

import os
from unittest.mock import Mock, patch, MagicMock
from simpleetl.core.job import ETLJob
from simpleetl.platforms.local import LocalPlatformRunner
from simpleetl.platforms.glue import GluePlatformRunner
from simpleetl.platforms.databricks import DatabricksPlatformRunner
from simpleetl.platforms.synapse import SynapsePlatformRunner


class DummyJob(ETLJob):
    """A dummy ETL job for testing."""

    def __init__(self):
        self.config = Mock()
        self.config.name = "test-job"

    def run(self) -> None:
        """Dummy implementation."""
        pass


def test_local_platform_runner():
    """Test that LocalPlatformRunner calls run_with_error_handling."""
    job = DummyJob()
    runner = LocalPlatformRunner()

    with patch.object(job, 'run_with_error_handling') as mock_run:
        runner.run_job(job)
        mock_run.assert_called_once()


def test_glue_platform_runner_in_glue_env():
    """Test GluePlatformRunner in Glue environment."""
    job = DummyJob()
    runner = GluePlatformRunner()

    with patch.dict(os.environ, {'AWS_EXECUTION_ENV': 'AWS_Glue'}), \
         patch('simpleetl.platforms.glue.is_aws_glue', return_value=True), \
         patch('simpleetl.platforms.glue.create_glue_context') as mock_ctx, \
         patch.object(job, 'run_with_error_handling') as mock_run:
        mock_ctx.return_value = MagicMock()
        runner.run_job(job)
        mock_run.assert_called_once()


def test_glue_platform_runner_not_in_glue_env():
    """Test GluePlatformRunner not in Glue environment logs warning and runs."""
    job = DummyJob()
    runner = GluePlatformRunner()

    with patch.dict(os.environ, {'AWS_EXECUTION_ENV': 'SomethingElse'}), \
         patch('simpleetl.platforms.glue.is_aws_glue', return_value=False), \
         patch.object(job, 'run_with_error_handling') as mock_run:
        runner.run_job(job)
        mock_run.assert_called_once()


def test_databricks_platform_runner_in_databricks_env():
    """Test DatabricksPlatformRunner in Databricks environment."""
    job = DummyJob()
    runner = DatabricksPlatformRunner()

    with patch.dict(os.environ, {'DATABRICKS_RUNTIME_VERSION': '10.4.x-scala2.12'}), \
         patch('simpleetl.platforms.detector.is_databricks', return_value=True), \
         patch.object(job, 'run_with_error_handling') as mock_run:
        runner.run_job(job)
        mock_run.assert_called_once()


def test_databricks_platform_runner_not_in_databricks_env():
    """Test DatabricksPlatformRunner not in Databricks environment."""
    job = DummyJob()
    runner = DatabricksPlatformRunner()

    with patch.dict(os.environ, {}, clear=True), \
         patch.object(job, 'run_with_error_handling') as mock_run, \
         patch('logging.getLogger') as mock_get_logger:
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        runner.run_job(job)
        mock_run.assert_called_once()
        mock_logger.warning.assert_called_once()


def test_synapse_platform_runner_in_synapse_env():
    """Test SynapsePlatformRunner in Synapse environment."""
    job = DummyJob()
    runner = SynapsePlatformRunner()

    with patch.dict(os.environ, {'AZURE_SYNAPSE_SPARK_POOL_NAME': 'my-pool'}), \
         patch.object(job, 'run_with_error_handling') as mock_run:
        runner.run_job(job)
        mock_run.assert_called_once()


def test_synapse_platform_runner_not_in_synapse_env():
    """Test SynapsePlatformRunner not in Synapse environment."""
    job = DummyJob()
    runner = SynapsePlatformRunner()

    with patch.dict(os.environ, {}, clear=True), \
         patch.object(job, 'run_with_error_handling') as mock_run, \
         patch('logging.getLogger') as mock_get_logger:
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        runner.run_job(job)
        mock_run.assert_called_once()
        mock_logger.warning.assert_called_once()
