"""
Tests for the ETL job execution engine.
"""

import logging
from unittest.mock import patch
import pytest
from simpleetl.core.job import ETLJob
from simpleetl.core.config import ETLJobConfig


class TestJob(ETLJob):
    """A test ETL job for testing purposes."""

    def __init__(self, config, should_fail=False, fail_count=0, error_type=None):
        super().__init__(config)
        self.should_fail = should_fail
        self.fail_count = fail_count
        self.call_count = 0
        self.error_type = error_type or ConnectionError

    def run(self) -> None:
        """Implementation of the run method for testing."""
        self.call_count += 1
        if self.should_fail and self.call_count <= self.fail_count:
            raise self.error_type(f"Intentional failure on call {self.call_count}")


def test_job_initialization():
    """Test that a job can be initialized with a config."""
    config = ETLJobConfig(
        name="test_job",
        input_format="csv",
        output_format="csv"
    )
    job = TestJob(config)
    assert job.config.name == "test_job"
    assert job.logger.name == "simpleetl.core.job.test_job"


def test_setup_logging():
    """Test that logging is set up correctly."""
    config = ETLJobConfig(
        name="test_job",
        input_format="csv",
        output_format="csv",
        log_level="DEBUG"
    )
    job = TestJob(config)

    # Clear any handlers from previous tests to ensure clean state
    job.logger.handlers.clear()

    # Logger starts with no handlers (setup_logging is not called in __init__)
    assert len(job.logger.handlers) == 0

    # After explicit setup, should have one handler
    job._setup_logging()
    assert len(job.logger.handlers) == 1
    assert job.logger.level == logging.DEBUG


def test_setup_logging_invalid_level():
    """Test that invalid log level defaults to INFO."""
    config = ETLJobConfig(
        name="test_job",
        input_format="csv",
        output_format="csv",
        log_level="INVALID"
    )
    job = TestJob(config)

    # Clear any existing handlers to ensure clean state
    job.logger.handlers.clear()

    with patch.object(job.logger, 'warning') as mock_warning:
        job._setup_logging()
        # Should warn about invalid log level
        mock_warning.assert_called_once()
        assert job.logger.level == logging.INFO


def test_run_success():
    """Test successful job execution."""
    config = ETLJobConfig(
        name="test_job",
        input_format="csv",
        output_format="csv"
    )
    job = TestJob(config, should_fail=False)

    with patch.object(job.logger, 'info') as mock_info:
        job.run_with_error_handling()
        # Should log start and success
        assert mock_info.call_count == 2
        assert job.call_count == 1


def test_run_with_retries_eventual_success():
    """Test job that fails initially but succeeds after retries."""
    config = ETLJobConfig(
        name="test_job",
        input_format="csv",
        output_format="csv",
        max_retries=2,
        retry_delay=0.01  # Short delay for testing
    )
    job = TestJob(config, should_fail=True, fail_count=1)  # Fail once, then succeed

    with patch('time.sleep') as mock_sleep, \
         patch.object(job.logger, 'info') as mock_info, \
         patch.object(job.logger, 'warning') as mock_warning:

        job.run_with_error_handling()

        # Should have attempted twice (1 failure + 1 success)
        assert job.call_count == 2
        # Should have slept once (between attempts)
        assert mock_sleep.call_count == 1
        # Should have logged warning for failure
        assert mock_warning.call_count >= 1
        # Should have logged info for start and success
        assert mock_info.call_count >= 2


def test_run_max_retries_exceeded():
    """Test job that fails beyond max retries."""
    config = ETLJobConfig(
        name="test_job",
        input_format="csv",
        output_format="csv",
        max_retries=2,
        retry_delay=0.01  # Short delay for testing
    )
    job = TestJob(config, should_fail=True, fail_count=5)  # Always fail

    with patch('time.sleep') as mock_sleep, \
         patch.object(job.logger, 'error') as mock_error:

        with pytest.raises(ConnectionError, match="Intentional failure"):
            job.run_with_error_handling()

        # Should have attempted 3 times (initial + 2 retries)
        assert job.call_count == 3
        # Should have slept twice (between attempts)
        assert mock_sleep.call_count == 2
        # Should have logged error at the end
        mock_error.assert_called_once()


def test_extract_transform_load_defaults():
    """Test the default extract, transform, and load methods."""
    config = ETLJobConfig(
        name="test_job",
        input_format="csv",
        output_format="csv"
    )
    job = TestJob(config)

    # extract should return None and log warning
    with patch.object(job.logger, 'warning') as mock_warning:
        result = job.extract()
        assert result is None
        mock_warning.assert_called_once()

    # transform should return input unchanged and log debug
    test_data = "test"
    with patch.object(job.logger, 'debug') as mock_debug:
        result = job.transform(test_data)
        assert result == test_data
        mock_debug.assert_called_once()

    # load should log warning and return None
    with patch.object(job.logger, 'warning') as mock_warning:
        job.load("test_data")
        mock_warning.assert_called_once()