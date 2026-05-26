"""
Tests for the structured logging module.
"""

import json
import logging
import pytest
import tempfile
import os
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch, MagicMock
from simpleetl.core.logger import JSONFormatter, StructuredLogger, get_logger


class TestJSONFormatter:
    """Test JSONFormatter."""

    def test_format_basic(self):
        """Test basic JSON formatting."""
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name='test', level=logging.INFO,
            pathname='test.py', lineno=1,
            msg='Hello', args=(), exc_info=None
        )
        result = formatter.format(record)
        parsed = json.loads(result)
        assert parsed['message'] == 'Hello'
        assert parsed['level'] == 'INFO'
        assert parsed['logger'] == 'test'
        assert 'timestamp' in parsed

    def test_format_with_exception(self):
        """Test JSON formatting with exception info."""
        formatter = JSONFormatter()
        try:
            raise ValueError("test error")
        except ValueError:
            import sys
            exc_info = sys.exc_info()
            record = logging.LogRecord(
                name='test', level=logging.ERROR,
                pathname='test.py', lineno=1,
                msg='Error occurred', args=(), exc_info=exc_info
            )
            result = formatter.format(record)
            parsed = json.loads(result)
            assert 'exception' in parsed
            assert 'ValueError' in parsed['exception']

    def test_format_with_extra_fields(self):
        """Test JSON formatting with extra fields."""
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name='test', level=logging.INFO,
            pathname='test.py', lineno=1,
            msg='Hello', args=(), exc_info=None
        )
        record.event = 'job_start'
        record.job_name = 'my_job'
        result = formatter.format(record)
        parsed = json.loads(result)
        assert parsed['event'] == 'job_start'
        assert parsed['job_name'] == 'my_job'


class TestStructuredLogger:
    """Test StructuredLogger."""

    def test_get_logger(self):
        """Test get_logger returns StructuredLogger."""
        logger = get_logger('test_module')
        assert isinstance(logger, StructuredLogger)

    def test_log_job_start(self):
        """Test log_job_start method."""
        logger = StructuredLogger('test')
        with patch.object(logger.logger, 'info') as mock_info:
            logger.log_job_start('my_job', 'job_123')
            mock_info.assert_called_once()
            call_args = mock_info.call_args
            assert call_args[0][0] == 'Starting job: my_job'
            extra = call_args[1]['extra']
            assert extra['event'] == 'job_start'
            assert extra['job_name'] == 'my_job'
            assert extra['job_id'] == 'job_123'

    def test_log_job_complete(self):
        """Test log_job_complete method."""
        logger = StructuredLogger('test')
        with patch.object(logger.logger, 'info') as mock_info:
            logger.log_job_complete('my_job', 'job_123', 5.5)
            mock_info.assert_called_once()
            call_args = mock_info.call_args
            assert 'Completed job: my_job' in call_args[0][0]
            extra = call_args[1]['extra']
            assert extra['event'] == 'job_complete'
            assert extra['duration'] == 5.5

    def test_log_job_error(self):
        """Test log_job_error method."""
        logger = StructuredLogger('test')
        with patch.object(logger.logger, 'error') as mock_error:
            logger.log_job_error('my_job', 'job_123', 'Something went wrong')
            mock_error.assert_called_once()
            call_args = mock_error.call_args
            assert 'Job failed: my_job' in call_args[0][0]
            extra = call_args[1]['extra']
            assert extra['event'] == 'job_error'
            assert extra['error'] == 'Something went wrong'

    def test_log_data_read(self):
        """Test log_data_read method."""
        logger = StructuredLogger('test')
        with patch.object(logger.logger, 'info') as mock_info:
            logger.log_data_read('input.csv', 100)
            mock_info.assert_called_once()
            call_args = mock_info.call_args
            assert 'Read 100 records from input.csv' in call_args[0][0]
            extra = call_args[1]['extra']
            assert extra['event'] == 'data_read'
            assert extra['record_count'] == 100

    def test_log_data_write(self):
        """Test log_data_write method."""
        logger = StructuredLogger('test')
        with patch.object(logger.logger, 'info') as mock_info:
            logger.log_data_write('output.parquet', 50)
            mock_info.assert_called_once()
            call_args = mock_info.call_args
            assert 'Wrote 50 records to output.parquet' in call_args[0][0]
            extra = call_args[1]['extra']
            assert extra['event'] == 'data_write'
            assert extra['record_count'] == 50

    def test_debug_method(self):
        """Test debug logging method."""
        logger = StructuredLogger('test')
        with patch.object(logger.logger, 'debug') as mock_debug:
            logger.debug('debug message', custom_field='value')
            mock_debug.assert_called_once()

    def test_warning_method(self):
        """Test warning logging method."""
        logger = StructuredLogger('test')
        with patch.object(logger.logger, 'warning') as mock_warning:
            logger.warning('warning message', custom_field='value')
            mock_warning.assert_called_once()

    def test_critical_method(self):
        """Test critical logging method."""
        logger = StructuredLogger('test')
        with patch.object(logger.logger, 'critical') as mock_critical:
            logger.critical('critical message', custom_field='value')
            mock_critical.assert_called_once()

    def test_format_with_stack_trace(self):
        """Test JSON formatting with both exception and stack_info (line 33)."""
        formatter = JSONFormatter()
        try:
            raise ValueError("test error")
        except ValueError:
            import sys
            exc_info = sys.exc_info()
            record = logging.LogRecord(
                name='test', level=logging.ERROR,
                pathname='test.py', lineno=1,
                msg='Error occurred', args=(), exc_info=exc_info
            )
            # Set stack_info to trigger line 33
            record.stack_info = "Stack trace info here"
            result = formatter.format(record)
            parsed = json.loads(result)
            assert 'exception' in parsed
            assert 'stack_trace' in parsed
            assert parsed['stack_trace'] == "Stack trace info here"

    def test_json_serializer_datetime(self):
        """Test _json_serializer handles datetime objects (line 51)."""
        formatter = JSONFormatter()
        dt = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        result = formatter._json_serializer(dt)
        assert result == "2024-01-15T10:30:00+00:00"

    def test_json_serializer_path(self):
        """Test _json_serializer handles Path objects (line 53)."""
        formatter = JSONFormatter()
        p = Path("/some/path/to/file.txt")
        result = formatter._json_serializer(p)
        assert result == "/some/path/to/file.txt"

    def test_json_serializer_type_error(self):
        """Test _json_serializer raises TypeError for unsupported types (line 54)."""
        formatter = JSONFormatter()
        with pytest.raises(TypeError, match="is not JSON serializable"):
            formatter._json_serializer({"set_value": set([1, 2, 3])})

    def test_file_handler_when_logs_dir_exists(self):
        """Test that file handler is added when logs directory exists (lines 79-85)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            logs_dir = os.path.join(tmpdir, "logs")
            os.makedirs(logs_dir)

            with patch('simpleetl.core.logger.Path') as mock_path_class:
                mock_path_instance = MagicMock()
                mock_path_instance.exists.return_value = True
                mock_path_instance.__truediv__ = MagicMock(return_value=Path(logs_dir) / 'etl.log')
                mock_path_class.return_value = mock_path_instance

                # Create a real Path for the RotatingFileHandler
                with patch('simpleetl.core.logger.logging.handlers.RotatingFileHandler') as mock_handler:
                    logger = StructuredLogger('test_file_handler')
                    # Verify the file handler was set up
                    mock_handler.assert_called_once()
                    # Should have 2 handlers: console + file
                    assert len(logger.logger.handlers) == 2
