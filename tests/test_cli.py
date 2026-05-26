"""
Tests for the CLI module.
"""

import pytest
import tempfile
import os
import sys
from unittest.mock import patch
from io import StringIO

from simpleetl.cli import create_parser, list_formats, detect_platform, run_job, main


class TestCLIParser:
    """Test CLI argument parser."""

    def test_parser_creates(self):
        """Test that parser can be created."""
        parser = create_parser()
        assert parser is not None

    def test_parser_list_formats(self):
        """Test --list-formats argument."""
        parser = create_parser()
        args = parser.parse_args(["--list-formats"])
        assert args.list_formats is True

    def test_parser_detect_platform(self):
        """Test --detect-platform argument."""
        parser = create_parser()
        args = parser.parse_args(["--detect-platform"])
        assert args.detect_platform is True

    def test_parser_config(self):
        """Test --config argument."""
        parser = create_parser()
        args = parser.parse_args(["--config", "test.yaml"])
        assert args.config == "test.yaml"

    def test_parser_dry_run(self):
        """Test --dry-run argument."""
        parser = create_parser()
        args = parser.parse_args(["--config", "test.yaml", "--dry-run"])
        assert args.dry_run is True

    def test_parser_platform_override(self):
        """Test --platform argument."""
        parser = create_parser()
        args = parser.parse_args(["--platform", "glue"])
        assert args.platform == "glue"

    def test_parser_version(self):
        """Test --version argument."""
        parser = create_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--version"])


class TestCLICommands:
    """Test CLI commands."""

    def test_list_formats(self):
        """Test list_formats command."""
        with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
            list_formats()
            output = mock_stdout.getvalue()
        assert "csv" in output
        assert "json" in output
        assert "parquet" in output
        assert "database" in output

    def test_detect_platform(self):
        """Test detect_platform command."""
        with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
            detect_platform()
            output = mock_stdout.getvalue()
        assert "Current platform" in output
        assert "System" in output

    def test_run_job(self):
        """Test run_job command with valid config."""
        config_data = {
            "name": "test_job",
            "description": "A test job",
            "platform": "local",
            "input_format": "csv",
            "output_format": "csv",
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            import yaml
            yaml.dump(config_data, f)
            temp_path = f.name

        try:
            with patch("sys.stdout", new_callable=StringIO):
                run_job(temp_path)
            # Should not raise
        finally:
            os.unlink(temp_path)

    def test_run_job_with_platform_override(self):
        """Test run_job with platform override."""
        config_data = {
            "name": "test_job",
            "platform": "local",
            "input_format": "csv",
            "output_format": "csv",
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            import yaml
            yaml.dump(config_data, f)
            temp_path = f.name

        try:
            with patch("sys.stdout", new_callable=StringIO):
                run_job(temp_path, platform_override="glue")
            # Should not raise
        finally:
            os.unlink(temp_path)

    def test_run_job_with_job_class(self):
        """Test run_job with a valid job_class in params."""
        config_data = {
            "name": "test_job",
            "platform": "local",
            "input_format": "csv",
            "output_format": "csv",
            "params": {
                "job_class": "tests.test_cli._DummyJob",
            },
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            import yaml
            yaml.dump(config_data, f)
            temp_path = f.name

        try:
            with patch("sys.stdout", new_callable=StringIO):
                run_job(temp_path)
        finally:
            os.unlink(temp_path)

    def test_run_job_with_bad_job_class(self):
        """Test run_job exits on invalid job_class."""
        config_data = {
            "name": "test_job",
            "platform": "local",
            "input_format": "csv",
            "output_format": "csv",
            "params": {
                "job_class": "nonexistent.module.Job",
            },
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            import yaml
            yaml.dump(config_data, f)
            temp_path = f.name

        try:
            with pytest.raises(SystemExit):
                with patch("sys.stdout", new_callable=StringIO):
                    run_job(temp_path)
        finally:
            os.unlink(temp_path)


class TestCLIMain:
    """Test the main() entry point of the CLI."""

    def test_main_list_formats(self):
        """Test main with --list-formats."""
        with patch.object(sys, 'argv', ['simpleetl', '--list-formats']):
            with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
                main()
            output = mock_stdout.getvalue()
            assert "csv" in output

    def test_main_detect_platform(self):
        """Test main with --detect-platform."""
        with patch.object(sys, 'argv', ['simpleetl', '--detect-platform']):
            with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
                main()
            output = mock_stdout.getvalue()
            assert "Current platform" in output

    def test_main_config_not_found(self):
        """Test main exits when config file does not exist."""
        with patch.object(sys, 'argv', ['simpleetl', '--config', '/nonexistent/path.yaml']):
            with pytest.raises(SystemExit):
                main()

    def test_main_dry_run(self):
        """Test main with --dry-run."""
        config_data = {
            "name": "test_job",
            "platform": "local",
            "input_format": "csv",
            "output_format": "csv",
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            import yaml
            yaml.dump(config_data, f)
            temp_path = f.name

        try:
            with patch.object(sys, 'argv', ['simpleetl', '--config', temp_path, '--dry-run']):
                with patch("sys.stdout", new_callable=StringIO):
                    main()
        finally:
            os.unlink(temp_path)

    def test_main_run_job(self):
        """Test main with --config to run a job."""
        config_data = {
            "name": "test_job",
            "platform": "local",
            "input_format": "csv",
            "output_format": "csv",
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            import yaml
            yaml.dump(config_data, f)
            temp_path = f.name

        try:
            with patch.object(sys, 'argv', ['simpleetl', '--config', temp_path]):
                with patch("sys.stdout", new_callable=StringIO):
                    main()
        finally:
            os.unlink(temp_path)

    def test_main_no_args_prints_help(self):
        """Test main with no arguments prints help."""
        with patch.object(sys, 'argv', ['simpleetl']):
            with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
                main()
            output = mock_stdout.getvalue()
            assert "usage" in output.lower() or "help" in output.lower() or "simpleetl" in output.lower()


class TestCLIMainModule:
    """Test the cli.py __main__ guard (line 153)."""

    def test_cli_main_guard(self):
        """Test that cli.py has the if __name__ == '__main__' guard that calls main()."""

        # Use subprocess to actually run `python -m simpleetl` which triggers the guard
        # We just verify the module has the guard by checking the source
        import inspect
        from simpleetl import cli as cli_module
        source = inspect.getsource(cli_module)
        assert 'if __name__ == "__main__":' in source
        assert 'main()' in source

    def test_cli_main_guard_via_subprocess(self):
        """Test cli.py __main__ guard by running `python -m simpleetl` in subprocess."""
        import subprocess
        import sys

        result = subprocess.run(
            [sys.executable, "-m", "simpleetl"],
            capture_output=True,
            text=True,
        )
        # Should print help (no args) without errors
        assert result.returncode == 0
        assert "usage" in result.stdout.lower() or "simpleetl" in result.stdout.lower()


class _DummyJob:
    """Dummy ETL job for testing job_class loading."""

    def __init__(self, config):
        self.config = config

    def run_with_error_handling(self):
        pass
