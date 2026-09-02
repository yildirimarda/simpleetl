"""
Tests for the CLI module.
"""

import pytest
import tempfile
import os
import sys
from unittest.mock import patch
from io import StringIO

from simpleetl.cli import (
    create_parser,
    detect_platform,
    init_project,
    list_formats,
    main,
    run_job,
    validate_config_file,
)


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

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
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

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
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

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
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

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
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
        with patch.object(sys, "argv", ["simpleetl", "--list-formats"]):
            with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
                main()
            output = mock_stdout.getvalue()
            assert "csv" in output

    def test_main_detect_platform(self):
        """Test main with --detect-platform."""
        with patch.object(sys, "argv", ["simpleetl", "--detect-platform"]):
            with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
                main()
            output = mock_stdout.getvalue()
            assert "Current platform" in output

    def test_main_config_not_found(self):
        """Test main exits when config file does not exist."""
        with patch.object(
            sys, "argv", ["simpleetl", "--config", "/nonexistent/path.yaml"]
        ):
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

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            import yaml

            yaml.dump(config_data, f)
            temp_path = f.name

        try:
            with patch.object(
                sys, "argv", ["simpleetl", "--config", temp_path, "--dry-run"]
            ):
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

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            import yaml

            yaml.dump(config_data, f)
            temp_path = f.name

        try:
            with patch.object(sys, "argv", ["simpleetl", "--config", temp_path]):
                with patch("sys.stdout", new_callable=StringIO):
                    main()
        finally:
            os.unlink(temp_path)

    def test_main_no_args_prints_help(self):
        """Test main with no arguments prints help."""
        with patch.object(sys, "argv", ["simpleetl"]):
            with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
                main()
            output = mock_stdout.getvalue()
            assert (
                "usage" in output.lower()
                or "help" in output.lower()
                or "simpleetl" in output.lower()
            )


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
        assert "main()" in source

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


class TestCLIInit:
    """Test the --init project scaffolding command."""

    def test_parser_init(self):
        """Test --init argument parsing."""
        parser = create_parser()
        args = parser.parse_args(["--init", "myproject"])
        assert args.init == "myproject"

    def test_init_creates_scaffold(self, tmp_path):
        """Test that --init creates the project files and prints a summary."""
        target = tmp_path / "proj"
        with patch.object(sys, "argv", ["simpleetl", "--init", str(target)]):
            with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
                main()
            output = mock_stdout.getvalue()

        assert (target / "config.yaml").is_file()
        assert (target / "job.py").is_file()
        assert (target / "tests" / "test_starter.py").is_file()
        assert (target / "data" / "input.csv").is_file()
        assert (target / "README.md").is_file()
        assert (target / "output").is_dir()
        assert "config.yaml" in output
        assert "tests/test_starter.py" in output
        assert "Next steps" in output

    def test_init_scaffold_contents(self, tmp_path):
        """Test the contents of the generated scaffold files."""
        target = tmp_path / "proj"
        with patch("sys.stdout", new_callable=StringIO):
            init_project(str(target))

        config_text = (target / "config.yaml").read_text()
        assert "input_format: csv" in config_text
        assert "output_format: parquet" in config_text
        assert "validation_rules" in config_text  # commented example

        job_text = (target / "job.py").read_text()
        assert "class StarterJob(ETLJob)" in job_text
        assert "def extract" in job_text
        assert "def transform" in job_text
        assert "def load" in job_text
        assert 'if __name__ == "__main__":' in job_text

        csv_lines = (target / "data" / "input.csv").read_text().splitlines()
        assert csv_lines[0] == "id,name,value"
        assert len(csv_lines) > 1

        test_text = (target / "tests" / "test_starter.py").read_text()
        assert "class TestStarterJob" in test_text
        assert "test_job_runs" in test_text
        assert "def test_config_loads" in test_text

        readme_text = (target / "README.md").read_text()
        assert "python job.py" in readme_text
        assert "simpleetl --config config.yaml" in readme_text
        assert "pytest" in readme_text

    def test_init_generated_config_validates(self, tmp_path):
        """Test that the generated config.yaml passes load_config()."""
        from simpleetl.core.config import load_config

        target = tmp_path / "proj"
        with patch("sys.stdout", new_callable=StringIO):
            init_project(str(target))

        config = load_config(str(target / "config.yaml"))
        assert config.name == "starter_job"
        assert config.platform == "local"
        assert config.input_format == "csv"
        assert config.output_format == "parquet"
        assert config.params["input_path"] == "data/input.csv"

    def test_init_existing_empty_dir_ok(self, tmp_path):
        """Test that --init accepts an existing but empty directory."""
        target = tmp_path / "proj"
        target.mkdir()
        with patch("sys.stdout", new_callable=StringIO):
            init_project(str(target))
        assert (target / "config.yaml").is_file()

    def test_init_nonempty_dir_errors(self, tmp_path):
        """Test that --init exits non-zero for a non-empty directory."""
        target = tmp_path / "proj"
        target.mkdir()
        (target / "existing.txt").write_text("data")

        with patch.object(sys, "argv", ["simpleetl", "--init", str(target)]):
            with patch("sys.stderr", new_callable=StringIO) as mock_stderr:
                with pytest.raises(SystemExit) as excinfo:
                    main()
        assert excinfo.value.code == 1
        assert "not" in mock_stderr.getvalue()
        assert "empty" in mock_stderr.getvalue()
        # Existing content untouched.
        assert (target / "existing.txt").read_text() == "data"
        assert not (target / "config.yaml").exists()

    def test_init_target_is_file_errors(self, tmp_path):
        """Test that --init exits non-zero when the target is a file."""
        target = tmp_path / "proj"
        target.write_text("I am a file")

        with patch("sys.stderr", new_callable=StringIO) as mock_stderr:
            with pytest.raises(SystemExit) as excinfo:
                init_project(str(target))
        assert excinfo.value.code == 1
        assert "not a directory" in mock_stderr.getvalue()

    def test_init_scaffolded_job_runs(self, tmp_path):
        """Test that the scaffolded job.py runs end to end."""
        import subprocess

        target = tmp_path / "proj"
        with patch("sys.stdout", new_callable=StringIO):
            init_project(str(target))

        result = subprocess.run(
            [sys.executable, "job.py"],
            cwd=str(target),
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr
        assert (target / "output" / "output.parquet").is_file()

    def test_init_scaffolded_test_runs(self, tmp_path):
        """Test that the scaffolded test_starter.py passes with pytest."""
        import subprocess

        target = tmp_path / "proj"
        with patch("sys.stdout", new_callable=StringIO):
            init_project(str(target))

        result = subprocess.run(
            [sys.executable, "-m", "pytest", "tests/test_starter.py", "-v", "-q"],
            cwd=str(target),
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr + result.stdout


class TestCLIValidateConfig:
    """Test the --validate-config command."""

    VALID_CONFIG = {
        "name": "test_job",
        "description": "A test job",
        "platform": "local",
        "input_format": "csv",
        "output_format": "parquet",
    }

    def _write_config(self, tmp_path, config_data):
        """Write *config_data* to a YAML file and return its path."""
        import yaml

        config_file = tmp_path / "config.yaml"
        config_file.write_text(yaml.safe_dump(config_data))
        return str(config_file)

    def test_parser_validate_config(self):
        """Test --validate-config argument parsing."""
        parser = create_parser()
        args = parser.parse_args(["--validate-config", "cfg.yaml"])
        assert args.validate_config == "cfg.yaml"

    def test_validate_config_success(self, tmp_path):
        """Test --validate-config prints a summary for a valid config."""
        config_path = self._write_config(tmp_path, self.VALID_CONFIG)

        argv = ["simpleetl", "--validate-config", config_path]
        with patch.object(sys, "argv", argv):
            with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
                main()
            output = mock_stdout.getvalue()

        assert "Configuration valid" in output
        assert "test_job" in output
        assert "local" in output
        assert "csv" in output
        assert "parquet" in output
        assert "Validation rules: 0" in output
        assert "Incremental:      off" in output
        assert "Schema drift:     off" in output
        assert "Tracing:          off" in output

    def test_validate_config_missing_field(self, tmp_path):
        """Test --validate-config exits non-zero on a missing field."""
        config_data = dict(self.VALID_CONFIG)
        del config_data["input_format"]
        config_path = self._write_config(tmp_path, config_data)

        argv = ["simpleetl", "--validate-config", config_path]
        with patch.object(sys, "argv", argv):
            with patch("sys.stderr", new_callable=StringIO) as mock_stderr:
                with pytest.raises(SystemExit) as excinfo:
                    main()
        assert excinfo.value.code == 1
        assert "Configuration invalid" in mock_stderr.getvalue()
        assert "input_format" in mock_stderr.getvalue()

    def test_validate_config_invalid_yaml(self, tmp_path):
        """Test --validate-config exits non-zero on malformed YAML."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("name: [unclosed\nplatform: local")

        with patch("sys.stderr", new_callable=StringIO) as mock_stderr:
            with pytest.raises(SystemExit) as excinfo:
                validate_config_file(str(config_file))
        assert excinfo.value.code == 1
        assert "Configuration invalid" in mock_stderr.getvalue()

    def test_validate_config_missing_file(self):
        """Test --validate-config exits non-zero on a missing file."""
        argv = ["simpleetl", "--validate-config", "/nonexistent/cfg.yaml"]
        with patch.object(sys, "argv", argv):
            with patch("sys.stderr", new_callable=StringIO) as mock_stderr:
                with pytest.raises(SystemExit) as excinfo:
                    main()
        assert excinfo.value.code == 1
        assert "not found" in mock_stderr.getvalue()

    def test_validate_config_with_param(self, tmp_path):
        """Test --validate-config renders --param template variables."""
        pytest.importorskip("jinja2")
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            "name: job_{{ params.env }}\n"
            "platform: local\n"
            "input_format: csv\n"
            "output_format: csv\n"
        )

        argv = [
            "simpleetl",
            "--validate-config",
            str(config_file),
            "--param",
            "env=prod",
        ]
        with patch.object(sys, "argv", argv):
            with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
                main()
        assert "job_prod" in mock_stdout.getvalue()

    def test_validate_config_rules_engine_unavailable(self, tmp_path):
        """Test that the rule check is skipped when quality_rules is absent."""
        config_data = dict(self.VALID_CONFIG)
        config_data["validation_rules"] = [
            {"type": "not_null", "column": "id"},
            {"type": "in_range", "column": "value", "min": 0, "max": 100},
        ]
        config_path = self._write_config(tmp_path, config_data)

        # Force `from simpleetl.core.quality_rules import ...` to raise
        # ImportError regardless of whether the module exists.
        modules = {"simpleetl.core.quality_rules": None}
        with patch.dict(sys.modules, modules):
            with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
                validate_config_file(config_path)
        assert "Validation rules: 2" in mock_stdout.getvalue()

    def test_validate_config_rules_engine_rejects(self, tmp_path):
        """Test exit non-zero when QualityRuleEngine rejects the rules."""
        import types

        config_data = dict(self.VALID_CONFIG)
        config_data["validation_rules"] = [{"type": "bogus"}]
        config_path = self._write_config(tmp_path, config_data)

        class _RejectingEngine:
            def __init__(self, rules):
                raise ValueError("unknown rule type 'bogus'")

        fake_module = types.ModuleType("simpleetl.core.quality_rules")
        fake_module.QualityRuleEngine = _RejectingEngine  # type: ignore

        modules = {"simpleetl.core.quality_rules": fake_module}
        with patch.dict(sys.modules, modules):
            with patch("sys.stderr", new_callable=StringIO) as mock_stderr:
                with pytest.raises(SystemExit) as excinfo:
                    validate_config_file(config_path)
        assert excinfo.value.code == 1
        assert "Invalid validation_rules" in mock_stderr.getvalue()
        assert "unknown rule type" in mock_stderr.getvalue()

    @pytest.mark.parametrize(
        "extra_args",
        [
            ["--config", "other.yaml"],
            ["--dag", "dag.yaml"],
            ["data.csv", "--profile"],
        ],
    )
    def test_validate_config_mutually_exclusive(self, extra_args):
        """Test --validate-config rejects --config/--dag/--profile."""
        argv = ["simpleetl", "--validate-config", "cfg.yaml"] + extra_args
        with patch.object(sys, "argv", argv):
            with patch("sys.stderr", new_callable=StringIO) as mock_stderr:
                with pytest.raises(SystemExit) as excinfo:
                    main()
        assert excinfo.value.code == 2
        assert "cannot be combined" in mock_stderr.getvalue()


class _DummyJob:
    """Dummy ETL job for testing job_class loading."""

    def __init__(self, config):
        self.config = config

    def run_with_error_handling(self):
        pass
