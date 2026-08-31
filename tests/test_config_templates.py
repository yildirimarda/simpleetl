"""Tests for Jinja2 config template support (Phase 8.1)."""

import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest

pytest.importorskip("jinja2")

from simpleetl.core.config import (
    ConfigTemplateError,
    load_config,
    render_config_template,
)


# ---------------------------------------------------------------------------
# render_config_template
# ---------------------------------------------------------------------------


class TestRenderConfigTemplate:
    def test_basic_variable(self):
        result = render_config_template("Hello {{ name }}!", {"name": "world"})
        assert result == "Hello world!"

    def test_env_namespace(self):
        with patch.dict(os.environ, {"MY_TEST_VAR": "hello"}):
            result = render_config_template("{{ env.MY_TEST_VAR }}")
        assert result == "hello"

    def test_today_variable(self):
        result = render_config_template("{{ today }}")
        import re

        assert re.match(r"\d{4}-\d{2}-\d{2}", result)

    def test_now_variable_strftime(self):
        result = render_config_template("{{ now.strftime('%Y') }}")
        assert len(result) == 4
        assert result.isdigit()

    def test_params_namespace(self):
        result = render_config_template("{{ params.key }}", {"key": "value"})
        assert result == "value"

    def test_no_vars(self):
        result = render_config_template("plain text")
        assert result == "plain text"

    def test_undefined_var_raises(self):
        with pytest.raises(ConfigTemplateError):
            render_config_template("{{ undefined_var }}")

    def test_template_syntax_error_raises(self):
        with pytest.raises(ConfigTemplateError):
            render_config_template("{% invalid %}")

    def test_jinja2_filter(self):
        result = render_config_template("{{ 'hello' | upper }}")
        assert result == "HELLO"

    def test_conditional(self):
        result = render_config_template(
            "{% if mode == 'prod' %}production{% else %}dev{% endif %}",
            {"mode": "prod"},
        )
        assert result == "production"

    def test_missing_jinja2_raises_import_error(self):
        import sys

        original = sys.modules.get("jinja2")
        sys.modules["jinja2"] = None  # type: ignore[assignment]
        try:
            with pytest.raises(ConfigTemplateError, match="jinja2 is required"):
                render_config_template("{{ x }}", {"x": "1"})
        finally:
            if original is not None:
                sys.modules["jinja2"] = original
            else:
                del sys.modules["jinja2"]


# ---------------------------------------------------------------------------
# load_config with template_vars
# ---------------------------------------------------------------------------


def _write_yaml(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


class TestLoadConfigWithTemplates:
    def test_template_vars_injected(self, tmp_path):
        cfg = tmp_path / "job.yaml"
        _write_yaml(
            cfg,
            "name: job_{{ params.run_date }}\n"
            "input_format: csv\n"
            "output_format: parquet\n",
        )
        result = load_config(cfg, template_vars={"run_date": "2024-01-15"})
        assert result.name == "job_2024-01-15"

    def test_env_in_template(self, tmp_path):
        cfg = tmp_path / "job.yaml"
        _write_yaml(
            cfg,
            "name: {{ env.TEST_JOB_NAME }}\n"
            "input_format: csv\n"
            "output_format: parquet\n",
        )
        with patch.dict(os.environ, {"TEST_JOB_NAME": "env-job"}):
            result = load_config(cfg, template_vars={})
        assert result.name == "env-job"

    def test_auto_detect_template_markers(self, tmp_path):
        """load_config should render even without explicit template_vars if {{ is present."""
        cfg = tmp_path / "job.yaml"
        _write_yaml(
            cfg,
            "name: auto_{{ 'detected' }}\ninput_format: csv\noutput_format: parquet\n",
        )
        result = load_config(cfg)
        assert result.name == "auto_detected"

    def test_plain_config_unchanged(self, tmp_path):
        cfg = tmp_path / "job.yaml"
        _write_yaml(
            cfg,
            "name: plain_job\ninput_format: csv\noutput_format: parquet\n",
        )
        result = load_config(cfg)
        assert result.name == "plain_job"

    def test_json_config_with_template(self, tmp_path):
        cfg = tmp_path / "job.json"
        cfg.write_text(
            json.dumps(
                {
                    "name": "{{ params.name }}",
                    "input_format": "csv",
                    "output_format": "parquet",
                }
            )
        )
        result = load_config(cfg, template_vars={"name": "json-job"})
        assert result.name == "json-job"

    def test_today_in_config(self, tmp_path):
        cfg = tmp_path / "job.yaml"
        _write_yaml(
            cfg,
            "name: job_{{ today }}\ninput_format: csv\noutput_format: parquet\n",
        )
        result = load_config(cfg, template_vars={})
        import re

        assert re.match(r"job_\d{4}-\d{2}-\d{2}", result.name)


# ---------------------------------------------------------------------------
# CLI --param parsing helper
# ---------------------------------------------------------------------------


class TestParseParams:
    def test_single_param(self):
        from simpleetl.cli import _parse_params

        result = _parse_params(["key=value"])
        assert result == {"key": "value"}

    def test_multiple_params(self):
        from simpleetl.cli import _parse_params

        result = _parse_params(["a=1", "b=hello world"])
        assert result == {"a": "1", "b": "hello world"}

    def test_empty_list(self):
        from simpleetl.cli import _parse_params

        assert _parse_params([]) == {}

    def test_none_list(self):
        from simpleetl.cli import _parse_params

        assert _parse_params(None) == {}

    def test_invalid_format_raises(self):
        from simpleetl.cli import _parse_params

        with pytest.raises(SystemExit):
            _parse_params(["no-equals-sign"])

    def test_value_with_equals(self):
        from simpleetl.cli import _parse_params

        result = _parse_params(["url=http://x.com?a=b"])
        assert result == {"url": "http://x.com?a=b"}
