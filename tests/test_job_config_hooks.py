"""
Tests for config-driven hook wiring in ETLJob.

Covers the ``validation_rules``, ``schema_drift`` and ``tracing`` config
sections that automatically attach hooks to the job lifecycle.
"""

import pandas as pd
import pytest

from simpleetl.core.config import ETLJobConfig
from simpleetl.core.drift import SchemaDriftError, SchemaDriftHook
from simpleetl.core.hooks import (
    ON_COMPLETE,
    POST_EXTRACT,
    POST_TRANSFORM,
    PRE_EXTRACT,
)
from simpleetl.core.job import ETLJob
from simpleetl.core.quality_rules import QualityRuleError, QualityRuleHook
from simpleetl.core.tracing import TracingHook


class PassthroughJob(ETLJob):
    """Minimal job that runs the standard lifecycle on an in-memory frame."""

    def __init__(self, config, data: pd.DataFrame):
        super().__init__(config)
        self.data = data
        self.loaded = None

    def extract(self, **kwargs):
        self._execute_hooks(PRE_EXTRACT)
        self._execute_hooks(POST_EXTRACT, data=self.data)
        return self.data

    def transform(self, data):
        result = data
        self._execute_hooks(POST_TRANSFORM, data=result)
        return result

    def load(self, data):
        self.loaded = data
        self._execute_hooks(ON_COMPLETE, data=data)

    def run(self) -> None:
        self.load(self.transform(self.extract()))


def _config(**overrides):
    base = {
        "name": "config_hooks_job",
        "input_format": "csv",
        "output_format": "csv",
    }
    base.update(overrides)
    return ETLJobConfig(**base)


@pytest.fixture
def frame():
    return pd.DataFrame({"id": [1, 2, 3], "value": [10.0, 20.0, 30.0]})


class TestValidationRulesWiring:
    def test_no_rules_registers_no_hooks(self, frame):
        job = PassthroughJob(_config(), frame)
        assert job._config_hooks == {}

    def test_rules_register_quality_hook_at_post_transform(self, frame):
        config = _config(
            validation_rules=[{"type": "not_null", "column": "id"}]
        )
        job = PassthroughJob(config, frame)
        hooks = job._config_hooks[POST_TRANSFORM]
        assert any(isinstance(h, QualityRuleHook) for h in hooks)

    def test_passing_rules_let_job_complete(self, frame):
        config = _config(
            validation_rules=[
                {"type": "not_null", "column": "id"},
                {"type": "in_range", "column": "value", "min": 0},
            ]
        )
        job = PassthroughJob(config, frame)
        job.run()
        assert job.loaded is not None

    def test_error_rule_failure_aborts_job(self):
        bad = pd.DataFrame({"id": [1, None], "value": [1.0, 2.0]})
        config = _config(
            validation_rules=[{"type": "not_null", "column": "id"}]
        )
        job = PassthroughJob(config, bad)
        with pytest.raises(QualityRuleError):
            job.run()
        assert job.loaded is None

    def test_warning_rule_failure_does_not_abort(self):
        bad = pd.DataFrame({"id": [1, None], "value": [1.0, 2.0]})
        config = _config(
            validation_rules=[
                {"type": "not_null", "column": "id", "severity": "warning"}
            ]
        )
        job = PassthroughJob(config, bad)
        job.run()
        assert job.loaded is not None

    def test_invalid_rule_fails_at_job_construction(self, frame):
        config = _config(validation_rules=[{"type": "no_such_rule"}])
        with pytest.raises(ValueError):
            PassthroughJob(config, frame)


class TestSchemaDriftWiring:
    def test_disabled_by_default(self, frame):
        job = PassthroughJob(_config(), frame)
        assert POST_EXTRACT not in job._config_hooks

    def test_enabled_registers_drift_hook(self, frame, tmp_path):
        config = _config(
            schema_drift={
                "enabled": True,
                "registry_path": str(tmp_path / "registry"),
            }
        )
        job = PassthroughJob(config, frame)
        hooks = job._config_hooks[POST_EXTRACT]
        assert any(isinstance(h, SchemaDriftHook) for h in hooks)

    def test_first_run_registers_baseline_then_drift_fails(self, frame, tmp_path):
        config = _config(
            schema_drift={
                "enabled": True,
                "registry_path": str(tmp_path / "registry"),
                "on_drift": "fail",
            }
        )
        job = PassthroughJob(config, frame)
        job.run()  # First run registers the baseline schema.
        assert job.loaded is not None

        drifted = frame.rename(columns={"value": "amount"})
        job2 = PassthroughJob(config, drifted)
        with pytest.raises(SchemaDriftError):
            job2.run()

    def test_warn_action_does_not_abort_on_drift(self, frame, tmp_path):
        config = _config(
            schema_drift={
                "enabled": True,
                "registry_path": str(tmp_path / "registry"),
                "on_drift": "warn",
            }
        )
        PassthroughJob(config, frame).run()
        drifted = frame.rename(columns={"value": "amount"})
        job = PassthroughJob(config, drifted)
        job.run()
        assert job.loaded is not None


class TestTracingWiring:
    def test_disabled_by_default(self, frame):
        job = PassthroughJob(_config(), frame)
        for hooks in job._config_hooks.values():
            assert not any(isinstance(h, TracingHook) for h in hooks)

    def test_enabled_registers_tracing_hook_on_all_points(self, frame):
        config = _config(tracing={"enabled": True})
        job = PassthroughJob(config, frame)
        assert all(
            any(isinstance(h, TracingHook) for h in hooks)
            for hooks in job._config_hooks.values()
        )
        # The same hook instance is shared across all points.
        instances = {
            id(h)
            for hooks in job._config_hooks.values()
            for h in hooks
            if isinstance(h, TracingHook)
        }
        assert len(instances) == 1

    def test_traced_job_runs_to_completion(self, frame):
        config = _config(tracing={"enabled": True})
        job = PassthroughJob(config, frame)
        job.run()
        assert job.loaded is not None


class TestCombinedWiring:
    def test_all_three_sections_together(self, frame, tmp_path):
        config = _config(
            validation_rules=[{"type": "not_null", "column": "id"}],
            schema_drift={
                "enabled": True,
                "registry_path": str(tmp_path / "registry"),
            },
            tracing={"enabled": True},
        )
        job = PassthroughJob(config, frame)
        job.run()
        assert job.loaded is not None
        assert POST_TRANSFORM in job._config_hooks
        assert POST_EXTRACT in job._config_hooks
