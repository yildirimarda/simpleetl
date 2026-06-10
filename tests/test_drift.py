"""
Tests for schema drift detection (drift.py).

Covers: DriftReport (to_dict, summary), SchemaDriftDetector (baseline
registration, no-drift runs, fail/warn/evolve policies, invalid policy),
and SchemaDriftHook (metadata storage, non-DataFrame handling, schema
name resolution).
"""

import logging
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock

import pandas as pd
import pytest

from simpleetl.core.config import SchemaDriftConfig
from simpleetl.core.drift import (
    DriftReport,
    SchemaDriftDetector,
    SchemaDriftError,
    SchemaDriftHook,
)
from simpleetl.core.hooks import (
    POST_EXTRACT,
    HookContext,
    HookRegistry,
    execute_hooks,
    register_hook,
)
from simpleetl.core.schema import Schema, SchemaDiff
from simpleetl.core.schema_registry import FileSchemaRegistry


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


def make_config(tmp_path: Path, **overrides: Any) -> SchemaDriftConfig:
    """Build a SchemaDriftConfig rooted in a temporary directory."""
    params: Dict[str, Any] = {
        "enabled": True,
        "registry_path": str(tmp_path / "registry"),
    }
    params.update(overrides)
    return SchemaDriftConfig(**params)


@pytest.fixture
def base_df() -> pd.DataFrame:
    return pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})


@pytest.fixture
def added_column_df() -> pd.DataFrame:
    return pd.DataFrame(
        {"id": [1, 2], "name": ["a", "b"], "email": ["x@y", "z@y"]}
    )


@pytest.fixture
def mock_job() -> MagicMock:
    job = MagicMock()
    job.config.name = "mock_job"
    return job


@pytest.fixture(autouse=True)
def reset_hook_registry():
    """Reset the singleton HookRegistry before and after each test."""
    reg = HookRegistry()
    reg.reset()
    yield
    reg.reset()


# ---------------------------------------------------------------------------
# DriftReport
# ---------------------------------------------------------------------------


class TestDriftReport:
    def test_defaults(self):
        report = DriftReport(schema_name="users", drifted=False)
        assert report.baseline_version is None
        assert report.diff is None
        assert report.action_taken == "none"

    def test_to_dict_without_diff(self):
        report = DriftReport(
            schema_name="users",
            drifted=False,
            baseline_version=2,
            action_taken="none",
        )
        d = report.to_dict()
        assert d == {
            "schema_name": "users",
            "drifted": False,
            "baseline_version": 2,
            "diff": None,
            "action_taken": "none",
        }

    def test_to_dict_with_diff(self):
        diff = SchemaDiff(added_columns=["email"])
        report = DriftReport(
            schema_name="users",
            drifted=True,
            baseline_version=1,
            diff=diff,
            action_taken="warned",
        )
        d = report.to_dict()
        assert d["diff"] == diff.to_dict()
        assert d["drifted"] is True

    def test_summary_no_drift(self):
        report = DriftReport(
            schema_name="users", drifted=False, action_taken="none"
        )
        summary = report.summary()
        assert "users" in summary
        assert "no drift" in summary

    def test_summary_with_drift_details(self):
        diff = SchemaDiff(
            added_columns=["email"],
            removed_columns=["age"],
            type_changes={"id": {"old": "int64", "new": "object"}},
            nullability_changes={"name": {"old": False, "new": True}},
        )
        report = DriftReport(
            schema_name="users",
            drifted=True,
            baseline_version=3,
            diff=diff,
            action_taken="warned",
        )
        summary = report.summary()
        assert "drifted from baseline version 3" in summary
        assert "email" in summary
        assert "age" in summary
        assert "int64 -> object" in summary
        assert "nullability change 'name'" in summary

    def test_summary_drifted_without_diff(self):
        report = DriftReport(
            schema_name="users",
            drifted=True,
            baseline_version=1,
            diff=None,
            action_taken="warned",
        )
        summary = report.summary()
        assert "drifted from baseline version 1" in summary


# ---------------------------------------------------------------------------
# SchemaDriftDetector construction
# ---------------------------------------------------------------------------


class TestDetectorInit:
    def test_invalid_on_drift_raises(self, tmp_path):
        config = make_config(tmp_path, on_drift="explode")
        with pytest.raises(ValueError, match="Invalid on_drift"):
            SchemaDriftDetector(config)

    def test_builds_file_registry_at_config_path(self, tmp_path):
        config = make_config(tmp_path)
        detector = SchemaDriftDetector(config)
        assert isinstance(detector.registry, FileSchemaRegistry)
        assert (tmp_path / "registry").exists()

    def test_uses_injected_registry(self, tmp_path):
        injected = FileSchemaRegistry(tmp_path / "custom")
        config = make_config(tmp_path)
        detector = SchemaDriftDetector(config, registry=injected)
        assert detector.registry is injected


# ---------------------------------------------------------------------------
# First run / baseline registration
# ---------------------------------------------------------------------------


class TestFirstRun:
    def test_baseline_registered(self, tmp_path, base_df):
        config = make_config(tmp_path)
        detector = SchemaDriftDetector(config)
        report = detector.check(base_df, "users")

        assert report.drifted is False
        assert report.action_taken == "baseline_registered"
        assert report.baseline_version is None
        assert report.diff is None
        assert detector.registry.list_versions("users") == [1]

        stored = detector.registry.get_schema("users", 1)
        assert stored.column_names == ["id", "name"]

    def test_auto_register_disabled(self, tmp_path, base_df):
        config = make_config(tmp_path, auto_register=False)
        detector = SchemaDriftDetector(config)
        report = detector.check(base_df, "users")

        assert report.drifted is False
        assert report.action_taken == "none"
        assert detector.registry.list_schemas() == []

    def test_empty_version_dir_treated_as_no_baseline(
        self, tmp_path, base_df
    ):
        config = make_config(tmp_path)
        detector = SchemaDriftDetector(config)
        # Create an empty schema directory: list_versions returns []
        (tmp_path / "registry" / "users").mkdir(parents=True)
        report = detector.check(base_df, "users")
        assert report.action_taken == "baseline_registered"


# ---------------------------------------------------------------------------
# No drift on subsequent runs
# ---------------------------------------------------------------------------


class TestNoDrift:
    def test_identical_schema_second_run(self, tmp_path, base_df):
        config = make_config(tmp_path)
        detector = SchemaDriftDetector(config)
        detector.check(base_df, "users")

        report = detector.check(base_df.copy(), "users")
        assert report.drifted is False
        assert report.action_taken == "none"
        assert report.baseline_version == 1
        assert report.diff is None
        assert detector.registry.list_versions("users") == [1]


# ---------------------------------------------------------------------------
# Drift policies
# ---------------------------------------------------------------------------


class TestDriftWarn:
    def test_warn_logs_and_returns_report(
        self, tmp_path, base_df, added_column_df, caplog
    ):
        config = make_config(tmp_path, on_drift="warn")
        detector = SchemaDriftDetector(config)
        detector.check(base_df, "users")

        with caplog.at_level(logging.WARNING, logger="simpleetl.core.drift"):
            report = detector.check(added_column_df, "users")

        assert report.drifted is True
        assert report.action_taken == "warned"
        assert report.baseline_version == 1
        assert report.diff is not None
        assert report.diff.added_columns == ["email"]
        assert "Schema drift detected" in caplog.text
        # Warn must not register a new version
        assert detector.registry.list_versions("users") == [1]

    def test_warn_detects_removed_column(self, tmp_path, base_df):
        config = make_config(tmp_path, on_drift="warn")
        detector = SchemaDriftDetector(config)
        detector.check(base_df, "users")

        report = detector.check(base_df[["id"]], "users")
        assert report.drifted is True
        assert report.diff is not None
        assert report.diff.removed_columns == ["name"]

    def test_warn_detects_nullability_change(self, tmp_path, base_df):
        config = make_config(tmp_path, on_drift="warn")
        detector = SchemaDriftDetector(config)
        detector.check(base_df, "users")

        nullable_df = pd.DataFrame({"id": [1, 2], "name": [None, "b"]})
        report = detector.check(nullable_df, "users")
        assert report.drifted is True
        assert report.diff is not None
        assert "name" in report.diff.nullability_changes


class TestDriftFail:
    def test_fail_raises_with_summary(
        self, tmp_path, base_df, added_column_df
    ):
        config = make_config(tmp_path, on_drift="fail")
        detector = SchemaDriftDetector(config)
        detector.check(base_df, "users")

        with pytest.raises(SchemaDriftError, match="email") as exc_info:
            detector.check(added_column_df, "users")

        assert exc_info.value.diff is not None
        assert exc_info.value.diff.added_columns == ["email"]
        # Fail must not register a new version
        assert detector.registry.list_versions("users") == [1]


class TestDriftEvolve:
    def test_evolve_registers_new_version(
        self, tmp_path, base_df, added_column_df
    ):
        config = make_config(tmp_path, on_drift="evolve")
        detector = SchemaDriftDetector(config)
        detector.check(base_df, "users")

        report = detector.check(added_column_df, "users")
        assert report.drifted is True
        assert report.action_taken == "evolved"
        assert report.baseline_version == 1
        assert detector.registry.list_versions("users") == [1, 2]

        evolved = detector.registry.get_schema("users", 2)
        assert "email" in evolved.column_names

    def test_evolve_applies_type_changes(self, tmp_path, base_df):
        config = make_config(tmp_path, on_drift="evolve")
        detector = SchemaDriftDetector(config)
        detector.check(base_df, "users")

        changed_df = pd.DataFrame({"id": ["x", "y"], "name": ["a", "b"]})
        report = detector.check(changed_df, "users")
        assert report.action_taken == "evolved"

        evolved = detector.registry.get_schema("users", 2)
        id_col = evolved.get_column("id")
        assert id_col is not None
        assert id_col.dtype == str(changed_df["id"].dtype)
        assert id_col.dtype != "int64"

    def test_no_drift_after_evolve(
        self, tmp_path, base_df, added_column_df
    ):
        config = make_config(tmp_path, on_drift="evolve")
        detector = SchemaDriftDetector(config)
        detector.check(base_df, "users")
        detector.check(added_column_df, "users")

        report = detector.check(added_column_df.copy(), "users")
        assert report.drifted is False
        assert report.baseline_version == 2
        assert detector.registry.list_versions("users") == [1, 2]


# ---------------------------------------------------------------------------
# SchemaDriftHook
# ---------------------------------------------------------------------------


class TestSchemaDriftHook:
    def test_stores_report_in_metadata(self, tmp_path, base_df, mock_job):
        config = make_config(tmp_path)
        hook = SchemaDriftHook(config)
        context = HookContext(
            job=mock_job, phase=POST_EXTRACT, data=base_df
        )
        hook.execute(context)

        report = context.metadata["schema_drift_report"]
        assert isinstance(report, DriftReport)
        assert report.schema_name == "mock_job"
        assert report.action_taken == "baseline_registered"

    def test_ignores_non_dataframe_data(self, tmp_path, mock_job):
        config = make_config(tmp_path)
        hook = SchemaDriftHook(config)
        context = HookContext(
            job=mock_job, phase=POST_EXTRACT, data={"not": "a df"}
        )
        hook.execute(context)
        assert "schema_drift_report" not in context.metadata

    def test_ignores_none_data(self, tmp_path, mock_job):
        config = make_config(tmp_path)
        hook = SchemaDriftHook(config)
        context = HookContext(job=mock_job, phase=POST_EXTRACT, data=None)
        hook.execute(context)
        assert "schema_drift_report" not in context.metadata

    def test_config_schema_name_takes_precedence(
        self, tmp_path, base_df, mock_job
    ):
        config = make_config(tmp_path, schema_name="configured")
        hook = SchemaDriftHook(config)
        context = HookContext(
            job=mock_job, phase=POST_EXTRACT, data=base_df
        )
        hook.execute(context)
        assert context.metadata["schema_drift_report"].schema_name == (
            "configured"
        )

    def test_constructor_override_beats_config(
        self, tmp_path, base_df, mock_job
    ):
        config = make_config(tmp_path, schema_name="configured")
        hook = SchemaDriftHook(config, schema_name="override")
        context = HookContext(
            job=mock_job, phase=POST_EXTRACT, data=base_df
        )
        hook.execute(context)
        assert context.metadata["schema_drift_report"].schema_name == (
            "override"
        )

    def test_fallback_to_default_without_job(self, tmp_path, base_df):
        config = make_config(tmp_path)
        hook = SchemaDriftHook(config)
        context = HookContext(job=None, phase=POST_EXTRACT, data=base_df)
        hook.execute(context)
        assert context.metadata["schema_drift_report"].schema_name == (
            "default"
        )

    def test_fallback_to_default_with_nameless_job(
        self, tmp_path, base_df
    ):
        class NamelessJob:
            config = None

        config = make_config(tmp_path)
        hook = SchemaDriftHook(config)
        context = HookContext(
            job=NamelessJob(),  # type: ignore[arg-type]
            phase=POST_EXTRACT,
            data=base_df,
        )
        hook.execute(context)
        assert context.metadata["schema_drift_report"].schema_name == (
            "default"
        )

    def test_invalid_on_drift_raises_at_construction(self, tmp_path):
        config = make_config(tmp_path, on_drift="bogus")
        with pytest.raises(ValueError, match="Invalid on_drift"):
            SchemaDriftHook(config)

    def test_fail_policy_propagates_from_execute(
        self, tmp_path, base_df, added_column_df, mock_job
    ):
        config = make_config(tmp_path, on_drift="fail")
        hook = SchemaDriftHook(config)
        first = HookContext(job=mock_job, phase=POST_EXTRACT, data=base_df)
        hook.execute(first)

        second = HookContext(
            job=mock_job, phase=POST_EXTRACT, data=added_column_df
        )
        with pytest.raises(SchemaDriftError):
            hook.execute(second)

    def test_uses_injected_registry(self, tmp_path, base_df, mock_job):
        injected = FileSchemaRegistry(tmp_path / "custom")
        config = make_config(tmp_path)
        hook = SchemaDriftHook(config, registry=injected)
        context = HookContext(
            job=mock_job, phase=POST_EXTRACT, data=base_df
        )
        hook.execute(context)
        assert injected.list_versions("mock_job") == [1]

    def test_registered_in_hook_registry(
        self, tmp_path, base_df, mock_job
    ):
        config = make_config(tmp_path)
        hook = SchemaDriftHook(config)
        register_hook(POST_EXTRACT, hook)

        context = HookContext(
            job=mock_job, phase=POST_EXTRACT, data=base_df
        )
        execute_hooks(POST_EXTRACT, context)

        report = context.metadata["schema_drift_report"]
        assert report.action_taken == "baseline_registered"


# ---------------------------------------------------------------------------
# End-to-end detector flow
# ---------------------------------------------------------------------------


class TestEndToEnd:
    def test_baseline_then_drift_then_evolve_round_trip(self, tmp_path):
        config = make_config(tmp_path, on_drift="evolve")
        detector = SchemaDriftDetector(config)

        df_v1 = pd.DataFrame({"id": [1], "name": ["a"]})
        df_v2 = pd.DataFrame({"id": [1], "name": ["a"], "score": [0.5]})

        first = detector.check(df_v1, "events")
        assert first.action_taken == "baseline_registered"

        second = detector.check(df_v2, "events")
        assert second.action_taken == "evolved"
        assert second.to_dict()["diff"]["added_columns"] == ["score"]

        latest = detector.registry.get_latest_schema("events")
        assert isinstance(latest, Schema)
        assert latest.column_names == ["id", "name", "score"]
