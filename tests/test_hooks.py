"""
Tests for the hook / interceptor mechanism.

Covers HookContext, HookRegistry (singleton, register, execute, clear,
get_hooks, reset), module-level convenience functions, and the built-in
LoggingHook, MetricsHook, and QualityCheckHook implementations.
"""

import logging
import time
from unittest.mock import MagicMock, patch

import pytest

from simpleetl.core.hooks import (
    ALL_HOOK_POINTS,
    ON_COMPLETE,
    ON_ERROR,
    POST_EXTRACT,
    POST_LOAD,
    POST_TRANSFORM,
    PRE_EXTRACT,
    PRE_LOAD,
    PRE_TRANSFORM,
    Hook,
    HookContext,
    HookRegistry,
    LoggingHook,
    MetricsHook,
    QualityCheckHook,
    execute_hooks,
    get_hook_registry,
    register_hook,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class DummyHook(Hook):
    """Minimal concrete hook for testing the registry."""

    name = "dummy"
    priority = 0

    def __init__(self):
        self.calls = []

    def execute(self, context: HookContext) -> None:
        self.calls.append(context)


@pytest.fixture(autouse=True)
def reset_registry():
    """Reset the singleton HookRegistry before and after each test."""
    reg = HookRegistry()
    reg.reset()
    yield
    reg.reset()


@pytest.fixture
def mock_job():
    """Return a mocked ETLJob with a config.name attribute."""
    job = MagicMock()
    job.config.name = "test_job"
    return job


# ---------------------------------------------------------------------------
# HookContext
# ---------------------------------------------------------------------------


class TestHookContext:
    def test_defaults(self):
        ctx = HookContext()
        assert ctx.job is None
        assert ctx.phase == ""
        assert ctx.data is None
        assert ctx.error is None
        assert ctx.metadata == {}
        assert ctx.start_time == 0.0
        assert ctx.extra == {}

    def test_explicit_values(self, mock_job):
        error = ValueError("boom")
        ctx = HookContext(
            job=mock_job,
            phase=PRE_EXTRACT,
            data={"key": "value"},
            error=error,
            metadata={"foo": "bar"},
            start_time=1234.5,
            extra={"x": 1},
        )
        assert ctx.job is mock_job
        assert ctx.phase == PRE_EXTRACT
        assert ctx.data == {"key": "value"}
        assert ctx.error is error
        assert ctx.metadata == {"foo": "bar"}
        assert ctx.start_time == 1234.5
        assert ctx.extra == {"x": 1}

    def test_metadata_is_independent(self):
        """Each instance should get its own metadata dict."""
        ctx1 = HookContext()
        ctx2 = HookContext()
        ctx1.metadata["only"] = "ctx1"
        assert "only" not in ctx2.metadata


# ---------------------------------------------------------------------------
# HookRegistry singleton
# ---------------------------------------------------------------------------


class TestHookRegistrySingleton:
    def test_same_instance(self):
        a = HookRegistry()
        b = HookRegistry()
        assert a is b

    def test_reset_clears_all(self):
        reg = HookRegistry()
        reg.register(PRE_EXTRACT, DummyHook())
        reg.register(POST_LOAD, DummyHook())
        reg.reset()
        assert reg.get_hooks(PRE_EXTRACT) == []
        assert reg.get_hooks(POST_LOAD) == []


# ---------------------------------------------------------------------------
# HookRegistry.register
# ---------------------------------------------------------------------------


class TestHookRegistryRegister:
    def test_valid_hook_point(self):
        reg = HookRegistry()
        hook = DummyHook()
        reg.register(PRE_EXTRACT, hook)
        assert hook in reg.get_hooks(PRE_EXTRACT)

    def test_invalid_hook_point_raises_value_error(self):
        reg = HookRegistry()
        with pytest.raises(ValueError, match="Unknown hook point 'bad_point'"):
            reg.register("bad_point", DummyHook())

    def test_invalid_hook_point_lists_valid_points(self):
        reg = HookRegistry()
        with pytest.raises(ValueError, match=str(ALL_HOOK_POINTS[0])):
            reg.register("bad_point", DummyHook())

    def test_priority_overrides_hook_attribute(self):
        reg = HookRegistry()
        hook = DummyHook()
        hook.priority = 5
        reg.register(PRE_EXTRACT, hook, priority=42)
        assert hook.priority == 42

    def test_hooks_sorted_by_priority_descending(self):
        reg = HookRegistry()
        low = DummyHook()
        mid = DummyHook()
        high = DummyHook()
        reg.register(PRE_EXTRACT, low, priority=1)
        reg.register(PRE_EXTRACT, high, priority=10)
        reg.register(PRE_EXTRACT, mid, priority=5)
        hooks = reg.get_hooks(PRE_EXTRACT)
        assert hooks[0] is high
        assert hooks[1] is mid
        assert hooks[2] is low

    def test_register_all_hook_points(self):
        reg = HookRegistry()
        for point in ALL_HOOK_POINTS:
            reg.register(point, DummyHook())
        for point in ALL_HOOK_POINTS:
            assert len(reg.get_hooks(point)) == 1


# ---------------------------------------------------------------------------
# HookRegistry.execute
# ---------------------------------------------------------------------------


class TestHookRegistryExecute:
    def test_hooks_execute_in_priority_order(self, mock_job):
        reg = HookRegistry()
        calls = []

        class H1(Hook):
            name = "h1"
            def execute(self, ctx):
                calls.append("h1")

        class H2(Hook):
            name = "h2"
            def execute(self, ctx):
                calls.append("h2")

        reg.register(PRE_EXTRACT, H1(), priority=1)
        reg.register(PRE_EXTRACT, H2(), priority=10)

        ctx = HookContext(job=mock_job, phase=PRE_EXTRACT)
        reg.execute(PRE_EXTRACT, ctx)
        assert calls == ["h2", "h1"]

    def test_no_hooks_is_noop(self, mock_job):
        reg = HookRegistry()
        ctx = HookContext(job=mock_job, phase=PRE_EXTRACT)
        # Should not raise
        reg.execute(PRE_EXTRACT, ctx)

    def test_exception_in_hook_does_not_stop_others(self, mock_job):
        reg = HookRegistry()
        good = DummyHook()

        class BadHook(Hook):
            name = "bad"
            def execute(self, ctx):
                raise RuntimeError("hook failure")

        reg.register(PRE_EXTRACT, BadHook(), priority=5)
        reg.register(PRE_EXTRACT, good, priority=1)

        ctx = HookContext(job=mock_job, phase=PRE_EXTRACT)
        reg.execute(PRE_EXTRACT, ctx)
        assert len(good.calls) == 1

    def test_exception_is_logged(self, mock_job):
        reg = HookRegistry()

        class BadHook(Hook):
            name = "bad"
            def execute(self, ctx):
                raise RuntimeError("hook failure")

        reg.register(PRE_EXTRACT, BadHook())
        ctx = HookContext(job=mock_job, phase=PRE_EXTRACT)

        with patch("simpleetl.core.hooks.logger") as mock_logger:
            reg.execute(PRE_EXTRACT, ctx)
            mock_logger.warning.assert_called_once()
            # The logger uses %s formatting; the exception string is in later args
            all_args = mock_logger.warning.call_args[0]
            full_msg = " ".join(str(a) for a in all_args)
            assert "hook failure" in full_msg

    def test_context_passed_to_hook(self, mock_job):
        reg = HookRegistry()
        hook = DummyHook()
        reg.register(PRE_EXTRACT, hook)

        ctx = HookContext(job=mock_job, phase=PRE_EXTRACT, data=[1, 2, 3])
        reg.execute(PRE_EXTRACT, ctx)
        assert len(hook.calls) == 1
        assert hook.calls[0].data == [1, 2, 3]
        assert hook.calls[0].phase == PRE_EXTRACT


# ---------------------------------------------------------------------------
# HookRegistry.clear
# ---------------------------------------------------------------------------


class TestHookRegistryClear:
    def test_clear_specific_hook_point(self):
        reg = HookRegistry()
        reg.register(PRE_EXTRACT, DummyHook())
        reg.register(POST_EXTRACT, DummyHook())
        reg.clear(PRE_EXTRACT)
        assert reg.get_hooks(PRE_EXTRACT) == []
        assert len(reg.get_hooks(POST_EXTRACT)) == 1

    def test_clear_all(self):
        reg = HookRegistry()
        reg.register(PRE_EXTRACT, DummyHook())
        reg.register(POST_LOAD, DummyHook())
        reg.clear()
        for point in ALL_HOOK_POINTS:
            assert reg.get_hooks(point) == []


# ---------------------------------------------------------------------------
# HookRegistry.get_hooks
# ---------------------------------------------------------------------------


class TestHookRegistryGetHooks:
    def test_returns_copy(self):
        """get_hooks should return a list copy, not the internal list."""
        reg = HookRegistry()
        hook = DummyHook()
        reg.register(PRE_EXTRACT, hook)
        hooks = reg.get_hooks(PRE_EXTRACT)
        hooks.clear()
        assert len(reg.get_hooks(PRE_EXTRACT)) == 1

    def test_empty_for_unregistered(self):
        reg = HookRegistry()
        assert reg.get_hooks(PRE_EXTRACT) == []


# ---------------------------------------------------------------------------
# HookRegistry.reset
# ---------------------------------------------------------------------------


class TestHookRegistryReset:
    def test_reset_clears_everything(self):
        reg = HookRegistry()
        for point in ALL_HOOK_POINTS:
            reg.register(point, DummyHook())
        reg.reset()
        for point in ALL_HOOK_POINTS:
            assert reg.get_hooks(point) == []


# ---------------------------------------------------------------------------
# Module-level convenience functions
# ---------------------------------------------------------------------------


class TestModuleLevelFunctions:
    def test_register_hook(self):
        hook = DummyHook()
        register_hook(PRE_EXTRACT, hook)
        reg = get_hook_registry()
        assert hook in reg.get_hooks(PRE_EXTRACT)

    def test_execute_hooks(self, mock_job):
        hook = DummyHook()
        register_hook(PRE_EXTRACT, hook)
        ctx = HookContext(job=mock_job, phase=PRE_EXTRACT)
        execute_hooks(PRE_EXTRACT, ctx)
        assert len(hook.calls) == 1

    def test_get_hook_registry_returns_singleton(self):
        reg = get_hook_registry()
        assert isinstance(reg, HookRegistry)
        assert reg is HookRegistry()


# ---------------------------------------------------------------------------
# LoggingHook
# ---------------------------------------------------------------------------


class TestLoggingHook:
    def test_basic_execution(self, mock_job):
        hook = LoggingHook()
        ctx = HookContext(job=mock_job, phase=PRE_EXTRACT)
        # Should not raise
        hook.execute(ctx)

    def test_execution_without_job(self):
        hook = LoggingHook()
        ctx = HookContext(job=None, phase=PRE_EXTRACT)
        hook.execute(ctx)

    def test_execution_with_error(self, mock_job):
        hook = LoggingHook()
        error = ValueError("something went wrong")
        ctx = HookContext(job=mock_job, phase=ON_ERROR, error=error)
        hook.execute(ctx)

    def test_logs_at_custom_level(self, mock_job):
        hook = LoggingHook(log_level=logging.DEBUG)
        ctx = HookContext(job=mock_job, phase=PRE_EXTRACT)
        with patch.object(hook._logger, "log") as mock_log:
            hook.execute(ctx)
            mock_log.assert_called_once()
            assert mock_log.call_args[0][0] == logging.DEBUG

    def test_log_message_contains_job_name_and_phase(self, mock_job):
        hook = LoggingHook()
        ctx = HookContext(job=mock_job, phase=POST_LOAD)
        with patch.object(hook._logger, "log") as mock_log:
            hook.execute(ctx)
            msg = mock_log.call_args[0][1]
            assert "test_job" in msg
            assert POST_LOAD in msg

    def test_log_message_contains_error_when_present(self, mock_job):
        hook = LoggingHook()
        error = RuntimeError("boom")
        ctx = HookContext(job=mock_job, phase=ON_ERROR, error=error)
        with patch.object(hook._logger, "log") as mock_log:
            hook.execute(ctx)
            msg = mock_log.call_args[0][1]
            assert "boom" in msg


# ---------------------------------------------------------------------------
# MetricsHook
# ---------------------------------------------------------------------------


class TestMetricsHook:
    def test_pre_phase_stores_start_time(self, mock_job):
        hook = MetricsHook()
        ctx = HookContext(job=mock_job, phase=PRE_EXTRACT, metadata={})
        hook.execute(ctx)
        assert "pre_extract_start" in ctx.metadata

    def test_pre_transform_stores_start_time(self, mock_job):
        hook = MetricsHook()
        ctx = HookContext(job=mock_job, phase=PRE_TRANSFORM, metadata={})
        hook.execute(ctx)
        assert "pre_transform_start" in ctx.metadata

    def test_pre_load_stores_start_time(self, mock_job):
        hook = MetricsHook()
        ctx = HookContext(job=mock_job, phase=PRE_LOAD, metadata={})
        hook.execute(ctx)
        assert "pre_load_start" in ctx.metadata

    def test_post_extract_duration(self, mock_job):
        hook = MetricsHook()
        ctx = HookContext(
            job=mock_job,
            phase=POST_EXTRACT,
            metadata={"pre_extract_start": time.time() - 0.5},
        )
        hook.execute(ctx)
        assert "post_extract_duration" in ctx.metadata
        assert ctx.metadata["post_extract_duration"] >= 0.4

    def test_post_transform_duration(self, mock_job):
        hook = MetricsHook()
        ctx = HookContext(
            job=mock_job,
            phase=POST_TRANSFORM,
            metadata={"pre_transform_start": time.time() - 0.3},
        )
        hook.execute(ctx)
        assert "post_transform_duration" in ctx.metadata

    def test_post_load_duration(self, mock_job):
        hook = MetricsHook()
        ctx = HookContext(
            job=mock_job,
            phase=POST_LOAD,
            metadata={"pre_load_start": time.time() - 0.1},
        )
        hook.execute(ctx)
        assert "post_load_duration" in ctx.metadata

    def test_post_extract_counts_rows(self, mock_job):
        pytest.importorskip("pandas")
        import pandas as pd

        hook = MetricsHook()
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        ctx = HookContext(
            job=mock_job,
            phase=POST_EXTRACT,
            data=df,
            metadata={},
        )
        hook.execute(ctx)
        assert ctx.metadata["extracted_rows"] == 3

    def test_post_load_counts_rows(self, mock_job):
        pytest.importorskip("pandas")
        import pandas as pd

        hook = MetricsHook()
        df = pd.DataFrame({"x": range(10)})
        ctx = HookContext(
            job=mock_job,
            phase=POST_LOAD,
            data=df,
            metadata={},
        )
        hook.execute(ctx)
        assert ctx.metadata["loaded_rows"] == 10

    def test_non_dataframe_data_skipped(self, mock_job):
        hook = MetricsHook()
        ctx = HookContext(
            job=mock_job,
            phase=POST_EXTRACT,
            data=[1, 2, 3],
            metadata={},
        )
        hook.execute(ctx)
        assert "extracted_rows" not in ctx.metadata

    def test_post_extract_non_dataframe_no_loaded_rows(self, mock_job):
        hook = MetricsHook()
        ctx = HookContext(
            job=mock_job,
            phase=POST_LOAD,
            data="some string",
            metadata={},
        )
        hook.execute(ctx)
        assert "loaded_rows" not in ctx.metadata

    def test_no_job_name_defaults_to_unknown(self):
        hook = MetricsHook()
        ctx = HookContext(job=None, phase=PRE_EXTRACT, metadata={})
        # Should not raise
        hook.execute(ctx)

    def test_post_phase_without_pre_start(self, mock_job):
        """If no pre_start in metadata, duration should not be recorded."""
        hook = MetricsHook()
        ctx = HookContext(
            job=mock_job,
            phase=POST_EXTRACT,
            metadata={},
        )
        hook.execute(ctx)
        assert "post_extract_duration" not in ctx.metadata


# ---------------------------------------------------------------------------
# QualityCheckHook
# ---------------------------------------------------------------------------


class TestQualityCheckHook:
    def test_skips_non_applicable_phases(self, mock_job):
        """QualityCheckHook should ignore phases other than POST_EXTRACT/POST_TRANSFORM."""
        pytest.importorskip("pandas")
        import pandas as pd

        hook = QualityCheckHook()
        df = pd.DataFrame({"a": [1]})
        for phase in (PRE_EXTRACT, PRE_TRANSFORM, PRE_LOAD, POST_LOAD, ON_ERROR, ON_COMPLETE):
            ctx = HookContext(job=mock_job, phase=phase, data=df)
            hook.execute(ctx)
        # No quality metadata should have been set
        assert "quality_schema_ok" not in ctx.metadata

    def test_skips_none_data(self, mock_job):
        hook = QualityCheckHook(required_columns=["col_a"])
        ctx = HookContext(job=mock_job, phase=POST_EXTRACT, data=None)
        hook.execute(ctx)
        assert "quality_schema_ok" not in ctx.metadata

    def test_skips_non_dataframe_data(self, mock_job):
        hook = QualityCheckHook(required_columns=["col_a"])
        ctx = HookContext(job=mock_job, phase=POST_EXTRACT, data=[1, 2, 3])
        hook.execute(ctx)
        assert "quality_schema_ok" not in ctx.metadata

    def test_required_columns_all_present(self, mock_job):
        pytest.importorskip("pandas")
        import pandas as pd

        hook = QualityCheckHook(required_columns=["a", "b"])
        df = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
        ctx = HookContext(job=mock_job, phase=POST_EXTRACT, data=df)
        hook.execute(ctx)
        assert ctx.metadata["quality_schema_ok"] is True
        assert "quality_missing_columns" not in ctx.metadata

    def test_required_columns_missing(self, mock_job):
        pytest.importorskip("pandas")
        import pandas as pd

        hook = QualityCheckHook(required_columns=["a", "missing_col"])
        df = pd.DataFrame({"a": [1], "b": [2]})
        ctx = HookContext(job=mock_job, phase=POST_EXTRACT, data=df)
        hook.execute(ctx)
        assert "quality_missing_columns" in ctx.metadata
        assert "missing_col" in ctx.metadata["quality_missing_columns"]
        assert "quality_schema_ok" not in ctx.metadata

    def test_null_fractions(self, mock_job):
        pytest.importorskip("pandas")
        import pandas as pd

        hook = QualityCheckHook()
        df = pd.DataFrame({"a": [1, None, 3], "b": [None, None, 3]})
        ctx = HookContext(job=mock_job, phase=POST_EXTRACT, data=df)
        hook.execute(ctx)
        fractions = ctx.metadata["quality_null_fractions"]
        assert fractions["a"] == pytest.approx(1 / 3)
        assert fractions["b"] == pytest.approx(2 / 3)

    def test_null_fractions_no_nulls(self, mock_job):
        pytest.importorskip("pandas")
        import pandas as pd

        hook = QualityCheckHook()
        df = pd.DataFrame({"x": [1, 2, 3]})
        ctx = HookContext(job=mock_job, phase=POST_EXTRACT, data=df)
        hook.execute(ctx)
        fractions = ctx.metadata["quality_null_fractions"]
        assert fractions["x"] == 0.0

    def test_duplicate_count(self, mock_job):
        pytest.importorskip("pandas")
        import pandas as pd

        hook = QualityCheckHook()
        df = pd.DataFrame({"a": [1, 1, 2, 2, 2]})
        ctx = HookContext(job=mock_job, phase=POST_EXTRACT, data=df)
        hook.execute(ctx)
        assert ctx.metadata["quality_duplicate_count"] == 3

    def test_duplicate_count_no_duplicates(self, mock_job):
        pytest.importorskip("pandas")
        import pandas as pd

        hook = QualityCheckHook()
        df = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
        ctx = HookContext(job=mock_job, phase=POST_EXTRACT, data=df)
        hook.execute(ctx)
        assert ctx.metadata["quality_duplicate_count"] == 0

    def test_post_transform_phase(self, mock_job):
        """QualityCheckHook should also work for POST_TRANSFORM."""
        pytest.importorskip("pandas")
        import pandas as pd

        hook = QualityCheckHook(required_columns=["col_a"])
        df = pd.DataFrame({"col_a": [1, 2]})
        ctx = HookContext(job=mock_job, phase=POST_TRANSFORM, data=df)
        hook.execute(ctx)
        assert ctx.metadata["quality_schema_ok"] is True

    def test_empty_dataframe(self, mock_job):
        """QualityCheckHook should handle an empty DataFrame gracefully."""
        pytest.importorskip("pandas")
        import pandas as pd

        hook = QualityCheckHook(required_columns=["a"])
        df = pd.DataFrame({"a": pd.Series([], dtype="float64")})
        ctx = HookContext(job=mock_job, phase=POST_EXTRACT, data=df)
        hook.execute(ctx)
        assert ctx.metadata["quality_schema_ok"] is True
        assert ctx.metadata["quality_duplicate_count"] == 0

    def test_empty_dataframe_missing_columns(self, mock_job):
        """Empty DataFrame with missing required columns should report them."""
        pytest.importorskip("pandas")
        import pandas as pd

        hook = QualityCheckHook(required_columns=["missing"])
        df = pd.DataFrame({"a": pd.Series([], dtype="float64")})
        ctx = HookContext(job=mock_job, phase=POST_EXTRACT, data=df)
        hook.execute(ctx)
        assert "missing" in ctx.metadata["quality_missing_columns"]

    def test_no_job_name_defaults_to_unknown(self):
        pytest.importorskip("pandas")
        import pandas as pd

        hook = QualityCheckHook()
        df = pd.DataFrame({"a": [1]})
        ctx = HookContext(job=None, phase=POST_EXTRACT, data=df)
        # Should not raise
        hook.execute(ctx)

    def test_custom_thresholds_stored(self):
        hook = QualityCheckHook(
            required_columns=["x"],
            null_threshold=0.5,
            duplicate_threshold=0.1,
        )
        assert hook._null_threshold == 0.5
        assert hook._duplicate_threshold == 0.1
        assert hook._required_columns == ["x"]
