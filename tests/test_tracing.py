"""
Tests for the OpenTelemetry tracing module.

Covers is_tracing_available, setup_tracing (injected exporter, OTLP,
console fallback, disabled, missing SDK) and TracingHook (full
lifecycle, error path, partial/out-of-order lifecycles, disabled and
missing-SDK no-op paths).

Spans are captured with an in-memory exporter on a locally constructed
``TracerProvider``; the OpenTelemetry global provider is never mutated
(``trace.set_tracer_provider`` is monkeypatched in setup_tracing tests).
"""

import logging
import sys
import types
from typing import List, Tuple
from unittest.mock import MagicMock

import pandas as pd
import pytest

pytest.importorskip("opentelemetry")
from opentelemetry import trace as otel_trace_api
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)
from opentelemetry.trace import StatusCode

import simpleetl.core.tracing as tracing
from simpleetl.core.config import TracingConfig
from simpleetl.core.hooks import (
    HookContext,
    ON_COMPLETE,
    ON_ERROR,
    POST_EXTRACT,
    POST_LOAD,
    POST_TRANSFORM,
    PRE_EXTRACT,
    PRE_LOAD,
    PRE_TRANSFORM,
)
from simpleetl.core.tracing import (
    TracingHook,
    is_tracing_available,
    setup_tracing,
)

# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def reset_warn_flag(monkeypatch):
    """Reset the warn-once flag so each test sees a fresh module state."""
    monkeypatch.setattr(tracing, "_OTEL_MISSING_WARNED", False)


@pytest.fixture
def memory_tracer() -> Tuple[InMemorySpanExporter, otel_trace_api.Tracer]:
    """Return (exporter, tracer) backed by a local TracerProvider."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return exporter, provider.get_tracer("test_tracing")


@pytest.fixture
def mock_job() -> MagicMock:
    """Return a mocked ETLJob with config.name and config.platform."""
    job = MagicMock()
    job.config.name = "test_job"
    job.config.platform = "local"
    return job


def _ctx(phase: str, job=None, data=None, error=None) -> HookContext:
    return HookContext(job=job, phase=phase, data=data, error=error)


def _span_by_name(spans: List[ReadableSpan], name: str) -> ReadableSpan:
    return next(s for s in spans if s.name == name)


def _single_exporter(provider: TracerProvider):
    """Return the exporter of the provider's single batch processor."""
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    processors = provider._active_span_processor._span_processors
    assert len(processors) == 1
    assert isinstance(processors[0], BatchSpanProcessor)
    return processors[0].span_exporter


# ---------------------------------------------------------------------------
# is_tracing_available
# ---------------------------------------------------------------------------


class TestIsTracingAvailable:
    def test_true_when_sdk_installed(self):
        assert is_tracing_available() is True

    def test_false_when_sdk_missing(self, monkeypatch):
        # A None entry in sys.modules makes the import raise ImportError.
        monkeypatch.setitem(sys.modules, "opentelemetry.sdk.trace", None)
        assert is_tracing_available() is False


# ---------------------------------------------------------------------------
# setup_tracing
# ---------------------------------------------------------------------------


@pytest.fixture
def no_global_provider(monkeypatch) -> List[object]:
    """Prevent global provider mutation; record set_tracer_provider calls."""
    calls: List[object] = []
    monkeypatch.setattr(otel_trace_api, "set_tracer_provider", calls.append)
    return calls


class TestSetupTracing:
    def test_disabled_returns_none(self):
        assert setup_tracing(TracingConfig(enabled=False)) is None

    def test_missing_otel_returns_none_and_warns_once(self, monkeypatch, caplog):
        monkeypatch.setattr(tracing, "is_tracing_available", lambda: False)
        with caplog.at_level(logging.WARNING, logger=tracing.__name__):
            assert setup_tracing(TracingConfig(enabled=True)) is None
            assert setup_tracing(TracingConfig(enabled=True)) is None
        warnings = [r for r in caplog.records if "OpenTelemetry SDK" in r.message]
        assert len(warnings) == 1

    def test_injected_exporter(self, no_global_provider):
        exporter = InMemorySpanExporter()
        config = TracingConfig(enabled=True, service_name="svc-test")
        provider = setup_tracing(config, exporter=exporter)

        assert provider is not None
        assert provider.resource.attributes["service.name"] == "svc-test"
        assert no_global_provider == [provider]

        # SimpleSpanProcessor exports synchronously on span end.
        provider.get_tracer("t").start_span("probe").end()
        assert [s.name for s in exporter.get_finished_spans()] == ["probe"]

    def test_endpoint_with_otlp_installed(self, monkeypatch, no_global_provider):
        created = []

        class FakeOTLPSpanExporter:
            def __init__(self, endpoint=None):
                created.append(endpoint)

            def export(self, spans):  # pragma: no cover - never called
                return None

            def shutdown(self):
                return None

            def force_flush(self, timeout_millis=30000):
                return True

        # Install fake module hierarchy so the lazy OTLP import succeeds.
        names = [
            "opentelemetry.exporter",
            "opentelemetry.exporter.otlp",
            "opentelemetry.exporter.otlp.proto",
            "opentelemetry.exporter.otlp.proto.http",
            "opentelemetry.exporter.otlp.proto.http.trace_exporter",
        ]
        for name in names:
            monkeypatch.setitem(sys.modules, name, types.ModuleType(name))
        sys.modules[names[-1]].OTLPSpanExporter = FakeOTLPSpanExporter

        endpoint = "http://collector:4318/v1/traces"
        provider = setup_tracing(TracingConfig(enabled=True, endpoint=endpoint))
        assert provider is not None
        assert created == [endpoint]
        assert no_global_provider == [provider]
        provider.shutdown()

    def test_endpoint_without_otlp_falls_back_to_console(
        self, monkeypatch, no_global_provider, caplog
    ):
        from opentelemetry.sdk.trace.export import ConsoleSpanExporter

        # Force the OTLP import to fail even if the package is present.
        monkeypatch.setitem(
            sys.modules,
            "opentelemetry.exporter.otlp.proto.http.trace_exporter",
            None,
        )
        config = TracingConfig(enabled=True, endpoint="http://collector:4318/v1/traces")
        with caplog.at_level(logging.WARNING, logger=tracing.__name__):
            provider = setup_tracing(config)
        assert provider is not None
        assert any("falling back to console" in r.message for r in caplog.records)
        assert isinstance(_single_exporter(provider), ConsoleSpanExporter)
        provider.shutdown()

    def test_console_exporter_default(self, no_global_provider):
        from opentelemetry.sdk.trace.export import ConsoleSpanExporter

        provider = setup_tracing(TracingConfig(enabled=True))
        assert provider is not None
        assert isinstance(_single_exporter(provider), ConsoleSpanExporter)
        provider.shutdown()


# ---------------------------------------------------------------------------
# TracingHook
# ---------------------------------------------------------------------------


class TestTracingHookLifecycle:
    def _hook(self, memory_tracer) -> Tuple[InMemorySpanExporter, TracingHook]:
        exporter, tracer = memory_tracer
        hook = TracingHook(TracingConfig(enabled=True), tracer=tracer)
        return exporter, hook

    def test_full_lifecycle(self, memory_tracer, mock_job):
        exporter, hook = self._hook(memory_tracer)
        df3 = pd.DataFrame({"a": [1, 2, 3]})
        df2 = pd.DataFrame({"a": [1, 2]})

        hook.execute(_ctx(PRE_EXTRACT, job=mock_job))
        hook.execute(_ctx(POST_EXTRACT, job=mock_job, data=df3))
        hook.execute(_ctx(PRE_TRANSFORM, job=mock_job))
        hook.execute(_ctx(POST_TRANSFORM, job=mock_job, data=df2))
        hook.execute(_ctx(PRE_LOAD, job=mock_job))
        hook.execute(_ctx(POST_LOAD, job=mock_job, data=df2))
        hook.execute(_ctx(ON_COMPLETE, job=mock_job))

        spans = exporter.get_finished_spans()
        assert len(spans) == 4

        root = _span_by_name(spans, "etl.job test_job")
        assert root.attributes["job.name"] == "test_job"
        assert root.attributes["job.platform"] == "local"
        assert root.status.status_code is StatusCode.OK
        assert root.parent is None

        expected_counts = {
            "etl.extract": 3,
            "etl.transform": 2,
            "etl.load": 2,
        }
        for name, count in expected_counts.items():
            child = _span_by_name(spans, name)
            assert child.parent is not None
            assert child.parent.span_id == root.context.span_id
            assert child.context.trace_id == root.context.trace_id
            assert child.attributes["records.count"] == count

        # All open-span state is cleared after completion.
        assert hook._root_span is None
        assert hook._phase_span is None

    def test_error_path_records_exception_and_status(self, memory_tracer, mock_job):
        exporter, hook = self._hook(memory_tracer)
        error = ValueError("boom")

        hook.execute(_ctx(PRE_EXTRACT, job=mock_job))
        hook.execute(_ctx(ON_ERROR, job=mock_job, error=error))

        spans = exporter.get_finished_spans()
        assert len(spans) == 2

        for name in ("etl.extract", "etl.job test_job"):
            span = _span_by_name(spans, name)
            assert span.status.status_code is StatusCode.ERROR
            assert span.status.description == "boom"
            events = [e for e in span.events if e.name == "exception"]
            assert len(events) == 1
            assert events[0].attributes["exception.type"] == "ValueError"

        assert hook._root_span is None
        assert hook._phase_span is None

    def test_error_after_phase_closed(self, memory_tracer, mock_job):
        exporter, hook = self._hook(memory_tracer)
        hook.execute(_ctx(PRE_EXTRACT, job=mock_job))
        hook.execute(_ctx(POST_EXTRACT, job=mock_job))
        hook.execute(_ctx(ON_ERROR, job=mock_job, error=RuntimeError("x")))

        spans = exporter.get_finished_spans()
        assert len(spans) == 2
        root = _span_by_name(spans, "etl.job test_job")
        assert root.status.status_code is StatusCode.ERROR

    def test_error_without_error_object(self, memory_tracer, mock_job):
        exporter, hook = self._hook(memory_tracer)
        hook.execute(_ctx(PRE_EXTRACT, job=mock_job))
        hook.execute(_ctx(ON_ERROR, job=mock_job, error=None))

        spans = exporter.get_finished_spans()
        assert len(spans) == 2
        for span in spans:
            assert span.status.status_code is StatusCode.ERROR
            assert not [e for e in span.events if e.name == "exception"]


class TestTracingHookRobustness:
    def test_on_complete_alone(self, memory_tracer, mock_job):
        exporter, tracer = memory_tracer
        hook = TracingHook(TracingConfig(enabled=True), tracer=tracer)
        hook.execute(_ctx(ON_COMPLETE, job=mock_job))
        assert exporter.get_finished_spans() == ()

    def test_on_error_alone(self, memory_tracer, mock_job):
        exporter, tracer = memory_tracer
        hook = TracingHook(TracingConfig(enabled=True), tracer=tracer)
        hook.execute(_ctx(ON_ERROR, job=mock_job, error=ValueError("x")))
        assert exporter.get_finished_spans() == ()

    def test_post_without_pre(self, memory_tracer, mock_job):
        exporter, tracer = memory_tracer
        hook = TracingHook(TracingConfig(enabled=True), tracer=tracer)
        hook.execute(_ctx(POST_EXTRACT, job=mock_job, data=pd.DataFrame()))
        assert exporter.get_finished_spans() == ()

    def test_double_post(self, memory_tracer, mock_job):
        exporter, tracer = memory_tracer
        hook = TracingHook(TracingConfig(enabled=True), tracer=tracer)
        hook.execute(_ctx(PRE_EXTRACT, job=mock_job))
        hook.execute(_ctx(POST_EXTRACT, job=mock_job))
        hook.execute(_ctx(POST_EXTRACT, job=mock_job))
        hook.execute(_ctx(ON_COMPLETE, job=mock_job))

        names = [s.name for s in exporter.get_finished_spans()]
        assert names.count("etl.extract") == 1
        assert names.count("etl.job test_job") == 1

    def test_double_pre_closes_dangling_span(self, memory_tracer, mock_job):
        exporter, tracer = memory_tracer
        hook = TracingHook(TracingConfig(enabled=True), tracer=tracer)
        hook.execute(_ctx(PRE_EXTRACT, job=mock_job))
        hook.execute(_ctx(PRE_EXTRACT, job=mock_job))
        hook.execute(_ctx(ON_COMPLETE, job=mock_job))

        names = [s.name for s in exporter.get_finished_spans()]
        assert names.count("etl.extract") == 2
        assert names.count("etl.job test_job") == 1

    def test_unknown_phase_is_ignored(self, memory_tracer, mock_job):
        exporter, tracer = memory_tracer
        hook = TracingHook(TracingConfig(enabled=True), tracer=tracer)
        hook.execute(_ctx("not_a_phase", job=mock_job))
        assert exporter.get_finished_spans() == ()

    def test_job_without_config(self, memory_tracer):
        exporter, tracer = memory_tracer
        hook = TracingHook(TracingConfig(enabled=True), tracer=tracer)
        hook.execute(_ctx(PRE_EXTRACT, job=None))
        hook.execute(_ctx(ON_COMPLETE))

        root = _span_by_name(list(exporter.get_finished_spans()), "etl.job unknown")
        assert root.attributes["job.name"] == "unknown"
        assert "job.platform" not in root.attributes

    def test_record_count_non_dataframe(self, memory_tracer, mock_job):
        exporter, tracer = memory_tracer
        hook = TracingHook(TracingConfig(enabled=True), tracer=tracer)
        hook.execute(_ctx(PRE_EXTRACT, job=mock_job))
        hook.execute(_ctx(POST_EXTRACT, job=mock_job, data=[1, 2, 3]))

        extract = _span_by_name(list(exporter.get_finished_spans()), "etl.extract")
        assert "records.count" not in extract.attributes

    def test_record_count_none_data(self, memory_tracer, mock_job):
        exporter, tracer = memory_tracer
        hook = TracingHook(TracingConfig(enabled=True), tracer=tracer)
        hook.execute(_ctx(PRE_EXTRACT, job=mock_job))
        hook.execute(_ctx(POST_EXTRACT, job=mock_job, data=None))

        extract = _span_by_name(list(exporter.get_finished_spans()), "etl.extract")
        assert "records.count" not in extract.attributes

    def test_execute_never_raises(self, mock_job, caplog):
        broken_tracer = MagicMock()
        broken_tracer.start_span.side_effect = RuntimeError("tracer broke")
        hook = TracingHook(TracingConfig(enabled=True), tracer=broken_tracer)
        with caplog.at_level(logging.WARNING):
            hook.execute(_ctx(PRE_EXTRACT, job=mock_job))
        assert any("TracingHook failed" in r.message for r in caplog.records)


class TestTracingHookNoOpPaths:
    def test_disabled_config_is_noop(self, memory_tracer, mock_job):
        exporter, tracer = memory_tracer
        hook = TracingHook(TracingConfig(enabled=False), tracer=tracer)
        for phase in (PRE_EXTRACT, POST_EXTRACT, ON_ERROR, ON_COMPLETE):
            hook.execute(_ctx(phase, job=mock_job))
        assert exporter.get_finished_spans() == ()
        assert hook._root_span is None

    def test_missing_otel_is_noop_and_warns_once(self, monkeypatch, mock_job, caplog):
        monkeypatch.setattr(tracing, "is_tracing_available", lambda: False)
        with caplog.at_level(logging.WARNING, logger=tracing.__name__):
            hook = TracingHook(TracingConfig(enabled=True))
            another = TracingHook(TracingConfig(enabled=True))
        warnings = [r for r in caplog.records if "OpenTelemetry SDK" in r.message]
        assert len(warnings) == 1

        hook.execute(_ctx(PRE_EXTRACT, job=mock_job))
        another.execute(_ctx(ON_COMPLETE, job=mock_job))
        assert hook._root_span is None
        assert another._root_span is None

    def test_missing_otel_via_sys_modules(self, monkeypatch, mock_job):
        monkeypatch.setitem(sys.modules, "opentelemetry.sdk.trace", None)
        hook = TracingHook(TracingConfig(enabled=True))
        hook.execute(_ctx(PRE_EXTRACT, job=mock_job))
        assert hook._root_span is None

    def test_default_tracer_is_lazily_created(self, mock_job):
        # No tracer override: the hook falls back to trace.get_tracer().
        # The global provider default yields no-op/proxy spans, which the
        # hook must handle without raising.
        hook = TracingHook(TracingConfig(enabled=True))
        hook.execute(_ctx(PRE_EXTRACT, job=mock_job))
        assert hook._tracer is not None
        hook.execute(_ctx(POST_EXTRACT, job=mock_job))
        hook.execute(_ctx(ON_COMPLETE, job=mock_job))
        assert hook._root_span is None
