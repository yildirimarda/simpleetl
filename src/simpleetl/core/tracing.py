"""
OpenTelemetry tracing support for SimpleETL.

OpenTelemetry is an optional dependency (``simpleetl[otel]``).  All
``opentelemetry`` imports in this module are performed lazily so that the
module can always be imported.  When the SDK is not installed, tracing
degrades to a no-op and a warning is logged once.
"""

import importlib
import logging
from typing import Any, Dict, Optional, TYPE_CHECKING

from simpleetl.core.config import TracingConfig
from simpleetl.core.hooks import (
    Hook,
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

if TYPE_CHECKING:
    from opentelemetry.sdk.trace import TracerProvider

logger = logging.getLogger(__name__)

# Emitted at most once per process when tracing is requested but the
# OpenTelemetry SDK is not installed.
_OTEL_MISSING_WARNED = False


def is_tracing_available() -> bool:
    """Return ``True`` when the OpenTelemetry SDK is importable.

    Returns:
        Whether ``opentelemetry-sdk`` is installed in the current
        environment.
    """
    try:
        import opentelemetry.sdk.trace  # noqa: F401
    except ImportError:
        return False
    return True


def _warn_otel_missing() -> None:
    """Log a warning (once per process) that the OTel SDK is missing."""
    global _OTEL_MISSING_WARNED
    if not _OTEL_MISSING_WARNED:
        logger.warning(
            "Tracing is enabled but the OpenTelemetry SDK is not "
            "installed; tracing is disabled. Install it with: "
            "pip install simpleetl[otel]"
        )
        _OTEL_MISSING_WARNED = True


def setup_tracing(
    config: TracingConfig,
    *,
    exporter: Optional[Any] = None,
) -> Optional["TracerProvider"]:
    """Configure the global OpenTelemetry tracer provider.

    Builds a ``TracerProvider`` with a ``service.name`` resource taken
    from *config* and registers it as the global provider.

    Exporter precedence:

    1. An explicit *exporter* argument (intended for tests); attached via
       a ``SimpleSpanProcessor`` for deterministic, synchronous export.
    2. An OTLP HTTP exporter when ``config.endpoint`` is set and
       ``opentelemetry-exporter-otlp`` is installed.  When the exporter
       package is missing, a warning is logged and the console exporter
       is used instead.
    3. ``ConsoleSpanExporter`` as the default fallback.

    Args:
        config: Tracing configuration.
        exporter: Optional span exporter override (primarily for tests).

    Returns:
        The configured ``TracerProvider``, or ``None`` when tracing is
        disabled or the OpenTelemetry SDK is not installed.
    """
    if not config.enabled:
        return None
    if not is_tracing_available():
        _warn_otel_missing()
        return None

    from opentelemetry import trace
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import (
        BatchSpanProcessor,
        ConsoleSpanExporter,
        SimpleSpanProcessor,
    )

    resource = Resource.create({"service.name": config.service_name})
    provider = TracerProvider(resource=resource)

    if exporter is not None:
        provider.add_span_processor(SimpleSpanProcessor(exporter))
    else:
        span_exporter: Optional[Any] = None
        if config.endpoint:
            try:
                otlp = importlib.import_module(
                    "opentelemetry.exporter.otlp.proto.http.trace_exporter"
                )
            except ImportError:
                logger.warning(
                    "Tracing endpoint '%s' is configured but the OTLP "
                    "exporter is not installed; falling back to console "
                    "export. Install it with: pip install simpleetl[otel]",
                    config.endpoint,
                )
            else:
                span_exporter = otlp.OTLPSpanExporter(endpoint=config.endpoint)
        if span_exporter is None:
            span_exporter = ConsoleSpanExporter()
        provider.add_span_processor(BatchSpanProcessor(span_exporter))

    trace.set_tracer_provider(provider)
    logger.info(
        "OpenTelemetry tracing configured for service '%s'",
        config.service_name,
    )
    return provider


class TracingHook(Hook):
    """Hook that emits OpenTelemetry spans for each ETL job run.

    Maintains one root span per job run (``etl.job <job name>``) plus a
    child span per ETL phase (``etl.extract``, ``etl.transform``,
    ``etl.load``).  The hook is robust to partial or out-of-order
    lifecycles: ``execute`` never raises, missing ``PRE_*`` events make
    the matching ``POST_*`` a no-op, and ``ON_COMPLETE`` / ``ON_ERROR``
    close whatever spans are still open.

    When ``config.enabled`` is ``False`` or the OpenTelemetry SDK is not
    installed (and no *tracer* override is given), ``execute`` is a
    no-op.

    Note:
        Span state is kept on the hook instance, so a single instance is
        **not thread-safe across concurrent jobs**.  Use one
        ``TracingHook`` per job.
    """

    name = "tracing"
    priority = 0

    _PHASE_SPAN_NAMES: Dict[str, str] = {
        PRE_EXTRACT: "etl.extract",
        PRE_TRANSFORM: "etl.transform",
        PRE_LOAD: "etl.load",
    }
    _POST_PHASES = (POST_EXTRACT, POST_TRANSFORM, POST_LOAD)

    def __init__(
        self,
        config: TracingConfig,
        tracer: Optional[Any] = None,
    ) -> None:
        """Initialize the tracing hook.

        Args:
            config: Tracing configuration.
            tracer: Optional tracer override (primarily for tests).  When
                omitted, ``opentelemetry.trace.get_tracer`` is used
                lazily on first span creation.
        """
        self._config = config
        self._tracer = tracer
        self._root_span: Optional[Any] = None
        self._phase_span: Optional[Any] = None
        self._logger = logging.getLogger(f"{__name__}.TracingHook")
        self._enabled = config.enabled and (
            tracer is not None or is_tracing_available()
        )
        if config.enabled and not self._enabled:
            _warn_otel_missing()

    def execute(self, context: HookContext) -> None:
        """Handle a hook point by starting/ending the matching spans.

        Never raises: any internal tracing failure is logged as a
        warning so the ETL job itself is unaffected.

        Args:
            context: The hook context for this invocation.
        """
        if not self._enabled:
            return
        try:
            self._handle(context)
        except Exception as exc:
            self._logger.warning(
                "TracingHook failed during '%s': %s", context.phase, exc
            )

    # -- internal helpers ---------------------------------------------------

    def _handle(self, context: HookContext) -> None:
        """Dispatch the hook context to the right span operation."""
        phase = context.phase
        if phase in self._PHASE_SPAN_NAMES:
            self._start_phase(context, phase)
        elif phase in self._POST_PHASES:
            self._end_phase(context)
        elif phase == ON_ERROR:
            self._handle_error(context)
        elif phase == ON_COMPLETE:
            self._handle_complete()

    def _get_tracer(self) -> Any:
        """Return the tracer, creating one lazily when not injected."""
        if self._tracer is None:
            from opentelemetry import trace

            self._tracer = trace.get_tracer(__name__)
        return self._tracer

    def _start_root(self, context: HookContext) -> Any:
        """Start the root span for the job run and return it."""
        job_name = "unknown"
        platform: Optional[str] = None
        job_config = getattr(context.job, "config", None)
        if job_config is not None:
            job_name = getattr(job_config, "name", "unknown")
            platform = getattr(job_config, "platform", None)

        span = self._get_tracer().start_span(f"etl.job {job_name}")
        span.set_attribute("job.name", job_name)
        if platform is not None:
            span.set_attribute("job.platform", platform)
        self._root_span = span
        return span

    def _start_phase(self, context: HookContext, phase: str) -> None:
        """Start a child span for an ETL phase (``pre_*`` hook points)."""
        from opentelemetry import trace

        root = self._root_span
        if root is None:
            root = self._start_root(context)

        # Defensive: a phase span left open (e.g. double PRE without a
        # POST in between) is closed before starting the next one.
        if self._phase_span is not None:
            self._phase_span.end()
            self._phase_span = None

        parent_context = trace.set_span_in_context(root)
        self._phase_span = self._get_tracer().start_span(
            self._PHASE_SPAN_NAMES[phase], context=parent_context
        )

    def _end_phase(self, context: HookContext) -> None:
        """End the current phase span (``post_*`` hook points)."""
        span = self._phase_span
        if span is None:
            # POST without a matching PRE: nothing to end.
            return
        record_count = self._record_count(context.data)
        if record_count is not None:
            span.set_attribute("records.count", record_count)
        span.end()
        self._phase_span = None

    def _handle_error(self, context: HookContext) -> None:
        """Record the error and close all open spans with error status."""
        from opentelemetry.trace import Status, StatusCode

        description = str(context.error) if context.error else None
        for span in (self._phase_span, self._root_span):
            if span is None:
                continue
            if context.error is not None:
                span.record_exception(context.error)
            span.set_status(Status(StatusCode.ERROR, description))
            span.end()
        self._phase_span = None
        self._root_span = None

    def _handle_complete(self) -> None:
        """Close any open phase span and the root span with OK status."""
        from opentelemetry.trace import Status, StatusCode

        if self._phase_span is not None:
            self._phase_span.end()
            self._phase_span = None
        if self._root_span is not None:
            self._root_span.set_status(Status(StatusCode.OK))
            self._root_span.end()
            self._root_span = None

    @staticmethod
    def _record_count(data: Any) -> Optional[int]:
        """Return ``len(data)`` when *data* is a pandas DataFrame."""
        if data is None:
            return None
        try:
            import pandas as pd
        except ImportError:  # pragma: no cover - pandas is a core dep
            return None
        if isinstance(data, pd.DataFrame):
            return int(len(data))
        return None
