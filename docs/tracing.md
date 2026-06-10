# OpenTelemetry Tracing

SimpleETL v1.2 emits OpenTelemetry traces for ETL jobs: one root span per
job run with child spans for the extract, transform and load phases.

## Installation

```bash
pip install simpleetl[otel]
```

Tracing degrades gracefully: when the OpenTelemetry SDK is not installed
(or tracing is disabled), all tracing code paths are no-ops — jobs run
unchanged.

## Quick Start

### 1. Enable in the Job Config

```yaml
name: orders_job
input_format: csv
output_format: parquet
tracing:
  enabled: true
  service_name: etl-orders
  endpoint: http://otel-collector:4318/v1/traces
```

With `enabled: true`, a `TracingHook` is attached to every lifecycle hook
point. When `endpoint` is set, an OTLP HTTP exporter is configured
automatically (requires `opentelemetry-exporter-otlp`); without an
endpoint, spans go to whatever tracer provider you configured yourself.

### 2. Programmatic Setup

```python
from simpleetl import setup_tracing, TracingHook, is_tracing_available
from simpleetl.core.config import TracingConfig

config = TracingConfig(enabled=True, service_name="etl-orders")
setup_tracing(config)            # console exporter by default
hook = TracingHook(config)       # register on a job or the hook registry
```

For tests, inject your own exporter:

```python
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

exporter = InMemorySpanExporter()
setup_tracing(config, exporter=exporter)
```

## Span Model

| Span | Attributes |
|------|------------|
| `etl.job <name>` (root) | `job.name`, `job.platform` |
| `etl.extract` | `records.count` (when data is a DataFrame) |
| `etl.transform` | `records.count` |
| `etl.load` | `records.count` |

On job failure, the active spans record the exception and are marked with
ERROR status; on completion the root span closes with OK status.

## Notes

- One `TracingHook` instance traces one job at a time — create a hook per
  job; it is not thread-safe across concurrent jobs.
- The hook never raises: tracing problems are logged, never fail a job.
- Exporter precedence in `setup_tracing`: injected `exporter` argument >
  OTLP HTTP (when `endpoint` is set and the exporter package is
  installed) > console exporter.
