# OpenLineage Integration

SimpleETL integrates with [OpenLineage](https://openlineage.io/) to provide
standardized data lineage tracking compatible with metadata platforms like
Marquez, DataHub, and OpenMetadata.

## Overview

OpenLineage is an open framework for collecting lineage metadata from data
pipelines. SimpleETL converts its internal lineage events into the OpenLineage
RunEvent format and emits them via HTTP POST.

## Quick Start

### 1. Configure OpenLineage in Your Job Config

```yaml
name: my_etl_job
input_format: csv
output_format: parquet
openlineage_url: "http://marquez:5000/api/v1/lineage"
openlineage_namespace: "production"
```

### 2. Programmatic Configuration

```python
from simpleetl.core.lineage import configure_openlineage
from simpleetl.core.config import load_config
from simpleetl.core.job import ETLJob

# Configure the global OpenLineage converter
configure_openlineage(
    url="http://marquez:5000/api/v1/lineage",
    namespace="production",
)

config = load_config("job.yaml")
job = ETLJob(config)
job.run()

# Emit all recorded events
job.lineage_tracker.emit_openlineage(config.openlineage_url)
```

### 3. Custom Converter

```python
from simpleetl.core.lineage import OpenLineageConverter, get_lineage_tracker

converter = OpenLineageConverter(
    namespace="my-company",
    producer="simpleetl/1.0.0",
)

tracker = get_lineage_tracker()
emitted = tracker.emit_openlineage(
    url="http://marquez:5000/api/v1/lineage",
    converter=converter,
)
print(f"Emitted {emitted} events")
```

## OpenLineageConverter

The `OpenLineageConverter` transforms SimpleETL `LineageEvent` objects into
the OpenLineage RunEvent JSON format.

### Supported Fields

| OpenLineage Field | Source |
|-------------------|--------|
| `producer` | Converter config |
| `eventType` | Always `"COMPLETE"` (one event per completed phase) |
| `eventTime` | `LineageEvent.timestamp` |
| `run.runId` | `LineageEvent.event_id` |
| `job.namespace` | Converter config or `"simpleetl"` |
| `job.name` | `LineageEvent.job_name` |
| `inputs[]` | `LineageEvent.source` + `input_schema` |
| `outputs[]` | `LineageEvent.destination` + `output_schema` |
| `nominalTime` facet | `LineageEvent.timestamp` |
| `simpleetl` custom facet | All event metadata (phase, rows, duration, etc.) |

### Events Produced

For a typical ETL job with three phases (extract, transform, load), three
OpenLineage RunEvents are produced -- one per phase. Each event contains:

- **Inputs**: The data source and its schema
- **Outputs**: The data destination and its schema
- **Custom facet**: SimpleETL-specific metadata (rows processed, duration, etc.)

## Error Handling

The `emit_openlineage()` method is designed to be resilient:

- Network errors are caught and logged (never raised)
- Non-2xx HTTP responses are logged with status codes
- Partial success is reported (emitted count vs total events)
- Timeout defaults to 10 seconds per event

## Integration with Marquez

To integrate with [Marquez](https://marquezproject.ai/):

1. Set `openlineage_url` to your Marquez API endpoint:
   ```
   openlineage_url: "http://marquez:5000/api/v1/lineage"
   ```
2. Set `openlineage_namespace` to organize jobs in Marquez
3. Events will appear in the Marquez web UI under the configured namespace

## Integration with DataHub

For [DataHub](https://datahubproject.io/) integration via its OpenLineage
compatible API:

```
openlineage_url: "http://datahub-gms:8080/openlineage/api/v1/lineage"
openlineage_namespace: "my-data-platform"
```

## Schema Facets

When input or output schemas are available, they are included as
`SchemaDatasetFacet` in the dataset objects:

```json
{
  "namespace": "production",
  "name": "s3://bucket/data.csv",
  "facets": {
    "schema": {
      "fields": [
        {"name": "id", "type": "int64"},
        {"name": "name", "type": "object"},
        {"name": "age", "type": "int64"}
      ]
    }
  }
}
```

## Configuration Reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `openlineage_url` | `Optional[str]` | `None` | OpenLineage HTTP endpoint URL |
| `openlineage_namespace` | `str` | `"simpleetl"` | Namespace for datasets and jobs |
