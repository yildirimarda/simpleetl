# Schema Drift Detection

SimpleETL v1.2 detects schema drift between job runs automatically: the
schema of extracted data is compared against the latest registered
baseline in a schema registry, and the job reacts according to policy.

## Quick Start

```yaml
name: orders_job
input_format: csv
output_format: parquet
schema_drift:
  enabled: true
  registry_path: .simpleetl/schema_registry
  on_drift: fail        # fail | warn | evolve
  auto_register: true
```

With `enabled: true`, a `SchemaDriftHook` is registered automatically at
the `post_extract` hook point:

1. **First run** — the inferred schema is registered as the baseline
   (version 1) when `auto_register` is true.
2. **Subsequent runs** — the current schema is diffed against the latest
   registered version. No changes: the job continues silently.
3. **Drift detected** — behavior depends on `on_drift`:
   - `fail`: raise `SchemaDriftError` with a summary of the changes
   - `warn`: log a warning and continue
   - `evolve`: register the evolved schema as a new version and continue

## Configuration Reference

| Key | Default | Description |
|-----|---------|-------------|
| `enabled` | `false` | Turn drift detection on |
| `registry_path` | `.simpleetl/schema_registry` | Directory for the file-based schema registry |
| `schema_name` | job name | Registry key used for the schema |
| `on_drift` | `warn` | Action on drift: `fail`, `warn`, or `evolve` |
| `auto_register` | `true` | Register the baseline on first run |

## Programmatic Usage

```python
from simpleetl import SchemaDriftDetector, SchemaDriftConfig

detector = SchemaDriftDetector(
    SchemaDriftConfig(enabled=True, registry_path="/tmp/registry",
                      on_drift="evolve")
)
report = detector.check(df, schema_name="orders")

print(report.drifted)        # True/False
print(report.action_taken)   # baseline_registered / none / warned / evolved
print(report.summary())      # Added/removed/type-changed columns
```

The `DriftReport` includes the underlying `SchemaDiff` (added columns,
removed columns, type changes, nullability changes) and serializes via
`to_dict()` for logging or alerting.

## Accessing the Report in Hooks

```python
report = context.metadata["schema_drift_report"]
```

## Notes

- Drift detection builds on the existing `Schema.from_dataframe()`,
  `Schema.diff()` and `FileSchemaRegistry` primitives — registered
  versions are plain JSON files you can inspect and check into VCS.
- `evolve` uses `Schema.evolve()` with type and nullability changes
  allowed, guaranteeing the next run sees no drift.
