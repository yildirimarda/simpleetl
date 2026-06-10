# Declarative Data Quality Rules

SimpleETL v1.2 adds config-driven data quality validation: declare
expectations directly in the job config and the framework enforces them
after the transform phase — no validation code required.

## Quick Start

### 1. Declare Rules in Your Job Config

```yaml
name: orders_job
input_format: csv
output_format: parquet
validation_rules:
  - type: not_null
    column: order_id
  - type: unique
    column: order_id
  - type: in_range
    column: amount
    min: 0
    max: 1000000
  - type: in_set
    column: status
    values: [new, paid, shipped, cancelled]
  - type: matches_regex
    column: email
    pattern: "^[^@]+@[^@]+\\.[^@]+$"
    severity: warning
  - type: row_count_min
    value: 1
```

When the job runs, a `QualityRuleHook` is registered automatically at the
`post_transform` hook point. Rules with `severity: error` (the default)
abort the job with `QualityRuleError`; `severity: warning` rules log and
continue.

### 2. Programmatic Usage

```python
import pandas as pd
from simpleetl import QualityRuleEngine

engine = QualityRuleEngine([
    {"type": "not_null", "column": "id"},
    {"type": "expression", "expr": "price > 0", "name": "positive_price"},
])
report = engine.evaluate(df)

print(report.passed)      # False if any error-severity rule failed
print(report.summary())   # Human-readable result listing
report.to_dict()          # Serializable form for logging/alerting
```

## Supported Rule Types

| Type | Required keys | Description |
|------|---------------|-------------|
| `not_null` | `column` | No null values in the column |
| `unique` | `column` | No duplicate values in the column |
| `in_range` | `column`, `min` and/or `max` | Numeric bounds check |
| `in_set` | `column`, `values` | Values restricted to an allowlist |
| `matches_regex` | `column`, `pattern` | Strings must match the pattern |
| `min_length` | `column`, `value` | Minimum string length |
| `max_length` | `column`, `value` | Maximum string length |
| `row_count_min` | `value` | Minimum number of rows |
| `row_count_max` | `value` | Maximum number of rows |
| `expression` | `expr` | `DataFrame.eval` predicate that must hold for all rows |

Every rule also accepts:

- `severity`: `error` (default) or `warning`
- `name`: display name used in reports and error messages

Unknown rule types or missing required keys raise `ValueError` when the
job (or engine) is constructed — bad rules fail fast, not mid-pipeline.
A rule referencing a column missing from the DataFrame produces a failed
result with a clear message instead of crashing.

## Validating Rules from the CLI

```bash
simpleetl --validate-config config.yaml
```

This validates the whole config — including `validation_rules` sanity
checks — without running the job.

## Accessing the Report in Hooks

The evaluation report is stored on the hook context before any exception
is raised:

```python
report = context.metadata["quality_rule_report"]
```

`QualityRuleError` also carries the report on its `.report` attribute.
