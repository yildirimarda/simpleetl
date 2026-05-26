# Per-Record Provenance

SimpleETL provides per-record provenance tracking, allowing you to trace the
complete transformation history of individual records through an ETL pipeline.

## Overview

Each record can be tagged with a unique identifier. As the record passes through
extract, transform, and load phases, the provenance tracker records which
transformations were applied. This is useful for:

- **Debugging**: Identify exactly which transformation caused an issue
- **Compliance**: Prove which records were processed and how
- **Auditing**: Track the full lifecycle of sensitive data
- **Data quality**: Understand the lineage of individual rows

## Quick Start

### 1. Using the Configuration API

```python
from simpleetl.core.lineage import ProvenanceHook
from simpleetl.core.job import ETLJob

job = ETLJob(config)

# Add provenance tracking (assumes records have an "id" column)
provenance_hook = ProvenanceHook(record_id_column="id")
job.add_hook(provenance_hook)

job.run()

# Get provenance for a specific record
history = job.lineage_tracker.get_provenance("user_123")
print(history)
# Output: ['post_extract', 'post_transform', 'post_load']
```

### 2. Custom Record ID Column

If your records use a different identifier column:

```python
hook = ProvenanceHook(record_id_column="customer_id")
job.add_hook(hook)
```

### 3. Direct API Usage

```python
from simpleetl.core.lineage import (
    get_lineage_tracker,
    ProvenanceTracker,
)

# Use the global tracker
tracker = get_lineage_tracker()
tracker.record_provenance("record_001", "filter:age>18")
tracker.record_provenance("record_001", "map:upper(name)")

print(tracker.get_provenance("record_001"))
# Output: ['filter:age>18', 'map:upper(name)']

# Get all provenance data
all_prov = tracker.get_all_provenance()
```

## ProvenanceTracker

The `ProvenanceTracker` is a standalone class optimized for efficient per-record
tracking with O(1) lookups:

```python
from simpleetl.core.lineage import ProvenanceTracker

tracker = ProvenanceTracker()
tracker.track("rec_001", "extract")
tracker.track("rec_001", "filter:active=true")
tracker.track("rec_001", "transform:normalize")

# Get the transformation chain
chain = tracker.get("rec_001")
# Returns: ["extract", "filter:active=true", "transform:normalize"]

# Serialization
data = tracker.to_dict()
restored = ProvenanceTracker.from_dict(data)
```

## ProvenanceHook

The `ProvenanceHook` is a `Hook` subclass that automatically records provenance
during ETL execution:

- **POST_EXTRACT**: Records each extracted record's ID
- **POST_TRANSFORM**: Records each transformed record's ID
- **POST_LOAD**: Records each loaded record's ID

### Initialization

```python
class ProvenanceHook(Hook):
    name = "provenance"

    def __init__(
        self,
        record_id_column: str = "id",
        tracker: Optional[LineageTracker] = None,
    ) -> None:
        ...
```

### Supported Data Formats

The hook extracts record IDs from:

- **pandas DataFrames**: Looks for the `record_id_column` in the DataFrame columns
- **Lists of dicts**: Looks for the `record_id_column` key in each dict

## LineageTracker Provenance Methods

The `LineageTracker` class provides provenance methods that work alongside
the standard event tracking:

```python
# Record provenance for a specific record
tracker.record_provenance(
    record_id="user_42",
    transformation="filter:age>18",
    event_id="optional-event-id",  # optional: link to a specific event
)

# Get provenance for a single record
transformations = tracker.get_provenance("user_42")

# Get all provenance data
all_data = tracker.get_all_provenance()
# Returns: {"user_42": ["filter:age>18", ...], ...}
```

## LineageEvent.record_provenance

Each `LineageEvent` includes a `record_provenance` field that maps record IDs
to their transformation chains at that specific point in time:

```python
event = tracker.get_events("my_job")[0]
print(event.record_provenance)
# {"rec_001": ["post_extract"], "rec_002": ["post_extract"], ...}
```

This field is automatically included in `to_dict()`, `to_json()`, and file
serialization.

## Example: Full Pipeline Audit

```python
from simpleetl import SimpleETL
from simpleetl.core.lineage import ProvenanceHook, ProvenanceTracker

# Process data
simple_etl = SimpleETL(input_path="data.csv", output_path="output.parquet")

# Use the provenance hook with DataFrames that have an "id" column
df = simple_etl.read("data.csv")
hook = ProvenanceHook(record_id_column="id")

print("Processing records...")
transformed = df[df["age"] > 18]

# Manually track provenance (normally done by the hook during job execution)
from simpleetl.core.lineage import get_lineage_tracker
tracker = get_lineage_tracker()

for record_id in transformed["id"]:
    tracker.record_provenance(str(record_id), "filter:age>18")

# Audit a specific record
print(tracker.get_provenance("42"))
# Output: ['filter:age>18']
```
