"""
Data lineage and observability for SimpleETL.

Tracks data flow from source through transformations to destination,
records per-transformation audit trails, and provides hooks for
integration with OpenLineage and alerting systems.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request
import urllib.error
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from .hooks import (
    Hook,
    HookContext,
    POST_EXTRACT,
    POST_TRANSFORM,
    POST_LOAD,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# LineageEvent
# ---------------------------------------------------------------------------

@dataclass
class LineageEvent:
    """Represents a single lineage event (a transformation step).

    Attributes:
        event_id: Unique identifier for this event.
        timestamp: When the event was recorded.
        job_name: Name of the ETL job that produced this event.
        phase: The ETL phase (e.g. ``post_extract``, ``post_transform``).
        source: Data source identifier.
        destination: Data destination identifier.
        operation: Description of the operation performed.
        input_schema: Schema of the input data (column names/types).
        output_schema: Schema of the output data (column names/types).
        input_rows: Number of input rows.
        output_rows: Number of output rows.
        duration_seconds: Time taken for this phase.
        metadata: Arbitrary additional metadata.
    """

    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    job_name: str = ""
    phase: str = ""
    source: str = ""
    destination: str = ""
    operation: str = ""
    input_schema: Dict[str, str] = field(default_factory=dict)
    output_schema: Dict[str, str] = field(default_factory=dict)
    input_rows: int = 0
    output_rows: int = 0
    duration_seconds: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    record_provenance: Dict[str, List[str]] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize the event to a dictionary."""
        d = asdict(self)
        d["timestamp"] = self.timestamp.isoformat()
        return d

    def to_json(self) -> str:
        """Serialize the event to a JSON string."""
        return json.dumps(self.to_dict(), default=str)


# ---------------------------------------------------------------------------
# ProvenanceTracker
# ---------------------------------------------------------------------------


class ProvenanceTracker:
    """Efficiently tracks per-record provenance across ETL phases.

    Uses an internal dict for O(1) lookups and supports
    serialization to/from JSON.
    """

    def __init__(self) -> None:
        self._provenance: Dict[str, List[str]] = {}

    def track(self, record_id: str, transformation: str) -> None:
        """Record a transformation for a specific record.

        Args:
            record_id: Unique identifier for the record.
            transformation: Description of the transformation applied.
        """
        if record_id not in self._provenance:
            self._provenance[record_id] = []
        self._provenance[record_id].append(transformation)

    def get(self, record_id: str) -> List[str]:
        """Get the provenance chain for a record.

        Args:
            record_id: The record identifier.

        Returns:
            List of transformation descriptions in order.
        """
        return list(self._provenance.get(record_id, []))

    def to_dict(self) -> Dict[str, List[str]]:
        """Serialize provenance data to a dictionary.

        Returns:
            Dictionary mapping record IDs to transformation chains.
        """
        return {k: list(v) for k, v in self._provenance.items()}

    @classmethod
    def from_dict(cls, data: Dict[str, List[str]]) -> ProvenanceTracker:
        """Create a ProvenanceTracker from a dictionary.

        Args:
            data: Dictionary mapping record IDs to transformation chains.

        Returns:
            A new ``ProvenanceTracker`` populated with the data.
        """
        tracker = cls()
        for record_id, transformations in data.items():
            tracker._provenance[record_id] = list(transformations)
        return tracker


# ---------------------------------------------------------------------------
# LineageTracker
# ---------------------------------------------------------------------------

class LineageTracker:
    """Collects and stores lineage events during ETL execution.

    Provides methods to record, query, and serialize lineage events
    for auditing, debugging, and observability.
    """

    def __init__(self) -> None:
        """Initialize the lineage tracker with an empty event store."""
        self._events: List[LineageEvent] = []
        self._provenance: Dict[str, List[str]] = {}

    def record_event(self, event: LineageEvent) -> None:
        """Record a lineage event.

        Args:
            event: The ``LineageEvent`` to store.
        """
        self._events.append(event)
        logger.debug(
            "Recorded lineage event %s for job '%s' phase '%s'",
            event.event_id,
            event.job_name,
            event.phase,
        )

    def get_events(
        self, job_name: Optional[str] = None
    ) -> List[LineageEvent]:
        """Retrieve lineage events, optionally filtered by job name.

        Args:
            job_name: If provided, return only events for this job.

        Returns:
            A list of ``LineageEvent`` objects.
        """
        if job_name is None:
            return list(self._events)
        return [e for e in self._events if e.job_name == job_name]

    def get_lineage(self, job_name: str) -> Dict[str, Any]:
        """Return the full lineage graph for a job.

        The returned dictionary contains:

        - ``job_name``: The job name.
        - ``events``: Chronological list of events.
        - ``phases``: List of phases observed.
        - ``total_rows_processed``: Sum of output rows across all events.
        - ``total_duration_seconds``: Sum of durations across all events.

        Args:
            job_name: The job to build the lineage graph for.

        Returns:
            A dictionary representing the lineage graph.
        """
        events = self.get_events(job_name)
        phases = list(dict.fromkeys(e.phase for e in events))
        total_rows = sum(e.output_rows for e in events)
        total_duration = sum(e.duration_seconds for e in events)

        return {
            "job_name": job_name,
            "events": [e.to_dict() for e in events],
            "phases": phases,
            "total_rows_processed": total_rows,
            "total_duration_seconds": total_duration,
        }

    def to_dict(self) -> Dict[str, Any]:
        """Serialize all events to a dictionary.

        Returns:
            A dictionary with an ``events`` key containing all events.
        """
        return {"events": [e.to_dict() for e in self._events]}

    def to_json(self) -> str:
        """Serialize all events to a JSON string.

        Returns:
            JSON string of all recorded events.
        """
        return json.dumps(self.to_dict(), default=str)

    def clear(self) -> None:
        """Remove all recorded events."""
        self._events.clear()
        logger.debug("Cleared all lineage events.")

    def summary(self) -> Dict[str, Any]:
        """Return summary statistics of recorded events.

        Returns:
            Dictionary with:

            - ``total_events``: Number of recorded events.
            - ``total_rows_processed``: Sum of output rows.
            - ``total_duration_seconds``: Sum of durations.
            - ``jobs``: List of unique job names.
        """
        jobs = list(dict.fromkeys(e.job_name for e in self._events))
        return {
            "total_events": len(self._events),
            "total_rows_processed": sum(
                e.output_rows for e in self._events
            ),
            "total_duration_seconds": sum(
                e.duration_seconds for e in self._events
            ),
            "jobs": jobs,
        }

    def to_file(self, path: str) -> None:
        """Write all lineage events to a JSON-lines file.

        Each event is serialized as a single JSON object per line.
        Creates parent directories if they do not exist.

        Args:
            path: File path to write events to.
        """
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(path, "w") as fh:  # noqa: PTH123
            for event in self._events:
                fh.write(json.dumps(event.to_dict(), default=str) + "\n")
        logger.info("Wrote %d lineage events to %s", len(self._events), path)

    def record_provenance(
        self,
        record_id: str,
        transformation: str,
        event_id: str = "",
    ) -> None:
        """Record a provenance entry for a single record.

        Args:
            record_id: Unique identifier for the record.
            transformation: Description of the transformation applied.
            event_id: Optional parent event ID for grouping.
        """
        if record_id not in self._provenance:
            self._provenance[record_id] = []
        self._provenance[record_id].append(transformation)
        # Also attach to the target event if one exists
        if self._events:
            target_event: Optional[LineageEvent] = None
            if event_id:
                for event in reversed(self._events):
                    if event.event_id == event_id:
                        target_event = event
                        break
            if target_event is None:
                target_event = self._events[-1]
            if record_id not in target_event.record_provenance:
                target_event.record_provenance[record_id] = []
            target_event.record_provenance[record_id].append(transformation)
        logger.debug(
            "Recorded provenance for record '%s': %s",
            record_id,
            transformation,
        )

    def get_provenance(self, record_id: str) -> List[str]:
        """Get the provenance chain for a record.

        Args:
            record_id: The record identifier.

        Returns:
            List of transformation descriptions in order.
        """
        return list(self._provenance.get(record_id, []))

    def get_all_provenance(self) -> Dict[str, List[str]]:
        """Return all provenance data.

        Returns:
            Dictionary mapping record IDs to transformation chains.
        """
        return {k: list(v) for k, v in self._provenance.items()}

    @classmethod
    def from_file(cls, path: str) -> LineageTracker:
        """Load lineage events from a JSON-lines file.

        Returns a new ``LineageTracker`` instance populated with
        events from the file.

        Args:
            path: File path to load events from.

        Returns:
            A new ``LineageTracker`` with loaded events.

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        tracker = cls()
        with open(path) as fh:  # noqa: PTH123
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                data = json.loads(line)
                # Reconstruct the LineageEvent from the dict
                event = LineageEvent(
                    event_id=data.get("event_id", str(uuid.uuid4())),
                    timestamp=datetime.fromisoformat(data["timestamp"]),
                    job_name=data.get("job_name", ""),
                    phase=data.get("phase", ""),
                    source=data.get("source", ""),
                    destination=data.get("destination", ""),
                    operation=data.get("operation", ""),
                    input_schema=data.get("input_schema", {}),
                    output_schema=data.get("output_schema", {}),
                    input_rows=data.get("input_rows", 0),
                    output_rows=data.get("output_rows", 0),
                    duration_seconds=data.get("duration_seconds", 0.0),
                    metadata=data.get("metadata", {}),
                    record_provenance=data.get("record_provenance", {}),
                )
                tracker._events.append(event)
        logger.info(
            "Loaded %d lineage events from %s", len(tracker._events), path
        )
        return tracker

    def emit_openlineage(
        self,
        url: str,
        converter: Optional[OpenLineageConverter] = None,
    ) -> int:
        """POST all recorded events as OpenLineage RunEvents to *url*.

        Each event is converted to an OpenLineage RunEvent via *converter*
        (or a default ``OpenLineageConverter`` if not provided) and sent
        as an HTTP POST with ``Content-Type: application/json``.

        Errors are caught and logged -- this method never raises.

        Args:
            url: The OpenLineage-compatible HTTP endpoint.
            converter: Optional ``OpenLineageConverter`` instance.
                Defaults to a converter with namespace ``"simpleetl"``.

        Returns:
            The number of events successfully emitted.
        """
        if converter is None:
            converter = OpenLineageConverter()
        emitted = 0
        for event in self._events:
            run_event = converter.event_to_run_event(event)
            payload = json.dumps(run_event).encode("utf-8")
            req = urllib.request.Request(
                url,
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            try:
                with urllib.request.urlopen(req, timeout=10) as resp:
                    if resp.status in (200, 201, 202, 204):
                        emitted += 1
                    else:
                        logger.warning(
                            "OpenLineage emit returned HTTP %d for "
                            "event %s",
                            resp.status,
                            event.event_id,
                        )
            except (urllib.error.URLError, OSError) as exc:
                logger.error(
                    "Failed to emit OpenLineage event %s to %s: %s",
                    event.event_id,
                    url,
                    exc,
                )
        logger.info(
            "Emitted %d/%d OpenLineage events to %s",
            emitted,
            len(self._events),
            url,
        )
        return emitted


# ---------------------------------------------------------------------------
# OpenLineageConverter
# ---------------------------------------------------------------------------


class OpenLineageConverter:
    """Converts SimpleETL LineageEvents to OpenLineage RunEvent format.

    Follows the OpenLineage spec:
    https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json
    """

    def __init__(
        self,
        namespace: str = "simpleetl",
        producer: str = "simpleetl/1.0.0",
    ) -> None:
        """Initialize the converter.

        Args:
            namespace: OpenLineage namespace for datasets and jobs.
            producer: Producer URI string identifying this emitter.
        """
        self.namespace = namespace
        self.producer = producer

    def event_to_run_event(
        self, event: LineageEvent, run_id: str | None = None
    ) -> Dict[str, Any]:
        """Convert a LineageEvent to an OpenLineage RunEvent dict.

        Args:
            event: The ``LineageEvent`` to convert.
            run_id: Optional explicit run ID. Defaults to *event.event_id*.

        Returns:
            An OpenLineage-compatible RunEvent dictionary.
        """
        if run_id is None:
            run_id = event.event_id

        inputs: list[Dict[str, Any]] = []
        outputs: list[Dict[str, Any]] = []

        if event.source:
            inputs.append(
                self._build_dataset(event.source, event.input_schema)
            )
        if event.destination:
            outputs.append(
                self._build_dataset(event.destination, event.output_schema)
            )

        return {
            "producer": self.producer,
            "schemaURL": (
                "https://openlineage.io/spec/1-0-5/OpenLineage.json"
                "#/$defs/RunEvent"
            ),
            "eventType": "COMPLETE",
            "eventTime": event.timestamp.isoformat(),
            "run": {
                "runId": run_id,
                "facets": {
                    "nominalTime": {
                        "nominalStartTime": event.timestamp.isoformat(),
                        "_producer": self.producer,
                        "_schemaURL": (
                            "https://openlineage.io/spec/facets/1-0-0"
                            "/NominalTimeRunFacet.json"
                        ),
                    },
                    "simpleetl": {
                        "job_name": event.job_name,
                        "phase": event.phase,
                        "operation": event.operation,
                        "input_rows": event.input_rows,
                        "output_rows": event.output_rows,
                        "duration_seconds": event.duration_seconds,
                        "metadata": event.metadata,
                        "_producer": self.producer,
                        "_schemaURL": (
                            "https://openlineage.io/spec/facets/1-0-0"
                            "/CustomRunFacet.json"
                        ),
                    },
                },
            },
            "job": {
                "namespace": self.namespace,
                "name": event.job_name or "unknown",
                "facets": {},
            },
            "inputs": inputs,
            "outputs": outputs,
        }

    def event_to_dataset(
        self, event: LineageEvent, is_input: bool = True
    ) -> Dict[str, Any]:
        """Convert event source/destination to an OpenLineage Dataset.

        Args:
            event: The ``LineageEvent`` to extract a dataset from.
            is_input: If True, use the event's *source* and *input_schema*.
                Otherwise, use *destination* and *output_schema*.

        Returns:
            An OpenLineage-compatible Dataset dictionary.
        """
        if is_input:
            name = event.source
            schema = event.input_schema
        else:
            name = event.destination
            schema = event.output_schema

        return self._build_dataset(name, schema)

    def _build_dataset(
        self, name: str, schema_fields: Dict[str, str]
    ) -> Dict[str, Any]:
        """Build an OpenLineage Dataset dict from a name and schema.

        Args:
            name: The dataset name (e.g. ``s3://bucket/data.csv``).
            schema_fields: Mapping of field names to type names.

        Returns:
            An OpenLineage Dataset dictionary.
        """
        fields: list[Dict[str, Any]] = [
            {"name": field_name, "type": field_type}
            for field_name, field_type in schema_fields.items()
        ]
        return {
            "namespace": self.namespace,
            "name": name or "unknown",
            "facets": {
                "schema": {
                    "fields": fields,
                    "_producer": self.producer,
                    "_schemaURL": (
                        "https://openlineage.io/spec/facets/1-0-1"
                        "/SchemaDatasetFacet.json"
                    ),
                },
            },
        }


# ---------------------------------------------------------------------------
# FileLineageStore
# ---------------------------------------------------------------------------


class FileLineageStore:
    """Wraps file-based persistence for a ``LineageTracker``.

    Automatically persists events to a JSON-lines file when
    ``record_event`` is called, or on explicit ``flush()`` /
    ``close()`` calls.

    Example::

        store = FileLineageStore("/var/lineage/job_events.jsonl")
        store.record_event(event)
        store.close()

    Attributes:
        file_path: The JSON-lines file path for persistence.
    """

    def __init__(
        self,
        file_path: str,
        lineage_tracker: Optional[LineageTracker] = None,
    ) -> None:
        """Initialize the file lineage store.

        Args:
            file_path: Path to the JSON-lines file for persistence.
            lineage_tracker: Optional tracker to wrap. If None, uses
                the module-level singleton.
        """
        self.file_path = file_path
        self._tracker = lineage_tracker or get_lineage_tracker()
        self._write_on_record = True
        # Ensure parent directory exists
        parent = os.path.dirname(file_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        # Open file in append mode for incremental writes
        self._fh = open(file_path, "a")  # noqa: PTH123
        logger.info("FileLineageStore initialized with path: %s", file_path)

    def record_event(self, event: LineageEvent) -> None:
        """Record an event and optionally flush to file immediately.

        Args:
            event: The ``LineageEvent`` to store.
        """
        self._tracker.record_event(event)
        if self._write_on_record:
            self._flush_event(event)

    def _flush_event(self, event: LineageEvent) -> None:
        """Write a single event to the file."""
        try:
            self._fh.write(
                json.dumps(event.to_dict(), default=str) + "\n"
            )
            self._fh.flush()
        except OSError as exc:
            logger.error("Failed to write lineage event to file: %s", exc)

    def flush(self) -> None:
        """Flush all buffered events to the file.

        Writes all events from the tracked to the file in JSON-lines
        format. This is useful when ``_write_on_record`` is False.
        """
        with open(self.file_path, "w") as fh:  # noqa: PTH123
            for event in self._tracker._events:
                fh.write(json.dumps(event.to_dict(), default=str) + "\n")
        logger.info(
            "Flushed %d events to %s",
            len(self._tracker._events),
            self.file_path,
        )

    def close(self) -> None:
        """Close the underlying file handle."""
        if self._fh and not self._fh.closed:
            self._fh.close()
            logger.debug("Closed FileLineageStore file handle.")

    def get_events(
        self, job_name: Optional[str] = None
    ) -> List[LineageEvent]:
        """Delegate to the underlying tracker's ``get_events``."""
        return self._tracker.get_events(job_name)

    def __enter__(self) -> FileLineageStore:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()


# ---------------------------------------------------------------------------
# LineageHook
# ---------------------------------------------------------------------------

class LineageHook(Hook):
    """Hook that automatically records lineage events at each ETL phase.

    Records events at ``POST_EXTRACT``, ``POST_TRANSFORM``, and
    ``POST_LOAD`` phases. Captures phase name, data shape, duration,
    and metadata from the hook context.

    Attributes:
        name: The hook name (``"lineage"``).
        priority: Execution priority (default 0).
    """

    name = "lineage"
    priority = 0

    def __init__(
        self,
        tracker: Optional[LineageTracker] = None,
        job_name: str = "",
    ) -> None:
        """Initialize the lineage hook.

        Args:
            tracker: ``LineageTracker`` instance to record events into.
                If None, the module-level singleton is used.
            job_name: Default job name to use if not available from context.
        """
        self._tracker = tracker
        self._job_name = job_name
        self._phase_starts: Dict[str, float] = {}
        self._logger = logging.getLogger(f"{__name__}.LineageHook")

    def _get_tracker(self) -> LineageTracker:
        """Return the tracker, falling back to the module singleton."""
        if self._tracker is not None:
            return self._tracker
        return get_lineage_tracker()

    def _extract_shape(self, data: Any) -> tuple[int, Dict[str, str]]:
        """Extract row count and schema from data.

        Args:
            data: The data object (typically a DataFrame).

        Returns:
            Tuple of (row_count, schema_dict).
        """
        if data is None:
            return 0, {}
        try:
            import pandas as pd

            if isinstance(data, pd.DataFrame):
                schema = {
                    str(col): str(dtype)
                    for col, dtype in data.dtypes.items()
                }
                return len(data), schema
        except ImportError:
            pass
        if hasattr(data, "__len__"):
            return len(data), {}
        return 0, {}

    def execute(self, context: HookContext) -> None:
        """Execute the hook: record a lineage event for post-* phases.

        Args:
            context: The hook context from the ETL job.
        """
        import time as _time

        phase = context.phase

        # Track start times for pre-* phases
        if phase in ("pre_extract", "pre_transform", "pre_load"):
            self._phase_starts[phase] = _time.time()
            return

        if phase not in (POST_EXTRACT, POST_TRANSFORM, POST_LOAD):
            return

        # Compute duration from corresponding pre-* phase
        pre_phase = phase.replace("post_", "pre_")
        start = self._phase_starts.get(pre_phase, 0.0)
        duration = _time.time() - start if start else 0.0

        # Determine job name
        job_name = self._job_name
        if context.job is not None:
            job_name = context.job.config.name

        # Extract data shape
        input_rows = 0
        output_rows = 0
        output_schema: Dict[str, str] = {}

        if context.data is not None:
            output_rows, output_schema = self._extract_shape(context.data)

        # Try to get input_rows from metadata
        input_rows = context.metadata.get("extracted_rows", 0)

        event = LineageEvent(
            job_name=job_name,
            phase=phase,
            operation=phase,
            input_rows=input_rows,
            output_rows=output_rows,
            output_schema=output_schema,
            duration_seconds=round(duration, 6),
            metadata=dict(context.metadata),
        )

        self._get_tracker().record_event(event)
        self._logger.debug(
            "[LineageHook] Recorded event for job '%s' phase '%s': "
            "%d -> %d rows in %.4fs",
            job_name,
            phase,
            input_rows,
            output_rows,
            duration,
        )


# ---------------------------------------------------------------------------
# ProvenanceHook
# ---------------------------------------------------------------------------


class ProvenanceHook(Hook):
    """Hook that records per-record provenance during ETL execution.

    Tracks which transformations each record passes through.
    Requires that records have a unique identifier column.

    Attributes:
        name: The hook name ("provenance").
        record_id_column: Column name containing record IDs.
    """

    name = "provenance"

    def __init__(
        self,
        record_id_column: str = "id",
        tracker: Optional[LineageTracker] = None,
    ) -> None:
        """Initialize the provenance hook.

        Args:
            record_id_column: Column name containing record IDs.
            tracker: ``LineageTracker`` instance to record into.
                If None, the module-level singleton is used.
        """
        self._record_id_column = record_id_column
        self._tracker = tracker
        self._provenance_tracker = ProvenanceTracker()
        self._logger = logging.getLogger(f"{__name__}.ProvenanceHook")

    def _get_tracker(self) -> LineageTracker:
        """Return the tracker, falling back to the module singleton."""
        if self._tracker is not None:
            return self._tracker
        return get_lineage_tracker()

    def _extract_record_ids(self, data: Any) -> List[str]:
        """Extract record IDs from the data.

        Args:
            data: The data object (typically a DataFrame or list of dicts).

        Returns:
            List of record ID strings.
        """
        if data is None:
            return []
        try:
            import pandas as pd

            if isinstance(data, pd.DataFrame):
                if self._record_id_column in data.columns:
                    return [
                        str(v)
                        for v in data[self._record_id_column].tolist()
                    ]
        except ImportError:
            pass
        if isinstance(data, list):
            ids: List[str] = []
            for item in data:
                if isinstance(item, dict) and self._record_id_column in item:
                    ids.append(str(item[self._record_id_column]))
            return ids
        return []

    def execute(self, context: HookContext) -> None:
        """Execute the hook: record provenance for each record in data.

        Args:
            context: The hook context from the ETL job.
        """
        phase = context.phase
        if phase not in (
            POST_EXTRACT,
            POST_TRANSFORM,
            POST_LOAD,
        ):
            return

        record_ids = self._extract_record_ids(context.data)
        if not record_ids:
            return

        transformation = phase
        tracker = self._get_tracker()

        for record_id in record_ids:
            self._provenance_tracker.track(record_id, transformation)
            tracker.record_provenance(record_id, transformation)

        self._logger.debug(
            "[ProvenanceHook] Recorded provenance for %d records at phase '%s'",
            len(record_ids),
            phase,
        )


# ---------------------------------------------------------------------------
# DataFreshnessTracker
# ---------------------------------------------------------------------------

class DataFreshnessTracker:
    """Tracks data freshness (last update time) for data sources.

    Allows monitoring of when each data source was last updated
    and checking whether data has become stale.
    """

    def __init__(self) -> None:
        """Initialize the freshness tracker."""
        self._freshness: Dict[str, datetime] = {}

    def record_freshness(
        self, source: str, timestamp: Optional[datetime] = None
    ) -> None:
        """Record that a data source was updated.

        Args:
            source: Identifier for the data source.
            timestamp: The update time. Defaults to current UTC time.
        """
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
        self._freshness[source] = timestamp
        logger.debug("Recorded freshness for '%s': %s", source, timestamp)

    def get_freshness(self, source: str) -> Optional[datetime]:
        """Get the last update time for a data source.

        Args:
            source: The data source identifier.

        Returns:
            The ``datetime`` of the last update, or ``None`` if unknown.
        """
        return self._freshness.get(source)

    def is_stale(self, source: str, max_age_seconds: float) -> bool:
        """Check whether a data source is stale.

        A source is considered stale if its last update time is older
        than *max_age_seconds*, or if it has never been recorded.

        Args:
            source: The data source identifier.
            max_age_seconds: Maximum allowed age in seconds.

        Returns:
            ``True`` if the source is stale or unknown.
        """
        ts = self._freshness.get(source)
        if ts is None:
            return True
        now = datetime.now(timezone.utc)
        age = (now - ts).total_seconds()
        return age > max_age_seconds

    def get_all_freshness(self) -> Dict[str, datetime]:
        """Return freshness data for all tracked sources.

        Returns:
            Dictionary mapping source names to their last update times.
        """
        return dict(self._freshness)

    def summary(self) -> Dict[str, Any]:
        """Return a summary of freshness data.

        Returns:
            Dictionary with:

            - ``total_sources``: Number of tracked sources.
            - ``sources``: List of source names.
            - ``stalest_source``: Name of the source not updated longest ago.
            - ``newest_source``: Name of the most recently updated source.
        """
        sources = list(self._freshness.keys())
        stalest = None
        newest = None
        if sources:
            stalest = min(sources, key=lambda s: self._freshness[s])
            newest = max(sources, key=lambda s: self._freshness[s])
        return {
            "total_sources": len(sources),
            "sources": sources,
            "stalest_source": stalest,
            "newest_source": newest,
        }


# ---------------------------------------------------------------------------
# AlertRule
# ---------------------------------------------------------------------------

@dataclass
class AlertRule:
    """Defines an alert condition for monitoring.

    Attributes:
        name: Human-readable name for the alert rule.
        condition: A callable that receives a context dict and returns
            ``True`` if the alert should fire.
        severity: Severity level (e.g. ``"warning"``, ``"critical"``).
        message_template: A format string for the alert message. Can use
            keys from the context dict via ``str.format()``.
        channels: List of notification channel names
            (e.g. ``["email", "slack"]``).
    """

    name: str
    condition: Callable[[Dict[str, Any]], bool]
    severity: str = "warning"
    message_template: str = "Alert '{name}' triggered"
    channels: List[str] = field(default_factory=list)
    channel_instances: List[AlertChannel] = field(
        default_factory=list, repr=False
    )

    def evaluate(self, context: Dict[str, Any]) -> Optional[str]:
        """Evaluate the rule against a context.

        Returns:
            Alert message string if triggered, None otherwise.
        """
        try:
            if self.condition(context):
                return self.message_template.format(
                    name=self.name, severity=self.severity, **context
                )
        except Exception as exc:
            logger.warning(
                "Error evaluating alert rule '%s': %s", self.name, exc
            )
        return None

    def dispatch(self, message: str) -> Dict[str, bool]:
        """Send the alert through all configured channels.

        Returns:
            Dictionary mapping channel names to success status.
        """
        results: Dict[str, bool] = {}
        for channel in self.channel_instances:
            channel_name = type(channel).__name__
            results[channel_name] = channel.send(
                message, self.severity, self.name
            )
        return results


# ---------------------------------------------------------------------------
# AlertChannel
# ---------------------------------------------------------------------------


class AlertChannel(ABC):
    """Base class for alert notification channels."""

    @abstractmethod
    def send(self, message: str, severity: str, rule_name: str) -> bool:
        """Send an alert notification.

        Args:
            message: The alert message.
            severity: Severity level (e.g., "warning", "critical").
            rule_name: Name of the triggered rule.

        Returns:
            True if the notification was sent successfully.
        """
        pass


class WebhookChannel(AlertChannel):
    """Send alerts via HTTP webhook (POST JSON)."""

    def __init__(self, url: str, timeout: float = 10.0) -> None:
        self.url = url
        self.timeout = timeout

    def send(self, message: str, severity: str, rule_name: str) -> bool:
        import json

        import urllib.request

        payload = json.dumps(
            {
                "message": message,
                "severity": severity,
                "rule": rule_name,
                "source": "simpleetl",
            }
        ).encode()
        req = urllib.request.Request(
            self.url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(
                req, timeout=self.timeout
            ) as resp:
                return resp.status == 200
        except Exception:
            return False


class SlackChannel(AlertChannel):
    """Send alerts to Slack via incoming webhook."""

    def __init__(self, webhook_url: str) -> None:
        self.webhook_url = webhook_url

    def send(self, message: str, severity: str, rule_name: str) -> bool:
        import json

        import urllib.request

        color = "#ff0000" if severity == "critical" else "#ffaa00"
        payload = json.dumps(
            {
                "attachments": [
                    {
                        "color": color,
                        "title": f"SimpleETL Alert: {rule_name}",
                        "text": message,
                        "fields": [
                            {
                                "title": "Severity",
                                "value": severity,
                                "short": True,
                            },
                            {
                                "title": "Source",
                                "value": "simpleetl",
                                "short": True,
                            },
                        ],
                    }
                ],
            }
        ).encode()
        req = urllib.request.Request(
            self.webhook_url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.status == 200
        except Exception:
            return False


class EmailChannel(AlertChannel):
    """Stub for email alerts (logs the alert, actual SMTP is platform-specific)."""

    def __init__(
        self, recipients: List[str], smtp_host: str = "localhost"
    ) -> None:
        self.recipients = recipients
        self.smtp_host = smtp_host

    def send(self, message: str, severity: str, rule_name: str) -> bool:
        logger = logging.getLogger(__name__)
        logger.info(
            "[EMAIL ALERT] To: %s | Severity: %s | Rule: %s | Message: %s",
            self.recipients,
            severity,
            rule_name,
            message,
        )
        return True


# ---------------------------------------------------------------------------
# AlertManager
# ---------------------------------------------------------------------------

class AlertManager:
    """Manages alert rules and triggers alerts based on conditions.

    Rules are evaluated against a context dictionary. When a rule's
    condition returns ``True``, its message template is formatted with
    the context and the rule name, and the resulting message is returned.
    """

    def __init__(self) -> None:
        """Initialize the alert manager with an empty rule set."""
        self._rules: List[AlertRule] = []

    def add_rule(self, rule: AlertRule) -> None:
        """Add an alert rule.

        Args:
            rule: The ``AlertRule`` to add.
        """
        self._rules.append(rule)
        logger.debug(
            "Added alert rule '%s' (severity=%s)", rule.name, rule.severity
        )

    def check_alerts(self, context: Dict[str, Any]) -> List[str]:
        """Evaluate all rules against the given context.

        Args:
            context: Dictionary of values available to rule conditions
                and message templates.

        Returns:
            List of alert messages for rules that fired.
        """
        triggered: List[str] = []
        for rule in self._rules:
            try:
                if rule.condition(context):
                    msg = rule.message_template.format(
                        name=rule.name, severity=rule.severity, **context
                    )
                    triggered.append(msg)
                    logger.info(
                        "Alert '%s' (severity=%s) triggered: %s",
                        rule.name,
                        rule.severity,
                        msg,
                    )
            except Exception as exc:
                logger.warning(
                    "Error evaluating alert rule '%s': %s", rule.name, exc
                )
        return triggered

    def check_and_dispatch(
        self, context: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Evaluate all rules and dispatch alerts for triggered rules.

        Returns:
            List of dicts with rule name, message, and dispatch results.
        """
        results: List[Dict[str, Any]] = []
        for rule in self._rules:
            message = rule.evaluate(context)
            if message is not None:
                dispatch_results = rule.dispatch(message)
                results.append(
                    {
                        "rule": rule.name,
                        "message": message,
                        "severity": rule.severity,
                        "dispatch_results": dispatch_results,
                    }
                )
                logger.info(
                    "Alert '%s' (severity=%s) dispatched: %s",
                    rule.name,
                    rule.severity,
                    dispatch_results,
                )
        return results

    def clear_rules(self) -> None:
        """Remove all alert rules."""
        self._rules.clear()
        logger.debug("Cleared all alert rules.")


# ---------------------------------------------------------------------------
# Module-level singletons
# ---------------------------------------------------------------------------

_lineage_tracker: Optional[LineageTracker] = None
_freshness_tracker = DataFreshnessTracker()
_alert_manager = AlertManager()
_lineage_persistence_path: Optional[str] = None
_file_lineage_store: Optional[FileLineageStore] = None
_openlineage_converter: Optional[OpenLineageConverter] = None


def get_lineage_tracker(
    persistence_path: Optional[str] = None,
) -> LineageTracker:
    """Return the global ``LineageTracker`` singleton.

    On first call, initializes the tracker. If *persistence_path* is
    provided (or was configured via ``configure_lineage_persistence``),
    the tracker will automatically persist events to that file.

    Args:
        persistence_path: Optional file path for JSON-lines persistence.
            If provided, overrides any previously configured path.

    Returns:
        The global ``LineageTracker`` instance.
    """
    global _lineage_tracker, _lineage_persistence_path  # noqa: PLW0603
    if _lineage_tracker is None:
        _lineage_tracker = LineageTracker()
    if persistence_path is not None:
        _lineage_persistence_path = persistence_path
    return _lineage_tracker


def configure_lineage_persistence(
    path: str,
    auto_flush: bool = True,
) -> FileLineageStore:
    """Configure file-based persistence for the lineage tracker.

    Creates a ``FileLineageStore`` that writes events to *path* in
    JSON-lines format. The store wraps the module-level singleton
    tracker.

    Args:
        path: File path for JSON-lines persistence.
        auto_flush: If True, each event is written immediately.

    Returns:
        The configured ``FileLineageStore`` instance.
    """
    global _file_lineage_store, _lineage_persistence_path  # noqa: PLW0603
    tracker = get_lineage_tracker()
    _lineage_persistence_path = path
    _file_lineage_store = FileLineageStore(
        file_path=path, lineage_tracker=tracker
    )
    _file_lineage_store._write_on_record = auto_flush
    return _file_lineage_store


def get_file_lineage_store() -> Optional[FileLineageStore]:
    """Return the configured file lineage store, if any.

    Returns:
        The ``FileLineageStore`` or ``None`` if not configured.
    """
    return _file_lineage_store


def get_freshness_tracker() -> DataFreshnessTracker:
    """Return the global ``DataFreshnessTracker`` singleton."""
    return _freshness_tracker


def get_alert_manager() -> AlertManager:
    """Return the global ``AlertManager`` singleton."""
    return _alert_manager


def create_lineage_hook(job_name: str = "") -> LineageHook:
    """Create a ``LineageHook`` bound to the global tracker.

    Args:
        job_name: Default job name for recorded events.

    Returns:
        A configured ``LineageHook`` instance.
    """
    store = get_file_lineage_store()
    if store is not None:
        # FileLineageStore wraps a LineageTracker; mypy can't see the delegation
        return LineageHook(tracker=store._tracker, job_name=job_name)  # type: ignore[arg-type]
    return LineageHook(tracker=get_lineage_tracker(), job_name=job_name)


def configure_openlineage(
    url: str,
    namespace: str = "simpleetl",
) -> OpenLineageConverter:
    """Configure OpenLineage emission for the global tracker.

    Creates an ``OpenLineageConverter`` with the given *namespace* and
    stores it as the module-level default. The returned converter can be
    passed to ``LineageTracker.emit_openlineage()`` or used directly.

    Args:
        url: The OpenLineage-compatible HTTP endpoint URL.
        namespace: OpenLineage namespace for datasets and jobs.

    Returns:
        The configured ``OpenLineageConverter`` instance.
    """
    global _openlineage_converter  # noqa: PLW0603
    _openlineage_converter = OpenLineageConverter(
        namespace=namespace,
        producer="simpleetl/1.0.0",
    )
    logger.info(
        "OpenLineage configured with namespace '%s' and url '%s'",
        namespace,
        url,
    )
    return _openlineage_converter


def get_openlineage_converter() -> Optional[OpenLineageConverter]:
    """Return the configured OpenLineage converter, if any.

    Returns:
        The ``OpenLineageConverter`` or ``None`` if not configured.
    """
    return _openlineage_converter
