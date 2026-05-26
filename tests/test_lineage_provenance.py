"""
Tests for per-record provenance tracking in the lineage module.
"""

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

from simpleetl.core.lineage import (
    LineageEvent,
    LineageTracker,
    ProvenanceTracker,
    ProvenanceHook,
    get_lineage_tracker,
)
from simpleetl.core.hooks import (
    HookContext,
    POST_EXTRACT,
    POST_TRANSFORM,
    POST_LOAD,
    PRE_EXTRACT,
    PRE_TRANSFORM,
    PRE_LOAD,
)


# ---------------------------------------------------------------------------
# ProvenanceTracker tests
# ---------------------------------------------------------------------------


class TestProvenanceTracker:
    """Test ProvenanceTracker operations."""

    def setup_method(self):
        """Create a fresh tracker for each test."""
        self.tracker = ProvenanceTracker()

    def test_track_single_record(self):
        """Test tracking a single transformation for a record."""
        self.tracker.track("rec_001", "filter:age>18")
        assert self.tracker.get("rec_001") == ["filter:age>18"]

    def test_track_multiple_transformations(self):
        """Test tracking multiple transformations for one record."""
        self.tracker.track("rec_001", "filter:age>18")
        self.tracker.track("rec_001", "map:upper(name)")
        assert self.tracker.get("rec_001") == [
            "filter:age>18",
            "map:upper(name)",
        ]

    def test_track_multiple_records(self):
        """Test tracking transformations for multiple records."""
        self.tracker.track("rec_001", "filter:age>18")
        self.tracker.track("rec_002", "filter:age>18")
        self.tracker.track("rec_001", "map:upper(name)")
        assert self.tracker.get("rec_001") == [
            "filter:age>18",
            "map:upper(name)",
        ]
        assert self.tracker.get("rec_002") == ["filter:age>18"]

    def test_get_nonexistent_record(self):
        """Test getting provenance for an unknown record returns empty list."""
        assert self.tracker.get("nonexistent") == []

    def test_to_dict(self):
        """Test serialization to dictionary."""
        self.tracker.track("rec_001", "filter:age>18")
        self.tracker.track("rec_002", "map:upper(name)")
        d = self.tracker.to_dict()
        assert d == {
            "rec_001": ["filter:age>18"],
            "rec_002": ["map:upper(name)"],
        }

    def test_to_dict_returns_copies(self):
        """Test that to_dict returns copies, not references."""
        self.tracker.track("rec_001", "step1")
        d = self.tracker.to_dict()
        d["rec_001"].append("step2")
        assert self.tracker.get("rec_001") == ["step1"]

    def test_from_dict(self):
        """Test deserialization from dictionary."""
        data = {
            "rec_001": ["filter:age>18", "map:upper(name)"],
            "rec_002": ["filter:status=active"],
        }
        tracker = ProvenanceTracker.from_dict(data)
        assert tracker.get("rec_001") == [
            "filter:age>18",
            "map:upper(name)",
        ]
        assert tracker.get("rec_002") == ["filter:status=active"]

    def test_from_dict_returns_copies(self):
        """Test that from_dict copies data, not references."""
        data = {"rec_001": ["step1"]}
        tracker = ProvenanceTracker.from_dict(data)
        data["rec_001"].append("step2")
        assert tracker.get("rec_001") == ["step1"]

    def test_from_dict_empty(self):
        """Test from_dict with empty data."""
        tracker = ProvenanceTracker.from_dict({})
        assert tracker.get("any") == []
        assert tracker.to_dict() == {}

    def test_round_trip_serialization(self):
        """Test that to_dict -> from_dict preserves data."""
        self.tracker.track("rec_001", "extract")
        self.tracker.track("rec_001", "transform")
        self.tracker.track("rec_002", "extract")
        restored = ProvenanceTracker.from_dict(self.tracker.to_dict())
        assert restored.get("rec_001") == ["extract", "transform"]
        assert restored.get("rec_002") == ["extract"]


# ---------------------------------------------------------------------------
# LineageTracker provenance tests
# ---------------------------------------------------------------------------


class TestLineageTrackerProvenance:
    """Test LineageTracker provenance methods."""

    def setup_method(self):
        """Create a fresh tracker for each test."""
        self.tracker = LineageTracker()

    def test_record_provenance_basic(self):
        """Test basic provenance recording."""
        event = LineageEvent(job_name="test_job", phase=POST_EXTRACT)
        self.tracker.record_event(event)
        self.tracker.record_provenance("rec_001", "filter:age>18")
        provenance = self.tracker.get_provenance("rec_001")
        assert provenance == ["filter:age>18"]

    def test_record_provenance_multiple_entries(self):
        """Test recording multiple provenance entries."""
        event = LineageEvent(job_name="test_job", phase=POST_EXTRACT)
        self.tracker.record_event(event)
        self.tracker.record_provenance("rec_001", "filter:age>18")
        self.tracker.record_provenance("rec_001", "map:upper(name)")
        provenance = self.tracker.get_provenance("rec_001")
        assert provenance == ["filter:age>18", "map:upper(name)"]

    def test_record_provenance_multiple_records(self):
        """Test recording provenance for multiple records."""
        event = LineageEvent(job_name="test_job", phase=POST_EXTRACT)
        self.tracker.record_event(event)
        self.tracker.record_provenance("rec_001", "filter:age>18")
        self.tracker.record_provenance("rec_002", "filter:age>18")
        assert self.tracker.get_provenance("rec_001") == ["filter:age>18"]
        assert self.tracker.get_provenance("rec_002") == ["filter:age>18"]

    def test_record_provenance_with_event_id(self):
        """Test recording provenance targeting a specific event."""
        event1 = LineageEvent(job_name="test_job", phase=POST_EXTRACT)
        event2 = LineageEvent(job_name="test_job", phase=POST_TRANSFORM)
        self.tracker.record_event(event1)
        self.tracker.record_event(event2)
        self.tracker.record_provenance(
            "rec_001", "extract_filter", event_id=event1.event_id
        )
        self.tracker.record_provenance(
            "rec_001", "transform_map", event_id=event2.event_id
        )
        all_prov = self.tracker.get_all_provenance()
        assert "extract_filter" in all_prov["rec_001"]
        assert "transform_map" in all_prov["rec_001"]

    def test_record_provenance_no_events(self):
        """Test that record_provenance works even when no events exist."""
        self.tracker.record_provenance("rec_001", "filter:age>18")
        # Should not raise, and provenance is stored independently
        assert self.tracker.get_provenance("rec_001") == ["filter:age>18"]

    def test_record_provenance_falls_back_to_last_event(self):
        """Test that record_provenance falls back to last event."""
        event1 = LineageEvent(job_name="test_job", phase=POST_EXTRACT)
        event2 = LineageEvent(job_name="test_job", phase=POST_TRANSFORM)
        self.tracker.record_event(event1)
        self.tracker.record_event(event2)
        # No event_id, should target last event
        self.tracker.record_provenance("rec_001", "some_transform")
        assert event2.record_provenance["rec_001"] == ["some_transform"]
        assert "rec_001" not in event1.record_provenance

    def test_get_provenance_nonexistent(self):
        """Test getting provenance for an unknown record."""
        assert self.tracker.get_provenance("nonexistent") == []

    def test_get_all_provenance(self):
        """Test getting all provenance data."""
        event = LineageEvent(job_name="test_job", phase=POST_EXTRACT)
        self.tracker.record_event(event)
        self.tracker.record_provenance("rec_001", "filter:age>18")
        self.tracker.record_provenance("rec_002", "filter:status=active")
        all_prov = self.tracker.get_all_provenance()
        assert all_prov == {
            "rec_001": ["filter:age>18"],
            "rec_002": ["filter:status=active"],
        }

    def test_get_all_provenance_empty(self):
        """Test getting all provenance when nothing recorded."""
        assert self.tracker.get_all_provenance() == {}

    def test_get_all_provenance_merges_across_events(self):
        """Test that get_all_provenance merges entries from multiple events."""
        event1 = LineageEvent(job_name="test_job", phase=POST_EXTRACT)
        event2 = LineageEvent(job_name="test_job", phase=POST_TRANSFORM)
        self.tracker.record_event(event1)
        self.tracker.record_event(event2)
        self.tracker.record_provenance(
            "rec_001", "extract", event_id=event1.event_id
        )
        self.tracker.record_provenance(
            "rec_001", "transform", event_id=event2.event_id
        )
        all_prov = self.tracker.get_all_provenance()
        assert all_prov["rec_001"] == ["extract", "transform"]


# ---------------------------------------------------------------------------
# LineageEvent record_provenance field tests
# ---------------------------------------------------------------------------


class TestLineageEventRecordProvenance:
    """Test the record_provenance field on LineageEvent."""

    def test_default_record_provenance(self):
        """Test that record_provenance defaults to empty dict."""
        event = LineageEvent()
        assert event.record_provenance == {}

    def test_record_provenance_in_to_dict(self):
        """Test that record_provenance is included in serialization."""
        event = LineageEvent(
            job_name="test",
            record_provenance={"rec_001": ["filter:age>18"]},
        )
        d = event.to_dict()
        assert d["record_provenance"] == {"rec_001": ["filter:age>18"]}

    def test_record_provenance_in_to_json(self):
        """Test that record_provenance is included in JSON serialization."""
        event = LineageEvent(
            job_name="test",
            record_provenance={"rec_001": ["filter:age>18"]},
        )
        json_str = event.to_json()
        parsed = json.loads(json_str)
        assert parsed["record_provenance"] == {"rec_001": ["filter:age>18"]}

    def test_record_provenance_from_file(self):
        """Test that record_provenance is preserved through file round-trip."""
        event = LineageEvent(
            job_name="test",
            phase=POST_EXTRACT,
            record_provenance={"rec_001": ["step1", "step2"]},
        )
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".jsonl", delete=False
        ) as f:
            f.write(event.to_json() + "\n")
            path = f.name
        try:
            tracker = LineageTracker.from_file(path)
            events = tracker.get_events()
            assert len(events) == 1
            assert events[0].record_provenance == {
                "rec_001": ["step1", "step2"]
            }
        finally:
            Path(path).unlink()


# ---------------------------------------------------------------------------
# ProvenanceHook tests
# ---------------------------------------------------------------------------


class TestProvenanceHook:
    """Test ProvenanceHook integration with the hook system."""

    def setup_method(self):
        """Create a fresh tracker and hook for each test."""
        self.tracker = LineageTracker()
        self.hook = ProvenanceHook(
            record_id_column="id",
            tracker=self.tracker,
        )

    def _make_context(self, phase, data=None, metadata=None):
        """Helper to create a HookContext."""
        ctx = MagicMock(spec=HookContext)
        ctx.phase = phase
        ctx.data = data
        ctx.job = None
        ctx.metadata = metadata or {}
        ctx.error = None
        return ctx

    def test_hook_name(self):
        """Test hook has correct name."""
        assert self.hook.name == "provenance"

    def test_hook_default_record_id_column(self):
        """Test default record_id_column is 'id'."""
        hook = ProvenanceHook()
        assert hook._record_id_column == "id"

    def test_hook_custom_record_id_column(self):
        """Test custom record_id_column."""
        hook = ProvenanceHook(record_id_column="user_id")
        assert hook._record_id_column == "user_id"

    def test_extract_record_ids_from_dataframe(self):
        """Test extracting record IDs from a DataFrame."""
        try:
            import pandas as pd

            df = pd.DataFrame({"id": ["a", "b", "c"], "name": ["X", "Y", "Z"]})
            ids = self.hook._extract_record_ids(df)
            assert ids == ["a", "b", "c"]
        except ImportError:
            # pandas not installed, skip this test path
            pass

    def test_extract_record_ids_from_list_of_dicts(self):
        """Test extracting record IDs from a list of dicts."""
        data = [
            {"id": "rec_1", "name": "Alice"},
            {"id": "rec_2", "name": "Bob"},
        ]
        ids = self.hook._extract_record_ids(data)
        assert ids == ["rec_1", "rec_2"]

    def test_extract_record_ids_none_data(self):
        """Test extracting record IDs from None returns empty."""
        assert self.hook._extract_record_ids(None) == []

    def test_extract_record_ids_no_id_column(self):
        """Test extracting record IDs when column is missing."""
        data = [{"name": "Alice"}, {"name": "Bob"}]
        assert self.hook._extract_record_ids(data) == []

    def test_execute_post_extract(self):
        """Test provenance tracking at post_extract phase."""
        data = [
            {"id": "rec_1", "name": "Alice"},
            {"id": "rec_2", "name": "Bob"},
        ]
        ctx = self._make_context(POST_EXTRACT, data=data)
        self.hook.execute(ctx)

        all_prov = self.tracker.get_all_provenance()
        assert "rec_1" in all_prov
        assert "rec_2" in all_prov
        assert all_prov["rec_1"] == [POST_EXTRACT]
        assert all_prov["rec_2"] == [POST_EXTRACT]

    def test_execute_full_etl_cycle(self):
        """Test provenance tracking across a full ETL cycle."""
        data = [{"id": "rec_1", "name": "Alice"}]

        self.hook.execute(self._make_context(PRE_EXTRACT, data=None))
        self.hook.execute(self._make_context(POST_EXTRACT, data=data))
        self.hook.execute(self._make_context(PRE_TRANSFORM, data=data))
        self.hook.execute(self._make_context(POST_TRANSFORM, data=data))
        self.hook.execute(self._make_context(PRE_LOAD, data=data))
        self.hook.execute(self._make_context(POST_LOAD, data=data))

        all_prov = self.tracker.get_all_provenance()
        assert POST_EXTRACT in all_prov["rec_1"]
        assert POST_TRANSFORM in all_prov["rec_1"]
        assert POST_LOAD in all_prov["rec_1"]

    def test_execute_ignores_unknown_phases(self):
        """Test that unknown phases are ignored."""
        data = [{"id": "rec_1"}]
        ctx = self._make_context("on_complete", data=data)
        self.hook.execute(ctx)
        assert self.tracker.get_all_provenance() == {}

    def test_execute_no_data(self):
        """Test that execute with no data does nothing."""
        ctx = self._make_context(POST_EXTRACT, data=None)
        self.hook.execute(ctx)
        assert self.tracker.get_all_provenance() == {}

    def test_custom_column_name(self):
        """Test hook with a custom record ID column."""
        hook = ProvenanceHook(record_id_column="user_id")
        data = [{"user_id": "u1", "name": "Alice"}]
        ids = hook._extract_record_ids(data)
        assert ids == ["u1"]

    def test_provenance_tracker_internal_state(self):
        """Test that the internal ProvenanceTracker accumulates state."""
        data = [{"id": "rec_1"}, {"id": "rec_2"}]
        self.hook.execute(self._make_context(POST_EXTRACT, data=data))
        self.hook.execute(self._make_context(POST_TRANSFORM, data=data))

        internal = self.hook._provenance_tracker
        assert internal.get("rec_1") == [POST_EXTRACT, POST_TRANSFORM]
        assert internal.get("rec_2") == [POST_EXTRACT, POST_TRANSFORM]

    def test_uses_singleton_when_no_tracker(self):
        """Test that hook falls back to global singleton tracker."""
        hook = ProvenanceHook(record_id_column="id")
        data = [{"id": "rec_global"}]
        ctx = self._make_context(POST_EXTRACT, data=data)
        hook.execute(ctx)
        # Events should be in the global tracker
        global_tracker = get_lineage_tracker()
        provenance = global_tracker.get_provenance("rec_global")
        assert POST_EXTRACT in provenance
        # Clean up
        global_tracker.clear()
