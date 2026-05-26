"""
Tests for the data lineage and observability module.
"""

import json
import time
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock

from simpleetl.core.lineage import (
    LineageEvent,
    LineageTracker,
    LineageHook,
    DataFreshnessTracker,
    AlertRule,
    AlertManager,
    get_lineage_tracker,
    get_freshness_tracker,
    get_alert_manager,
    create_lineage_hook,
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
# LineageEvent tests
# ---------------------------------------------------------------------------

class TestLineageEvent:
    """Test LineageEvent creation and serialization."""

    def test_default_creation(self):
        """Test creating a LineageEvent with defaults."""
        event = LineageEvent()
        assert event.event_id is not None
        assert isinstance(event.timestamp, datetime)
        assert event.job_name == ""
        assert event.phase == ""
        assert event.input_rows == 0
        assert event.output_rows == 0
        assert event.duration_seconds == 0.0
        assert event.metadata == {}

    def test_creation_with_values(self):
        """Test creating a LineageEvent with specific values."""
        ts = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        event = LineageEvent(
            event_id="test-123",
            timestamp=ts,
            job_name="my_job",
            phase=POST_EXTRACT,
            source="s3://bucket/data.csv",
            destination="staging.table",
            operation="extract",
            input_schema={"id": "int", "name": "str"},
            output_schema={"id": "int", "name": "str", "email": "str"},
            input_rows=100,
            output_rows=95,
            duration_seconds=1.234,
            metadata={"key": "value"},
        )
        assert event.event_id == "test-123"
        assert event.timestamp == ts
        assert event.job_name == "my_job"
        assert event.phase == POST_EXTRACT
        assert event.source == "s3://bucket/data.csv"
        assert event.destination == "staging.table"
        assert event.operation == "extract"
        assert event.input_schema == {"id": "int", "name": "str"}
        assert event.output_schema == {"id": "int", "name": "str", "email": "str"}
        assert event.input_rows == 100
        assert event.output_rows == 95
        assert event.duration_seconds == 1.234
        assert event.metadata == {"key": "value"}

    def test_to_dict(self):
        """Test serialization to dictionary."""
        ts = datetime(2024, 6, 1, 10, 30, 0, tzinfo=timezone.utc)
        event = LineageEvent(
            event_id="abc",
            timestamp=ts,
            job_name="test_job",
            phase=POST_LOAD,
            input_rows=50,
            output_rows=50,
        )
        d = event.to_dict()
        assert d["event_id"] == "abc"
        assert d["timestamp"] == ts.isoformat()
        assert d["job_name"] == "test_job"
        assert d["phase"] == POST_LOAD
        assert d["input_rows"] == 50
        assert d["output_rows"] == 50
        assert isinstance(d, dict)

    def test_to_json(self):
        """Test serialization to JSON string."""
        ts = datetime(2024, 6, 1, 10, 30, 0, tzinfo=timezone.utc)
        event = LineageEvent(
            event_id="xyz",
            timestamp=ts,
            job_name="json_test",
        )
        json_str = event.to_json()
        parsed = json.loads(json_str)
        assert parsed["event_id"] == "xyz"
        assert parsed["job_name"] == "json_test"
        assert parsed["timestamp"] == ts.isoformat()

    def test_unique_event_ids(self):
        """Test that two events get different UUIDs by default."""
        event1 = LineageEvent()
        event2 = LineageEvent()
        assert event1.event_id != event2.event_id


# ---------------------------------------------------------------------------
# LineageTracker tests
# ---------------------------------------------------------------------------

class TestLineageTracker:
    """Test LineageTracker operations."""

    def setup_method(self):
        """Create a fresh tracker for each test."""
        self.tracker = LineageTracker()

    def test_record_event(self):
        """Test recording a single event."""
        event = LineageEvent(job_name="job1", phase=POST_EXTRACT)
        self.tracker.record_event(event)
        events = self.tracker.get_events()
        assert len(events) == 1
        assert events[0].job_name == "job1"

    def test_record_multiple_events(self):
        """Test recording multiple events."""
        for i in range(5):
            self.tracker.record_event(
                LineageEvent(job_name="job1", phase=POST_EXTRACT)
            )
        assert len(self.tracker.get_events()) == 5

    def test_get_events_filter_by_job(self):
        """Test filtering events by job name."""
        self.tracker.record_event(
            LineageEvent(job_name="job_a", phase=POST_EXTRACT)
        )
        self.tracker.record_event(
            LineageEvent(job_name="job_b", phase=POST_EXTRACT)
        )
        self.tracker.record_event(
            LineageEvent(job_name="job_a", phase=POST_LOAD)
        )
        job_a_events = self.tracker.get_events("job_a")
        assert len(job_a_events) == 2
        assert all(e.job_name == "job_a" for e in job_a_events)

    def test_get_events_no_filter(self):
        """Test getting all events without filter."""
        self.tracker.record_event(
            LineageEvent(job_name="x", phase=POST_EXTRACT)
        )
        self.tracker.record_event(
            LineageEvent(job_name="y", phase=POST_LOAD)
        )
        assert len(self.tracker.get_events()) == 2

    def test_get_events_nonexistent_job(self):
        """Test filtering by a job name with no events."""
        self.tracker.record_event(
            LineageEvent(job_name="real_job", phase=POST_EXTRACT)
        )
        assert self.tracker.get_events("nonexistent") == []

    def test_get_lineage(self):
        """Test building a lineage graph for a job."""
        self.tracker.record_event(
            LineageEvent(
                job_name="my_job",
                phase=POST_EXTRACT,
                output_rows=100,
                duration_seconds=1.0,
            )
        )
        self.tracker.record_event(
            LineageEvent(
                job_name="my_job",
                phase=POST_TRANSFORM,
                output_rows=95,
                duration_seconds=2.0,
            )
        )
        self.tracker.record_event(
            LineageEvent(
                job_name="my_job",
                phase=POST_LOAD,
                output_rows=95,
                duration_seconds=0.5,
            )
        )
        lineage = self.tracker.get_lineage("my_job")
        assert lineage["job_name"] == "my_job"
        assert len(lineage["events"]) == 3
        assert lineage["phases"] == [
            POST_EXTRACT, POST_TRANSFORM, POST_LOAD
        ]
        assert lineage["total_rows_processed"] == 290
        assert lineage["total_duration_seconds"] == 3.5

    def test_get_lineage_empty_job(self):
        """Test lineage for a job with no events."""
        lineage = self.tracker.get_lineage("empty_job")
        assert lineage["job_name"] == "empty_job"
        assert lineage["events"] == []
        assert lineage["phases"] == []
        assert lineage["total_rows_processed"] == 0
        assert lineage["total_duration_seconds"] == 0

    def test_to_dict(self):
        """Test serializing tracker to dict."""
        self.tracker.record_event(
            LineageEvent(job_name="j1", phase=POST_EXTRACT)
        )
        d = self.tracker.to_dict()
        assert "events" in d
        assert len(d["events"]) == 1
        assert d["events"][0]["job_name"] == "j1"

    def test_to_json(self):
        """Test serializing tracker to JSON."""
        self.tracker.record_event(
            LineageEvent(job_name="j1", phase=POST_EXTRACT)
        )
        json_str = self.tracker.to_json()
        parsed = json.loads(json_str)
        assert len(parsed["events"]) == 1

    def test_clear(self):
        """Test clearing all events."""
        self.tracker.record_event(
            LineageEvent(job_name="j1", phase=POST_EXTRACT)
        )
        self.tracker.record_event(
            LineageEvent(job_name="j2", phase=POST_LOAD)
        )
        assert len(self.tracker.get_events()) == 2
        self.tracker.clear()
        assert len(self.tracker.get_events()) == 0

    def test_summary(self):
        """Test summary statistics."""
        self.tracker.record_event(
            LineageEvent(
                job_name="job1",
                phase=POST_EXTRACT,
                output_rows=100,
                duration_seconds=1.0,
            )
        )
        self.tracker.record_event(
            LineageEvent(
                job_name="job1",
                phase=POST_LOAD,
                output_rows=100,
                duration_seconds=0.5,
            )
        )
        self.tracker.record_event(
            LineageEvent(
                job_name="job2",
                phase=POST_EXTRACT,
                output_rows=200,
                duration_seconds=2.0,
            )
        )
        summary = self.tracker.summary()
        assert summary["total_events"] == 3
        assert summary["total_rows_processed"] == 400
        assert summary["total_duration_seconds"] == 3.5
        assert set(summary["jobs"]) == {"job1", "job2"}

    def test_summary_empty(self):
        """Test summary with no events."""
        summary = self.tracker.summary()
        assert summary["total_events"] == 0
        assert summary["total_rows_processed"] == 0
        assert summary["total_duration_seconds"] == 0
        assert summary["jobs"] == []


# ---------------------------------------------------------------------------
# LineageHook tests
# ---------------------------------------------------------------------------

class TestLineageHook:
    """Test LineageHook integration with the hook system."""

    def setup_method(self):
        """Create a fresh tracker and hook for each test."""
        self.tracker = LineageTracker()
        self.hook = LineageHook(tracker=self.tracker, job_name="test_job")

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
        assert self.hook.name == "lineage"

    def test_pre_extract_stores_start_time(self):
        """Test that pre_extract records start time."""
        ctx = self._make_context(PRE_EXTRACT)
        self.hook.execute(ctx)
        assert "pre_extract" in self.hook._phase_starts

    def test_post_extract_records_event(self):
        """Test that post_extract records a lineage event."""
        self.hook.execute(self._make_context(PRE_EXTRACT))
        time.sleep(0.01)
        self.hook.execute(self._make_context(POST_EXTRACT))
        events = self.tracker.get_events()
        assert len(events) == 1
        assert events[0].phase == POST_EXTRACT
        assert events[0].job_name == "test_job"
        assert events[0].duration_seconds >= 0.01

    def test_post_transform_records_event(self):
        """Test that post_transform records a lineage event."""
        self.hook.execute(self._make_context(PRE_TRANSFORM))
        self.hook.execute(self._make_context(POST_TRANSFORM))
        events = self.tracker.get_events()
        assert len(events) == 1
        assert events[0].phase == POST_TRANSFORM

    def test_post_load_records_event(self):
        """Test that post_load records a lineage event."""
        self.hook.execute(self._make_context(PRE_LOAD))
        self.hook.execute(self._make_context(POST_LOAD))
        events = self.tracker.get_events()
        assert len(events) == 1
        assert events[0].phase == POST_LOAD

    def test_full_etl_cycle(self):
        """Test recording events across a full ETL cycle."""
        self.hook.execute(self._make_context(PRE_EXTRACT))
        self.hook.execute(self._make_context(POST_EXTRACT))

        self.hook.execute(self._make_context(PRE_TRANSFORM))
        self.hook.execute(self._make_context(POST_TRANSFORM))

        self.hook.execute(self._make_context(PRE_LOAD))
        self.hook.execute(self._make_context(POST_LOAD))

        events = self.tracker.get_events()
        assert len(events) == 3
        phases = [e.phase for e in events]
        assert POST_EXTRACT in phases
        assert POST_TRANSFORM in phases
        assert POST_LOAD in phases

    def test_no_event_for_unknown_phase(self):
        """Test that unknown phases don't produce events."""
        ctx = self._make_context("on_complete")
        self.hook.execute(ctx)
        assert len(self.tracker.get_events()) == 0

    def test_no_event_for_on_error(self):
        """Test that on_error phase doesn't produce lineage events."""
        ctx = self._make_context("on_error")
        self.hook.execute(ctx)
        assert len(self.tracker.get_events()) == 0

    def test_metadata_preserved(self):
        """Test that context metadata is preserved in the event."""
        meta = {"extracted_rows": 42, "custom_key": "custom_val"}
        self.hook.execute(self._make_context(PRE_EXTRACT))
        self.hook.execute(self._make_context(POST_EXTRACT, metadata=meta))
        event = self.tracker.get_events()[0]
        assert event.metadata["extracted_rows"] == 42
        assert event.metadata["custom_key"] == "custom_val"

    def test_input_rows_from_metadata(self):
        """Test that input_rows is extracted from metadata."""
        meta = {"extracted_rows": 150}
        self.hook.execute(self._make_context(PRE_EXTRACT))
        self.hook.execute(self._make_context(POST_EXTRACT, metadata=meta))
        event = self.tracker.get_events()[0]
        assert event.input_rows == 150

    def test_uses_singleton_when_no_tracker(self):
        """Test that hook falls back to global singleton tracker."""
        hook = LineageHook(job_name="singleton_test")
        ctx = self._make_context(PRE_EXTRACT)
        hook.execute(ctx)
        hook.execute(self._make_context(POST_EXTRACT))
        # Events should be in the global tracker
        global_tracker = get_lineage_tracker()
        events = global_tracker.get_events("singleton_test")
        assert len(events) == 1
        # Clean up
        global_tracker.clear()


# ---------------------------------------------------------------------------
# DataFreshnessTracker tests
# ---------------------------------------------------------------------------

class TestDataFreshnessTracker:
    """Test DataFreshnessTracker operations."""

    def setup_method(self):
        """Create a fresh tracker for each test."""
        self.tracker = DataFreshnessTracker()

    def test_record_freshness_default_timestamp(self):
        """Test recording freshness with default timestamp."""
        before = datetime.now(timezone.utc)
        self.tracker.record_freshness("source_a")
        after = datetime.now(timezone.utc)
        ts = self.tracker.get_freshness("source_a")
        assert ts is not None
        assert before <= ts <= after

    def test_record_freshness_custom_timestamp(self):
        """Test recording freshness with a custom timestamp."""
        ts = datetime(2024, 3, 15, 12, 0, 0, tzinfo=timezone.utc)
        self.tracker.record_freshness("source_b", timestamp=ts)
        assert self.tracker.get_freshness("source_b") == ts

    def test_get_freshness_unknown_source(self):
        """Test getting freshness for an unknown source returns None."""
        assert self.tracker.get_freshness("nonexistent") is None

    def test_is_stale_unknown_source(self):
        """Test that an unknown source is considered stale."""
        assert self.tracker.is_stale("unknown_source", max_age_seconds=60)

    def test_is_stale_fresh_source(self):
        """Test that a recently updated source is not stale."""
        self.tracker.record_freshness("fresh_source")
        assert not self.tracker.is_stale(
            "fresh_source", max_age_seconds=60
        )

    def test_is_stale_old_source(self):
        """Test that an old source is considered stale."""
        old_ts = datetime.now(timezone.utc) - timedelta(seconds=120)
        self.tracker.record_freshness("old_source", timestamp=old_ts)
        assert self.tracker.is_stale("old_source", max_age_seconds=60)

    def test_is_stale_exact_boundary(self):
        """Test staleness at the exact boundary."""
        old_ts = datetime.now(timezone.utc) - timedelta(seconds=60)
        self.tracker.record_freshness("boundary_source", timestamp=old_ts)
        # At 60s with max_age_seconds=120 (generous buffer), should not be stale
        assert not self.tracker.is_stale(
            "boundary_source", max_age_seconds=120
        )

    def test_get_all_freshness(self):
        """Test getting all freshness records."""
        self.tracker.record_freshness("src1")
        self.tracker.record_freshness("src2")
        all_fresh = self.tracker.get_all_freshness()
        assert "src1" in all_fresh
        assert "src2" in all_fresh
        assert len(all_fresh) == 2

    def test_get_all_freshness_empty(self):
        """Test getting all freshness when nothing is tracked."""
        assert self.tracker.get_all_freshness() == {}

    def test_summary(self):
        """Test freshness summary."""
        ts_old = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        ts_new = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
        self.tracker.record_freshness("alpha", timestamp=ts_old)
        self.tracker.record_freshness("beta", timestamp=ts_new)
        summary = self.tracker.summary()
        assert summary["total_sources"] == 2
        assert set(summary["sources"]) == {"alpha", "beta"}
        assert summary["stalest_source"] == "alpha"
        assert summary["newest_source"] == "beta"

    def test_summary_empty(self):
        """Test summary with no tracked sources."""
        summary = self.tracker.summary()
        assert summary["total_sources"] == 0
        assert summary["sources"] == []
        assert summary["stalest_source"] is None
        assert summary["newest_source"] is None

    def test_overwrite_freshness(self):
        """Test that recording freshness for the same source overwrites."""
        ts1 = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        ts2 = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
        self.tracker.record_freshness("src", timestamp=ts1)
        self.tracker.record_freshness("src", timestamp=ts2)
        assert self.tracker.get_freshness("src") == ts2


# ---------------------------------------------------------------------------
# AlertRule tests
# ---------------------------------------------------------------------------

class TestAlertRule:
    """Test AlertRule creation."""

    def test_default_creation(self):
        """Test creating an alert rule with defaults."""
        rule = AlertRule(
            name="test_alert",
            condition=lambda ctx: True,
        )
        assert rule.name == "test_alert"
        assert rule.severity == "warning"
        assert rule.message_template == "Alert '{name}' triggered"
        assert rule.channels == []

    def test_creation_with_values(self):
        """Test creating an alert rule with all values."""
        rule = AlertRule(
            name="critical_alert",
            condition=lambda ctx: ctx.get("errors", 0) > 0,
            severity="critical",
            message_template="Critical: {name} - {errors} errors",
            channels=["email", "slack"],
        )
        assert rule.name == "critical_alert"
        assert rule.severity == "critical"
        assert rule.message_template == "Critical: {name} - {errors} errors"
        assert rule.channels == ["email", "slack"]


# ---------------------------------------------------------------------------
# AlertManager tests
# ---------------------------------------------------------------------------

class TestAlertManager:
    """Test AlertManager operations."""

    def setup_method(self):
        """Create a fresh alert manager for each test."""
        self.manager = AlertManager()

    def test_add_rule(self):
        """Test adding an alert rule."""
        rule = AlertRule(name="r1", condition=lambda ctx: True)
        self.manager.add_rule(rule)
        assert len(self.manager._rules) == 1

    def test_add_multiple_rules(self):
        """Test adding multiple alert rules."""
        self.manager.add_rule(
            AlertRule(name="r1", condition=lambda ctx: True)
        )
        self.manager.add_rule(
            AlertRule(name="r2", condition=lambda ctx: False)
        )
        assert len(self.manager._rules) == 2

    def test_check_alerts_no_rules(self):
        """Test checking alerts with no rules returns empty list."""
        result = self.manager.check_alerts({"errors": 10})
        assert result == []

    def test_check_alerts_triggered(self):
        """Test that a triggered alert returns a message."""
        self.manager.add_rule(
            AlertRule(
                name="high_errors",
                condition=lambda ctx: ctx.get("errors", 0) > 5,
            )
        )
        result = self.manager.check_alerts({"errors": 10})
        assert len(result) == 1
        assert "high_errors" in result[0]

    def test_check_alerts_not_triggered(self):
        """Test that a non-triggered alert returns no message."""
        self.manager.add_rule(
            AlertRule(
                name="high_errors",
                condition=lambda ctx: ctx.get("errors", 0) > 5,
            )
        )
        result = self.manager.check_alerts({"errors": 2})
        assert result == []

    def test_check_alerts_multiple_triggered(self):
        """Test multiple triggered alerts."""
        self.manager.add_rule(
            AlertRule(
                name="rule_a",
                condition=lambda ctx: ctx.get("x", 0) > 0,
            )
        )
        self.manager.add_rule(
            AlertRule(
                name="rule_b",
                condition=lambda ctx: ctx.get("y", 0) > 0,
            )
        )
        result = self.manager.check_alerts({"x": 1, "y": 1})
        assert len(result) == 2

    def test_check_alerts_partial_trigger(self):
        """Test that only matching rules trigger."""
        self.manager.add_rule(
            AlertRule(
                name="fires",
                condition=lambda ctx: True,
            )
        )
        self.manager.add_rule(
            AlertRule(
                name="no_fire",
                condition=lambda ctx: False,
            )
        )
        result = self.manager.check_alerts({})
        assert len(result) == 1
        assert "fires" in result[0]

    def test_message_template_formatting(self):
        """Test that message templates are formatted with context."""
        self.manager.add_rule(
            AlertRule(
                name="err_alert",
                condition=lambda ctx: True,
                message_template="{name}: {count} items",
            )
        )
        result = self.manager.check_alerts({"count": 42})
        assert result[0] == "err_alert: 42 items"

    def test_clear_rules(self):
        """Test clearing all rules."""
        self.manager.add_rule(
            AlertRule(name="r1", condition=lambda ctx: True)
        )
        self.manager.add_rule(
            AlertRule(name="r2", condition=lambda ctx: True)
        )
        self.manager.clear_rules()
        assert len(self.manager._rules) == 0

    def test_condition_exception_handled(self):
        """Test that exceptions in conditions are handled gracefully."""
        def bad_condition(ctx):
            raise ValueError("boom")

        self.manager.add_rule(
            AlertRule(name="bad", condition=bad_condition)
        )
        result = self.manager.check_alerts({})
        assert result == []

    def test_severity_in_message(self):
        """Test that severity is available in message template."""
        self.manager.add_rule(
            AlertRule(
                name="sev_test",
                condition=lambda ctx: True,
                severity="critical",
                message_template="{name} [{severity}]",
            )
        )
        result = self.manager.check_alerts({})
        assert result[0] == "sev_test [critical]"


# ---------------------------------------------------------------------------
# Module-level singleton tests
# ---------------------------------------------------------------------------

class TestSingletons:
    """Test module-level singleton functions."""

    def test_get_lineage_tracker_returns_instance(self):
        """Test that get_lineage_tracker returns a LineageTracker."""
        tracker = get_lineage_tracker()
        assert isinstance(tracker, LineageTracker)

    def test_get_lineage_tracker_returns_same_instance(self):
        """Test that get_lineage_tracker returns the same singleton."""
        t1 = get_lineage_tracker()
        t2 = get_lineage_tracker()
        assert t1 is t2

    def test_get_freshness_tracker_returns_instance(self):
        """Test that get_freshness_tracker returns a DataFreshnessTracker."""
        tracker = get_freshness_tracker()
        assert isinstance(tracker, DataFreshnessTracker)

    def test_get_freshness_tracker_returns_same_instance(self):
        """Test that get_freshness_tracker returns the same singleton."""
        t1 = get_freshness_tracker()
        t2 = get_freshness_tracker()
        assert t1 is t2

    def test_get_alert_manager_returns_instance(self):
        """Test that get_alert_manager returns an AlertManager."""
        mgr = get_alert_manager()
        assert isinstance(mgr, AlertManager)

    def test_get_alert_manager_returns_same_instance(self):
        """Test that get_alert_manager returns the same singleton."""
        m1 = get_alert_manager()
        m2 = get_alert_manager()
        assert m1 is m2

    def test_create_lineage_hook(self):
        """Test create_lineage_hook returns a LineageHook."""
        hook = create_lineage_hook("my_job")
        assert isinstance(hook, LineageHook)
        assert hook._job_name == "my_job"

    def test_create_lineage_hook_default_name(self):
        """Test create_lineage_hook with default job name."""
        hook = create_lineage_hook()
        assert hook._job_name == ""
