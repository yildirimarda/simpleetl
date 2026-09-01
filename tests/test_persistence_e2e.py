"""
End-to-end persistence verification for Lineage, Audit, and RBAC.

Verifies that all three systems can persist to files, be restored,
and work together in an integrated ETL scenario.
"""

import json
import os

from simpleetl.core.lineage import (
    LineageEvent,
    LineageTracker,
    FileLineageStore,
    LineageHook,
    configure_lineage_persistence,
    ProvenanceHook,
    get_lineage_tracker,
    get_file_lineage_store,
)
from simpleetl.core.security import AuditLogger, RBACPolicy, apply_rbac_filter
from simpleetl.core.hooks import HookContext, POST_EXTRACT, POST_TRANSFORM, POST_LOAD


class TestPersistenceEndToEnd:
    """End-to-end persistence for lineage, audit, and RBAC."""

    def test_lineage_audit_rbac_round_trip_together(self, tmp_path):
        """Full persistence round-trip for all three systems."""
        # File paths
        lineage_path = str(tmp_path / "lineage.jsonl")
        audit_path = str(tmp_path / "audit.jsonl")
        rbac_path = str(tmp_path / "rbac.json")

        # 1. Lineage persistence
        tracker = LineageTracker()
        event = LineageEvent(
            job_name="e2e_job",
            phase=POST_EXTRACT,
            source="s3://bucket/input.csv",
            destination="postgresql://db/table",
            operation="extract",
            input_rows=100,
            output_rows=95,
            duration_seconds=1.234,
            metadata={"env": "test"},
            record_provenance={"rec_1": ["extract_filter"]},
        )
        tracker.record_event(event)
        tracker.to_file(lineage_path)

        # Load lineage back
        loaded_tracker = LineageTracker.from_file(lineage_path)
        loaded_events = loaded_tracker.get_events()
        assert len(loaded_events) == 1
        assert loaded_events[0].job_name == "e2e_job"
        assert loaded_events[0].record_provenance == {"rec_1": ["extract_filter"]}

        # 2. Audit persistence
        audit = AuditLogger(log_file=audit_path)
        audit.log_access("alice", "read", "customers", {"query": "SELECT *"})
        audit.log_transformation(
            user="alice",
            job_name="e2e_job",
            operation="filter",
            source="customers",
            destination="filtered_customers",
            details={"filter": "age >= 18"},
        )

        # Load audit back
        loaded_audit = AuditLogger.from_file(audit_path, log_file=audit_path)
        trail = loaded_audit.get_audit_trail()
        assert len(trail) == 2
        assert trail[0]["user"] == "alice"
        assert trail[0]["action"] == "read"
        assert trail[1]["event_type"] == "transformation"
        assert trail[1]["operation"] == "filter"

        # 3. RBAC persistence
        policy = RBACPolicy()
        policy.add_role(
            "analyst",
            permissions=["read", "transform"],
            allowed_columns={"customers": ["id", "name", "age"]},
        )
        policy.save_to_file(rbac_path)

        loaded_policy = RBACPolicy.load_from_file(rbac_path)
        assert loaded_policy.check_access("analyst", "read") is True
        assert loaded_policy.check_access("analyst", "transform") is True
        assert loaded_policy.check_access("analyst", "delete") is False
        allowed = loaded_policy.filter_columns(
            "analyst", "customers", ["id", "name", "ssn", "email"]
        )
        assert allowed == ["id", "name"]

    def test_file_lineage_store_persist_and_restore(self, tmp_path):
        """FileLineageStore writes immediately and survives reload."""
        path = str(tmp_path / "store.jsonl")
        store = FileLineageStore(file_path=path)
        event = LineageEvent(
            job_name="store_test",
            phase=POST_TRANSFORM,
            output_rows=42,
        )
        store.record_event(event)
        store.close()

        # The store writes to the same file; verify file exists and has content
        assert os.path.exists(path)
        with open(path) as fh:
            lines = fh.read().strip().splitlines()
        assert len(lines) == 1
        data = json.loads(lines[0])
        assert data["job_name"] == "store_test"
        assert data["phase"] == POST_TRANSFORM

    def test_configure_lineage_persistence_end_to_end(self, tmp_path):
        """Module-level persistence config writes and reloads correctly."""
        path = str(tmp_path / "persisted.jsonl")
        store = configure_lineage_persistence(path, auto_flush=True)

        tracker = get_lineage_tracker()
        tracker.clear()
        event = LineageEvent(
            job_name="configured",
            phase=POST_LOAD,
            destination="db.table",
        )
        tracker.record_event(event)

        # Persist through the configured file store
        store = get_file_lineage_store()
        assert store is not None
        store.record_event(event)

        # Verify file exists and has content
        assert os.path.exists(path)
        with open(path) as fh:
            lines = [line.strip() for line in fh if line.strip()]
        assert len(lines) == 1
        data = json.loads(lines[0])
        assert data["job_name"] == "configured"

    def test_lineage_hook_with_persistence(self, tmp_path):
        """LineageHook writes events that survive file reload."""
        tracker = LineageTracker()
        hook = LineageHook(tracker=tracker, job_name="hook_e2e")

        # Pre phase
        pre_ctx = HookContext(
            job=None,
            phase="pre_extract",
            data=None,
            metadata={},
        )
        hook.execute(pre_ctx)

        # Post phase with data
        import pandas as pd

        df = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        post_ctx = HookContext(
            job=None,
            phase="post_extract",
            data=df,
            metadata={"extracted_rows": 3},
        )
        hook.execute(post_ctx)

        # Persist
        persist_path = str(tmp_path / "hook_events.jsonl")
        tracker.to_file(persist_path)

        # Reload and verify event preserved
        reloaded = LineageTracker.from_file(persist_path)
        events = reloaded.get_events("hook_e2e")
        assert len(events) == 1
        assert events[0].phase == POST_EXTRACT
        assert events[0].output_rows == 3
        assert events[0].metadata == {"extracted_rows": 3}

    def test_provenance_hook_with_persistence(self, tmp_path):
        """ProvenanceHook records provenance that survives reload."""
        tracker = LineageTracker()
        hook = ProvenanceHook(record_id_column="id", tracker=tracker)
        data = [{"id": "r1", "name": "Alice"}, {"id": "r2", "name": "Bob"}]
        ctx = HookContext(
            job=None,
            phase=POST_TRANSFORM,
            data=data,
            metadata={},
        )
        hook.execute(ctx)

        # Record provenance into file
        persist_path = str(tmp_path / "provenance.jsonl")
        event = LineageEvent(job_name="prov", phase=POST_TRANSFORM)
        tracker.record_event(event)
        tracker.record_provenance("r1", "map:name", event_id=event.event_id)
        tracker.to_file(persist_path)

        # Reload and check provenance preserved via event records
        reloaded = LineageTracker.from_file(persist_path)
        events = reloaded.get_events()
        assert len(events) == 1
        assert events[0].record_provenance == {"r1": ["map:name"]}

    def test_rbac_filter_persisted_policy_applies_correctly(self, tmp_path):
        """RBAC policy saved, loaded, then applied to DataFrame."""
        import pandas as pd

        path = str(tmp_path / "rbac_filter_test.json")
        policy = RBACPolicy()
        policy.add_role(
            "restricted",
            permissions=["read"],
            allowed_columns={"table": ["id"]},
        )
        policy.save_to_file(path)

        loaded = RBACPolicy.load_from_file(path)
        df = pd.DataFrame({"id": [1], "secret": ["x"], "name": ["Alice"]})
        result = apply_rbac_filter(df, "restricted", "table", loaded)
        assert list(result.columns) == ["id"]
