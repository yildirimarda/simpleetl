"""
Tests for DAG-based job orchestration.
"""

import os
import tempfile
from datetime import datetime, timezone

import pytest
import yaml

from simpleetl.core.dag import (
    DAG,
    DAGCycleError,
    DAGMissingDependencyError,
    DAGResult,
    DAGRunner,
    JobNode,
    NodeResult,
    NodeStatus,
)
from simpleetl.core.schedule import (
    CronExpression,
    CronParseError,
    Schedule,
)


# ---------------------------------------------------------------------------
# JobNode tests
# ---------------------------------------------------------------------------

class TestJobNode:
    """Tests for the JobNode dataclass."""

    def test_create_basic_node(self):
        """Test creating a basic node."""
        node = JobNode(name="test_node")
        assert node.name == "test_node"
        assert node.status == NodeStatus.PENDING
        assert node.dependencies == []
        assert node.upstream == set()
        assert node.downstream == set()

    def test_create_full_node(self):
        """Test creating a node with all fields."""
        node = JobNode(
            name="extract",
            job_class="my_module.ExtractJob",
            config_path="extract.yaml",
            params={"key": "value"},
            dependencies=["source"],
        )
        assert node.job_class == "my_module.ExtractJob"
        assert node.config_path == "extract.yaml"
        assert node.params == {"key": "value"}
        assert node.dependencies == ["source"]

    def test_node_equality(self):
        """Test that nodes are compared by name."""
        a = JobNode(name="same")
        b = JobNode(name="same")
        c = JobNode(name="different")
        assert a == b
        assert a != c
        assert hash(a) == hash(b)

    def test_node_status_transitions(self):
        """Test that status can be updated."""
        node = JobNode(name="test")
        assert node.status == NodeStatus.PENDING
        node.status = NodeStatus.RUNNING
        assert node.status == NodeStatus.RUNNING
        node.status = NodeStatus.SUCCESS
        assert node.status == NodeStatus.SUCCESS


# ---------------------------------------------------------------------------
# DAG tests
# ---------------------------------------------------------------------------

class TestDAG:
    """Tests for the DAG class."""

    def _make_dag(self) -> DAG:
        """Helper: create a simple 3-node DAG: a -> b -> c."""
        dag = DAG("test")
        dag.add_node(JobNode(name="a"))
        dag.add_node(JobNode(name="b", dependencies=["a"]))
        dag.add_node(JobNode(name="c", dependencies=["b"]))
        dag.add_edge("a", "b")
        dag.add_edge("b", "c")
        return dag

    def test_add_node(self):
        """Test adding nodes."""
        dag = DAG("test")
        dag.add_node(JobNode(name="x"))
        assert "x" in dag.nodes

    def test_add_duplicate_node_raises(self):
        """Test that duplicate node names raise ValueError."""
        dag = DAG("test")
        dag.add_node(JobNode(name="x"))
        with pytest.raises(ValueError, match="already exists"):
            dag.add_node(JobNode(name="x"))

    def test_add_edge(self):
        """Test adding edges."""
        dag = self._make_dag()
        assert "b" in dag.get_node("a").downstream
        assert "a" in dag.get_node("b").upstream

    def test_add_edge_missing_node_raises(self):
        """Test that edges to missing nodes raise."""
        dag = DAG("test")
        dag.add_node(JobNode(name="a"))
        with pytest.raises(DAGMissingDependencyError):
            dag.add_edge("a", "nonexistent")
        with pytest.raises(DAGMissingDependencyError):
            dag.add_edge("nonexistent", "a")

    def test_get_node(self):
        """Test retrieving a node."""
        dag = self._make_dag()
        node = dag.get_node("b")
        assert node.name == "b"

    def test_get_node_missing_raises(self):
        """Test that getting a missing node raises KeyError."""
        dag = DAG("test")
        with pytest.raises(KeyError):
            dag.get_node("missing")

    def test_validate_success(self):
        """Test validation of a valid DAG."""
        dag = self._make_dag()
        dag.validate()  # Should not raise

    def test_validate_missing_dependency(self):
        """Test validation catches missing dependencies."""
        dag = DAG("test")
        dag.add_node(JobNode(name="a", dependencies=["missing"]))
        with pytest.raises(DAGMissingDependencyError, match="does not exist"):
            dag.validate()

    def test_validate_cycle_self(self):
        """Test that self-loop is detected as a cycle."""
        dag = DAG("test")
        dag.add_node(JobNode(name="a"))
        dag.add_edge("a", "a")
        with pytest.raises(DAGCycleError):
            dag.validate()

    def test_validate_cycle_two_nodes(self):
        """Test that a 2-node cycle is detected."""
        dag = DAG("test")
        dag.add_node(JobNode(name="a"))
        dag.add_node(JobNode(name="b"))
        dag.add_edge("a", "b")
        dag.add_edge("b", "a")
        with pytest.raises(DAGCycleError):
            dag.validate()

    def test_validate_cycle_three_nodes(self):
        """Test that a 3-node cycle is detected."""
        dag = DAG("test")
        dag.add_node(JobNode(name="a"))
        dag.add_node(JobNode(name="b"))
        dag.add_node(JobNode(name="c"))
        dag.add_edge("a", "b")
        dag.add_edge("b", "c")
        dag.add_edge("c", "a")
        with pytest.raises(DAGCycleError):
            dag.validate()

    def test_topological_sort_linear(self):
        """Test topological sort on a linear DAG."""
        dag = self._make_dag()
        order = dag.topological_sort()
        assert order == ["a", "b", "c"]

    def test_topological_sort_diamond(self):
        """Test topological sort on a diamond DAG: a -> b, a -> c, b -> d, c -> d."""
        dag = DAG("diamond")
        dag.add_node(JobNode(name="a"))
        dag.add_node(JobNode(name="b", dependencies=["a"]))
        dag.add_node(JobNode(name="c", dependencies=["a"]))
        dag.add_node(JobNode(name="d", dependencies=["b", "c"]))
        dag.add_edge("a", "b")
        dag.add_edge("a", "c")
        dag.add_edge("b", "d")
        dag.add_edge("c", "d")
        order = dag.topological_sort()
        assert order.index("a") < order.index("b")
        assert order.index("a") < order.index("c")
        assert order.index("b") < order.index("d")
        assert order.index("c") < order.index("d")

    def test_topological_sort_cycle_raises(self):
        """Test that topological sort raises on cycle."""
        dag = DAG("test")
        dag.add_node(JobNode(name="a"))
        dag.add_node(JobNode(name="b"))
        dag.add_edge("a", "b")
        dag.add_edge("b", "a")
        with pytest.raises(DAGCycleError):
            dag.topological_sort()

    def test_get_parallel_groups_linear(self):
        """Test parallel groups on a linear DAG (each node in its own group)."""
        dag = self._make_dag()
        groups = dag.get_parallel_groups()
        assert groups == [["a"], ["b"], ["c"]]

    def test_get_parallel_groups_diamond(self):
        """Test parallel groups on a diamond DAG."""
        dag = DAG("diamond")
        dag.add_node(JobNode(name="a"))
        dag.add_node(JobNode(name="b", dependencies=["a"]))
        dag.add_node(JobNode(name="c", dependencies=["a"]))
        dag.add_node(JobNode(name="d", dependencies=["b", "c"]))
        dag.add_edge("a", "b")
        dag.add_edge("a", "c")
        dag.add_edge("b", "d")
        dag.add_edge("c", "d")
        groups = dag.get_parallel_groups()
        assert groups == [["a"], ["b", "c"], ["d"]]

    def test_get_parallel_groups_independent(self):
        """Test parallel groups with independent nodes."""
        dag = DAG("independent")
        dag.add_node(JobNode(name="a"))
        dag.add_node(JobNode(name="b"))
        dag.add_node(JobNode(name="c"))
        groups = dag.get_parallel_groups()
        assert groups == [["a", "b", "c"]]

    def test_get_execution_plan(self):
        """Test execution plan generation."""
        dag = self._make_dag()
        plan = dag.get_execution_plan()
        assert plan["name"] == "test"
        assert plan["topological_order"] == ["a", "b", "c"]
        assert "nodes" in plan
        assert "a" in plan["nodes"]

    def test_from_dict(self):
        """Test building DAG from a dictionary."""
        data = {
            "name": "my_dag",
            "jobs": [
                {
                    "name": "extract",
                    "job_class": "my.ExtractJob",
                    "config_path": "extract.yaml",
                },
                {
                    "name": "transform",
                    "job_class": "my.TransformJob",
                    "config_path": "transform.yaml",
                    "dependencies": ["extract"],
                },
            ],
        }
        dag = DAG.from_dict(data)
        assert dag.name == "my_dag"
        assert len(dag.nodes) == 2
        assert "transform" in dag.get_node("extract").downstream

    def test_from_yaml(self):
        """Test loading DAG from a YAML file."""
        data = {
            "name": "yaml_dag",
            "jobs": [
                {"name": "a"},
                {"name": "b", "dependencies": ["a"]},
            ],
        }
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            yaml.dump(data, f)
            temp_path = f.name

        try:
            dag = DAG.from_yaml(temp_path)
            assert dag.name == "yaml_dag"
            assert len(dag.nodes) == 2
        finally:
            os.unlink(temp_path)


# ---------------------------------------------------------------------------
# DAGRunner tests
# ---------------------------------------------------------------------------

class FailingJob:
    """A mock job that always fails."""

    def __init__(self, config):
        self.config = config

    def run_with_error_handling(self):
        raise RuntimeError("Intentional failure")


# Module-level call tracker for SlowJob (avoids class-variable issues with
# dynamic imports across ThreadPoolExecutor boundaries).
_slow_job_calls: list = []


class SlowJob:
    """A mock job that tracks calls."""

    def __init__(self, config):
        self.config = config

    def run_with_error_handling(self):
        _slow_job_calls.append(
            self.config.name if hasattr(self.config, "name") else "unknown"
        )


class TestDAGRunner:
    """Tests for the DAGRunner class."""

    def test_run_all_success(self):
        """Test running a DAG where all nodes succeed."""
        dag = DAG("test")
        dag.add_node(
            JobNode(
                name="a",
                job_class="tests.test_dag.SlowJob",
            )
        )
        dag.add_node(
            JobNode(
                name="b",
                job_class="tests.test_dag.SlowJob",
                dependencies=["a"],
            )
        )
        dag.add_edge("a", "b")

        _slow_job_calls.clear()
        runner = DAGRunner()
        result = runner.run(dag)

        assert result.status == "success"
        assert len(result.node_results) == 2
        assert result.node_results["a"].status == NodeStatus.SUCCESS
        assert result.node_results["b"].status == NodeStatus.SUCCESS
        assert result.duration > 0

    def test_run_fail_fast(self):
        """Test fail-fast mode: downstream nodes are skipped on failure."""
        dag = DAG("test")
        dag.add_node(
            JobNode(
                name="a",
                job_class="tests.test_dag.FailingJob",
            )
        )
        dag.add_node(
            JobNode(
                name="b",
                job_class="tests.test_dag.SlowJob",
                dependencies=["a"],
            )
        )
        dag.add_edge("a", "b")

        runner = DAGRunner(fail_fast=True)
        result = runner.run(dag)

        assert result.status == "failed"
        assert result.node_results["a"].status == NodeStatus.FAILED
        assert result.node_results["b"].status == NodeStatus.SKIPPED
        assert "a" in result.failed_nodes
        assert "b" in result.skipped_nodes

    def test_run_continue_on_error(self):
        """Test continue-on-error mode: independent branches still run."""
        # a -> b, c (c is independent of a)
        dag = DAG("test")
        dag.add_node(
            JobNode(
                name="a",
                job_class="tests.test_dag.FailingJob",
            )
        )
        dag.add_node(
            JobNode(
                name="b",
                job_class="tests.test_dag.SlowJob",
                dependencies=["a"],
            )
        )
        dag.add_node(
            JobNode(
                name="c",
                job_class="tests.test_dag.SlowJob",
            )
        )
        dag.add_edge("a", "b")

        runner = DAGRunner(fail_fast=False)
        result = runner.run(dag)

        assert result.status == "failed"
        assert result.node_results["a"].status == NodeStatus.FAILED
        assert result.node_results["b"].status == NodeStatus.SKIPPED
        # c is independent and should succeed
        assert result.node_results["c"].status == NodeStatus.SUCCESS

    def test_run_parallel(self):
        """Test parallel execution within a group."""
        dag = DAG("test")
        dag.add_node(
            JobNode(
                name="a",
                job_class="tests.test_dag.SlowJob",
            )
        )
        dag.add_node(
            JobNode(
                name="b",
                job_class="tests.test_dag.SlowJob",
            )
        )

        runner = DAGRunner(max_parallel=2)
        result = runner.run(dag)

        assert result.status == "success"
        assert result.node_results["a"].status == NodeStatus.SUCCESS
        assert result.node_results["b"].status == NodeStatus.SUCCESS

    def test_node_result_error_message(self):
        """Test that error messages are captured in NodeResult."""
        dag = DAG("test")
        dag.add_node(
            JobNode(
                name="a",
                job_class="tests.test_dag.FailingJob",
            )
        )

        runner = DAGRunner()
        result = runner.run(dag)

        assert result.node_results["a"].error is not None
        assert "Intentional failure" in result.node_results["a"].error

    def test_dag_result_properties(self):
        """Test DAGResult helper properties."""
        result = DAGResult(
            status="failed",
            node_results={
                "a": NodeResult("a", NodeStatus.FAILED, error="err"),
                "b": NodeResult("b", NodeStatus.SKIPPED),
                "c": NodeResult("c", NodeStatus.SUCCESS),
            },
        )
        assert result.failed_nodes == ["a"]
        assert result.skipped_nodes == ["b"]


# ---------------------------------------------------------------------------
# CronExpression tests
# ---------------------------------------------------------------------------

class TestCronExpression:
    """Tests for cron expression parsing and matching."""

    def test_wildcard(self):
        """Test wildcard matches any value."""
        cron = CronExpression("* * * * *")
        dt = datetime(2024, 6, 15, 14, 30, tzinfo=timezone.utc)
        assert cron.matches(dt)

    def test_specific_minute(self):
        """Test specific minute matching."""
        cron = CronExpression("30 * * * *")
        assert cron.matches(datetime(2024, 1, 1, 0, 30, tzinfo=timezone.utc))
        assert not cron.matches(datetime(2024, 1, 1, 0, 15, tzinfo=timezone.utc))

    def test_specific_hour(self):
        """Test specific hour matching."""
        cron = CronExpression("0 14 * * *")
        assert cron.matches(datetime(2024, 1, 1, 14, 0, tzinfo=timezone.utc))
        assert not cron.matches(datetime(2024, 1, 1, 15, 0, tzinfo=timezone.utc))

    def test_range(self):
        """Test range matching."""
        cron = CronExpression("0 9-17 * * *")
        assert cron.matches(datetime(2024, 1, 1, 9, 0, tzinfo=timezone.utc))
        assert cron.matches(datetime(2024, 1, 1, 17, 0, tzinfo=timezone.utc))
        assert not cron.matches(datetime(2024, 1, 1, 18, 0, tzinfo=timezone.utc))

    def test_step(self):
        """Test step matching."""
        cron = CronExpression("*/15 * * * *")
        assert cron.matches(datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc))
        assert cron.matches(datetime(2024, 1, 1, 0, 15, tzinfo=timezone.utc))
        assert cron.matches(datetime(2024, 1, 1, 0, 30, tzinfo=timezone.utc))
        assert cron.matches(datetime(2024, 1, 1, 0, 45, tzinfo=timezone.utc))
        assert not cron.matches(datetime(2024, 1, 1, 0, 10, tzinfo=timezone.utc))

    def test_list(self):
        """Test list matching."""
        cron = CronExpression("0 0,12,18 * * *")
        assert cron.matches(datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc))
        assert cron.matches(datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc))
        assert cron.matches(datetime(2024, 1, 1, 18, 0, tzinfo=timezone.utc))
        assert not cron.matches(datetime(2024, 1, 1, 6, 0, tzinfo=timezone.utc))

    def test_range_with_step(self):
        """Test range with step."""
        cron = CronExpression("1-10/2 * * * *")
        assert 1 in cron.minutes
        assert 3 in cron.minutes
        assert 5 in cron.minutes
        assert 7 in cron.minutes
        assert 9 in cron.minutes
        assert 2 not in cron.minutes

    def test_invalid_field_count(self):
        """Test that wrong number of fields raises."""
        with pytest.raises(CronParseError, match="must have 5 fields"):
            CronExpression("* * *")

    def test_invalid_step(self):
        """Test that invalid step raises."""
        with pytest.raises(CronParseError, match="Invalid step"):
            CronExpression("*/abc * * * *")

    def test_negative_step(self):
        """Test that zero/negative step raises."""
        with pytest.raises(CronParseError, match="positive"):
            CronExpression("*/0 * * * *")

    def test_out_of_bounds(self):
        """Test that out-of-bounds values raise."""
        with pytest.raises(CronParseError, match="out of bounds"):
            CronExpression("60 * * * *")

    def test_day_of_week(self):
        """Test day of week matching (0=Sunday)."""
        # Monday = weekday() returns 0; Sunday = weekday() returns 6
        cron = CronExpression("0 0 * * 0")  # Sunday
        # 2024-01-07 is a Sunday
        assert cron.matches(datetime(2024, 1, 7, 0, 0, tzinfo=timezone.utc))
        # 2024-01-08 is a Monday
        assert not cron.matches(datetime(2024, 1, 8, 0, 0, tzinfo=timezone.utc))

    def test_next_run(self):
        """Test next_run calculation."""
        cron = CronExpression("0 2 * * *")  # Daily at 02:00
        ref = datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc)
        nxt = cron.next_run(ref)
        assert nxt.hour == 2
        assert nxt.minute == 0
        assert nxt.day == 16  # Next day

    def test_next_run_same_day(self):
        """Test next_run when the next run is later the same day."""
        cron = CronExpression("0 15 * * *")  # Daily at 15:00
        ref = datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc)
        nxt = cron.next_run(ref)
        assert nxt.hour == 15
        assert nxt.minute == 0
        assert nxt.day == 15  # Same day


# ---------------------------------------------------------------------------
# Schedule tests
# ---------------------------------------------------------------------------

class TestSchedule:
    """Tests for the Schedule class."""

    def test_from_string(self):
        """Test creating a schedule from a string."""
        schedule = Schedule.from_string("nightly", "0 2 * * *")
        assert schedule.name == "nightly"
        assert isinstance(schedule.cron, CronExpression)

    def test_should_run_matching(self):
        """Test should_run when cron matches."""
        schedule = Schedule.from_string("test", "0 2 * * *")
        dt = datetime(2024, 1, 15, 2, 0, tzinfo=timezone.utc)
        assert schedule.should_run(now=dt) is True

    def test_should_run_not_matching(self):
        """Test should_run when cron does not match."""
        schedule = Schedule.from_string("test", "0 2 * * *")
        dt = datetime(2024, 1, 15, 10, 0, tzinfo=timezone.utc)
        assert schedule.should_run(now=dt) is False

    def test_should_run_disabled(self):
        """Test that disabled schedule never runs."""
        schedule = Schedule.from_string("test", "* * * * *", enabled=False)
        dt = datetime(2024, 1, 15, 2, 0, tzinfo=timezone.utc)
        assert schedule.should_run(now=dt) is False

    def test_should_run_prevents_duplicate(self):
        """Test that should_run prevents duplicate runs in the same minute."""
        schedule = Schedule.from_string("test", "0 2 * * *")
        dt = datetime(2024, 1, 15, 2, 0, tzinfo=timezone.utc)
        assert schedule.should_run(now=dt, last_run=dt) is False

    def test_should_run_allows_after_gap(self):
        """Test that should_run allows after a gap."""
        schedule = Schedule.from_string("test", "0 2 * * *")
        now = datetime(2024, 1, 15, 2, 0, tzinfo=timezone.utc)
        last = datetime(2024, 1, 14, 2, 0, tzinfo=timezone.utc)
        assert schedule.should_run(now=now, last_run=last) is True

    def test_next_run_time(self):
        """Test next_run_time delegation."""
        schedule = Schedule.from_string("test", "0 2 * * *")
        ref = datetime(2024, 1, 15, 10, 0, tzinfo=timezone.utc)
        nxt = schedule.next_run_time(ref)
        assert nxt.day == 16
        assert nxt.hour == 2
