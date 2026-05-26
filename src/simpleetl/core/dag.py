"""
DAG-based job orchestration for production ETL pipelines.

Provides directed acyclic graph representation of ETL job dependencies,
topological sorting, parallel execution groups, and a runner that
executes jobs respecting dependency ordering.
"""

import importlib
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import yaml

from simpleetl.core.config import ETLJobConfig, load_config

logger = __import__("logging").getLogger(__name__)


class NodeStatus(str, Enum):
    """Status of a single job node in the DAG."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class JobNode:
    """Represents a single job in the DAG.

    Attributes:
        name: Unique identifier for this node.
        job_class: Dotted path to the ETLJob subclass
            (e.g. ``my_module.MyJob``).
        config_path: Path to the job configuration file.
        params: Extra parameters merged into the job config.
        dependencies: Names of upstream nodes that must complete before
            this node runs.
        status: Current execution status.
        upstream: Set of upstream node names (populated by DAG).
        downstream: Set of downstream node names (populated by DAG).
    """

    name: str
    job_class: str = ""
    config_path: str = ""
    params: Dict[str, Any] = field(default_factory=dict)
    dependencies: List[str] = field(default_factory=list)
    status: NodeStatus = NodeStatus.PENDING
    upstream: Set[str] = field(default_factory=set)
    downstream: Set[str] = field(default_factory=set)

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, JobNode):
            return NotImplemented
        return self.name == other.name


@dataclass
class NodeResult:
    """Result of executing a single JobNode.

    Attributes:
        node_name: Name of the node.
        status: Final status after execution.
        duration: Wall-clock execution time in seconds.
        error: Error message if the node failed, else None.
    """

    node_name: str
    status: NodeStatus
    duration: float = 0.0
    error: Optional[str] = None


@dataclass
class DAGResult:
    """Overall result of a DAG execution.

    Attributes:
        status: ``success`` if all nodes succeeded, ``failed`` otherwise.
        node_results: Per-node results keyed by node name.
        duration: Total wall-clock time for the entire DAG run.
    """

    status: str
    node_results: Dict[str, NodeResult] = field(default_factory=dict)
    duration: float = 0.0

    @property
    def failed_nodes(self) -> List[str]:
        """Return names of nodes that failed."""
        return [
            name
            for name, result in self.node_results.items()
            if result.status == NodeStatus.FAILED
        ]

    @property
    def skipped_nodes(self) -> List[str]:
        """Return names of nodes that were skipped."""
        return [
            name
            for name, result in self.node_results.items()
            if result.status == NodeStatus.SKIPPED
        ]


class DAGCycleError(Exception):
    """Raised when a cycle is detected in the DAG."""
    pass


class DAGMissingDependencyError(Exception):
    """Raised when a node depends on a non-existent node."""
    pass


class DAG:
    """Directed Acyclic Graph of ETL jobs.

    Supports building the graph, validation (cycle and dependency checks),
    topological sorting, and identification of parallel execution groups.

    Example::

        dag = DAG("my_pipeline")
        dag.add_node(JobNode(name="extract", job_class="my.ExtractJob", config_path="extract.yaml"))
        dag.add_node(JobNode(name="transform", job_class="my.TransformJob", config_path="transform.yaml", dependencies=["extract"]))
        dag.add_edge("extract", "transform")
        dag.validate()
        order = dag.topological_sort()
    """

    def __init__(self, name: str = "dag") -> None:
        self.name = name
        self._nodes: Dict[str, JobNode] = {}

    @property
    def nodes(self) -> Dict[str, JobNode]:
        """Return a copy of the node dictionary."""
        return dict(self._nodes)

    def add_node(self, node: JobNode) -> None:
        """Add a node to the DAG.

        Args:
            node: The ``JobNode`` to add.

        Raises:
            ValueError: If a node with the same name already exists.
        """
        if node.name in self._nodes:
            raise ValueError(f"Node '{node.name}' already exists in DAG")
        self._nodes[node.name] = node

    def add_edge(self, from_node: str, to_node: str) -> None:
        """Add a directed edge ``from_node -> to_node``.

        This establishes that ``to_node`` depends on ``from_node``.

        Args:
            from_node: Name of the upstream node.
            to_node: Name of the downstream node.

        Raises:
            DAGMissingDependencyError: If either node does not exist.
        """
        if from_node not in self._nodes:
            raise DAGMissingDependencyError(
                f"Node '{from_node}' not found in DAG"
            )
        if to_node not in self._nodes:
            raise DAGMissingDependencyError(
                f"Node '{to_node}' not found in DAG"
            )
        self._nodes[from_node].downstream.add(to_node)
        self._nodes[to_node].upstream.add(from_node)

    def get_node(self, name: str) -> JobNode:
        """Retrieve a node by name.

        Args:
            name: The node name.

        Returns:
            The ``JobNode`` instance.

        Raises:
            KeyError: If the node does not exist.
        """
        if name not in self._nodes:
            raise KeyError(f"Node '{name}' not found in DAG")
        return self._nodes[name]

    def validate(self) -> None:
        """Validate the DAG.

        Checks:
        1. All dependency references point to existing nodes.
        2. The graph contains no cycles (via DFS).

        Raises:
            DAGMissingDependencyError: If a dependency references a
                non-existent node.
            DAGCycleError: If a cycle is detected.
        """
        # Check all dependencies exist
        for node in self._nodes.values():
            for dep in node.dependencies:
                if dep not in self._nodes:
                    raise DAGMissingDependencyError(
                        f"Node '{node.name}' depends on '{dep}' "
                        f"which does not exist in the DAG"
                    )
            # Sync upstream set with explicit dependencies
            for dep in node.dependencies:
                node.upstream.add(dep)
                self._nodes[dep].downstream.add(node.name)

        # Cycle detection via DFS with coloring
        WHITE, GRAY, BLACK = 0, 1, 2
        color: Dict[str, int] = {name: WHITE for name in self._nodes}

        def _dfs(node_name: str) -> None:
            color[node_name] = GRAY
            for downstream_name in self._nodes[node_name].downstream:
                if color[downstream_name] == GRAY:
                    raise DAGCycleError(
                        f"Cycle detected: '{node_name}' -> "
                        f"'{downstream_name}'"
                    )
                if color[downstream_name] == WHITE:
                    _dfs(downstream_name)
            color[node_name] = BLACK

        for name in self._nodes:
            if color[name] == WHITE:
                _dfs(name)

    def topological_sort(self) -> List[str]:
        """Return node names in topological (dependency-respecting) order.

        Uses Kahn's algorithm.

        Returns:
            Ordered list of node names.

        Raises:
            DAGCycleError: If the graph contains a cycle.
        """
        in_degree: Dict[str, int] = {
            name: len(node.upstream) for name, node in self._nodes.items()
        }
        queue: List[str] = [
            name for name, deg in in_degree.items() if deg == 0
        ]
        order: List[str] = []

        while queue:
            # Sort for deterministic output
            queue.sort()
            current = queue.pop(0)
            order.append(current)
            for downstream_name in sorted(
                self._nodes[current].downstream
            ):
                in_degree[downstream_name] -= 1
                if in_degree[downstream_name] == 0:
                    queue.append(downstream_name)

        if len(order) != len(self._nodes):
            raise DAGCycleError(
                "Cycle detected during topological sort"
            )

        return order

    def get_parallel_groups(self) -> List[List[str]]:
        """Return groups of nodes that can execute in parallel.

        Each group contains nodes whose dependencies are all satisfied
        by earlier groups.

        Returns:
            List of groups, where each group is a list of node names.
        """
        in_degree: Dict[str, int] = {
            name: len(node.upstream) for name, node in self._nodes.items()
        }
        remaining = dict(in_degree)
        groups: List[List[str]] = []

        while remaining:
            group = sorted(
                [name for name, deg in remaining.items() if deg == 0]
            )
            if not group:
                raise DAGCycleError(
                    "Cycle detected while computing parallel groups"
                )
            groups.append(group)
            for name in group:
                del remaining[name]
                for downstream_name in self._nodes[name].downstream:
                    if downstream_name in remaining:
                        remaining[downstream_name] -= 1

        return groups

    def get_execution_plan(self) -> Dict[str, Any]:
        """Return a human-readable execution plan.

        Returns:
            Dictionary with ``name``, ``topological_order``,
            ``parallel_groups``, and ``nodes`` summary.
        """
        return {
            "name": self.name,
            "topological_order": self.topological_sort(),
            "parallel_groups": self.get_parallel_groups(),
            "nodes": {
                name: {
                    "job_class": node.job_class,
                    "config_path": node.config_path,
                    "dependencies": list(node.dependencies),
                }
                for name, node in self._nodes.items()
            },
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DAG":
        """Build a DAG from a dictionary (typically loaded from YAML).

        Expected format::

            name: my_dag
            jobs:
              - name: extract
                job_class: my.ExtractJob
                config_path: extract.yaml
              - name: transform
                job_class: my.TransformJob
                config_path: transform.yaml
                dependencies: [extract]

        Args:
            data: Parsed YAML/JSON dictionary.

        Returns:
            A validated ``DAG`` instance.
        """
        dag = cls(name=data.get("name", "dag"))
        jobs = data.get("jobs", [])

        for job_data in jobs:
            node = JobNode(
                name=job_data["name"],
                job_class=job_data.get("job_class", ""),
                config_path=job_data.get("config_path", ""),
                params=job_data.get("params", {}),
                dependencies=job_data.get("dependencies", []),
            )
            dag.add_node(node)

        # Build edges from dependencies
        for job_data in jobs:
            for dep in job_data.get("dependencies", []):
                dag.add_edge(dep, job_data["name"])

        return dag

    @classmethod
    def from_yaml(cls, path: str) -> "DAG":
        """Load a DAG from a YAML file.

        Args:
            path: Path to the YAML file.

        Returns:
            A validated ``DAG`` instance.
        """
        with open(path, "r") as f:
            data = yaml.safe_load(f)
        return cls.from_dict(data)


class DAGRunner:
    """Execute a DAG of ETL jobs.

    Supports sequential and parallel execution, fail-fast and continue-on-error
    modes, and collects per-node results.

    Example::

        runner = DAGRunner(max_parallel=4, fail_fast=True)
        result = runner.run(dag)
        print(result.status)
    """

    def __init__(
        self,
        max_parallel: int = 1,
        fail_fast: bool = True,
    ) -> None:
        """Initialize the runner.

        Args:
            max_parallel: Maximum number of concurrent jobs within a
                parallel group. ``1`` means sequential execution.
            fail_fast: If ``True``, stop the entire DAG on the first
                node failure. If ``ERROR``, continue independent
                branches but skip downstream of failed nodes.
        """
        self.max_parallel = max_parallel
        self.fail_fast = fail_fast

    def run(self, dag: DAG) -> DAGResult:
        """Execute all jobs in the DAG respecting dependencies.

        Args:
            dag: The ``DAG`` to execute. Will be validated first.

        Returns:
            ``DAGResult`` with per-node status, durations, and errors.
        """
        dag.validate()
        groups = dag.get_parallel_groups()
        node_results: Dict[str, NodeResult] = {}
        failed_set: Set[str] = set()
        overall_start = time.time()

        for group in groups:
            # Determine which nodes in this group should run, skip, or are
            # already failed
            to_run: List[str] = []
            for name in group:
                node = dag.get_node(name)
                # Check if any upstream dependency failed or was skipped
                upstream_failed = any(
                    dep in failed_set for dep in node.upstream
                )
                if upstream_failed:
                    node.status = NodeStatus.SKIPPED
                    node_results[name] = NodeResult(
                        node_name=name,
                        status=NodeStatus.SKIPPED,
                    )
                    failed_set.add(name)
                    logger.info(
                        "Skipping node '%s' due to upstream failure",
                        name,
                    )
                else:
                    to_run.append(name)

            if not to_run:
                continue

            group_results = self._run_group(dag, to_run)
            node_results.update(group_results)

            # Check for failures in this group
            group_failures = [
                name
                for name in to_run
                if group_results[name].status == NodeStatus.FAILED
            ]

            if group_failures:
                failed_set.update(group_failures)
                if self.fail_fast:
                    # Mark all remaining nodes as skipped
                    self._skip_remaining(
                        dag, groups, group, node_results, failed_set
                    )
                    break

        overall_duration = time.time() - overall_start
        overall_status = (
            "failed" if failed_set else "success"
        )

        return DAGResult(
            status=overall_status,
            node_results=node_results,
            duration=overall_duration,
        )

    def _run_group(
        self, dag: DAG, node_names: List[str]
    ) -> Dict[str, NodeResult]:
        """Execute a group of independent nodes.

        Uses ``ThreadPoolExecutor`` when ``max_parallel > 1``.
        """
        if self.max_parallel <= 1:
            return {
                name: self._execute_node(dag.get_node(name))
                for name in node_names
            }

        results: Dict[str, NodeResult] = {}
        with ThreadPoolExecutor(max_workers=self.max_parallel) as executor:
            future_to_name = {
                executor.submit(
                    self._execute_node, dag.get_node(name)
                ): name
                for name in node_names
            }
            for future in as_completed(future_to_name):
                name = future_to_name[future]
                results[name] = future.result()
        return results

    def _execute_node(self, node: JobNode) -> NodeResult:
        """Execute a single job node.

        Dynamically imports the job class, loads config, applies overrides,
        and runs the job.
        """
        node.status = NodeStatus.RUNNING
        start = time.time()

        try:
            config: dict[str, Any] | ETLJobConfig = {}
            if node.config_path:
                config = load_config(node.config_path)

            # Merge extra params into config
            if node.params and isinstance(config, dict):
                config.setdefault("params", {}).update(node.params)
            elif node.params and isinstance(config, ETLJobConfig):
                merged = dict(config.params)
                merged.update(node.params)
                config.params = merged

            if node.job_class:
                module_path, class_name = node.job_class.rsplit(".", 1)
                # Ensure the job class's parent directory is importable.
                # Walk up from the config file's directory (and cwd) looking
                # for the module so that examples/ and similar directories
                # are discovered even when they are not installed packages.
                if module_path not in sys.modules:
                    _add_import_paths(node.config_path)
                module = importlib.import_module(module_path)
                job_cls = getattr(module, class_name)
                job = job_cls(config)
                job.run_with_error_handling()
            elif node.config_path:
                # Use the standard run path via job_class from config
                from simpleetl.cli import run_job

                run_job(node.config_path)
            else:
                logger.warning(
                    "Node '%s' has no job_class or config_path; skipping execution",
                    node.name,
                )

            node.status = NodeStatus.SUCCESS
            duration = time.time() - start
            logger.info("Node '%s' completed successfully in %.2fs", node.name, duration)
            return NodeResult(
                node_name=node.name,
                status=NodeStatus.SUCCESS,
                duration=duration,
            )

        except Exception as e:
            node.status = NodeStatus.FAILED
            duration = time.time() - start
            logger.error(
                "Node '%s' failed after %.2fs: %s",
                node.name,
                duration,
                str(e),
            )
            return NodeResult(
                node_name=node.name,
                status=NodeStatus.FAILED,
                duration=duration,
                error=str(e),
            )

    @staticmethod
    def _skip_remaining(
        dag: DAG,
        groups: List[List[str]],
        current_group: List[str],
        node_results: Dict[str, NodeResult],
        failed_set: Set[str],
    ) -> None:
        """Mark all nodes after the current group as skipped (fail-fast)."""
        current_seen = False
        for group in groups:
            if group == current_group:
                current_seen = True
                continue
            if current_seen:
                for name in group:
                    if name not in node_results:
                        node = dag.get_node(name)
                        node.status = NodeStatus.SKIPPED
                        node_results[name] = NodeResult(
                            node_name=name,
                            status=NodeStatus.SKIPPED,
                        )
                        failed_set.add(name)


def _add_import_paths(config_path: str) -> None:
    """Add directories derived from *config_path* to ``sys.path``.

    Starting from the directory containing the config file, walk up
    four levels and add each directory that is not already on
    ``sys.path``.  This makes it possible to resolve modules that
    live next to the DAG config (e.g. an ``examples/`` directory).
    """
    base = Path(config_path).resolve().parent
    for _ in range(4):
        str_path = str(base)
        if str_path not in sys.path:
            sys.path.insert(0, str_path)
        parent = base.parent
        if parent == base:
            break  # reached filesystem root
        base = parent
