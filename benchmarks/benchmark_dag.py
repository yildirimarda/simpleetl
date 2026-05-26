"""Benchmark DAG execution performance."""

import time
import statistics
from typing import Dict, List

from simpleetl.core.dag import DAG, JobNode


def make_dummy_node(name: str, deps: List[str] | None = None) -> JobNode:
    """Create a lightweight JobNode with no actual job class for benchmarking."""
    return JobNode(name=name, dependencies=deps or [])


def build_linear_dag(n: int) -> DAG:
    """Build a linear chain DAG: node_0 -> node_1 -> ... -> node_n."""
    dag = DAG(name=f"linear_{n}")
    for i in range(n):
        deps = [f"node_{i-1}"] if i > 0 else []
        dag.add_node(make_dummy_node(f"node_{i}", deps))
    for i in range(1, n):
        dag.add_edge(f"node_{i-1}", f"node_{i}")
    return dag


def build_parallel_dag(n: int) -> DAG:
    """Build a DAG where all nodes are independent (max parallelism)."""
    dag = DAG(name=f"parallel_{n}")
    for i in range(n):
        dag.add_node(make_dummy_node(f"node_{i}"))
    return dag


def build_binary_tree_dag(depth: int) -> DAG:
    """Build a binary-tree shaped DAG of given depth."""
    dag = DAG(name=f"tree_{depth}")
    n_nodes = (1 << depth) - 1  # 2^depth - 1
    for i in range(n_nodes):
        deps = []
        if i > 0:
            parent = (i - 1) // 2
            deps.append(f"node_{parent}")
        dag.add_node(make_dummy_node(f"node_{i}", deps))
    for i in range(1, n_nodes):
        parent = (i - 1) // 2
        dag.add_edge(f"node_{parent}", f"node_{i}")
    return dag


def benchmark_topo_sort(dag: DAG, n_runs: int = 100) -> Dict[str, float]:
    """Benchmark topological_sort."""
    times = []
    for _ in range(n_runs):
        start = time.perf_counter()
        dag.topological_sort()
        times.append(time.perf_counter() - start)
    return {
        "mean_us": statistics.mean(times) * 1e6,
        "median_us": statistics.median(times) * 1e6,
        "min_us": min(times) * 1e6,
        "max_us": max(times) * 1e6,
    }


def benchmark_parallel_groups(dag: DAG, n_runs: int = 100) -> Dict[str, float]:
    """Benchmark get_parallel_groups."""
    times = []
    for _ in range(n_runs):
        start = time.perf_counter()
        dag.get_parallel_groups()
        times.append(time.perf_counter() - start)
    return {
        "mean_us": statistics.mean(times) * 1e6,
        "median_us": statistics.median(times) * 1e6,
        "min_us": min(times) * 1e6,
        "max_us": max(times) * 1e6,
    }


def benchmark_validate(dag: DAG, n_runs: int = 100) -> Dict[str, float]:
    """Benchmark validate."""
    times = []
    for _ in range(n_runs):
        start = time.perf_counter()
        dag.validate()
        times.append(time.perf_counter() - start)
    return {
        "mean_us": statistics.mean(times) * 1e6,
        "median_us": statistics.median(times) * 1e6,
        "min_us": min(times) * 1e6,
        "max_us": max(times) * 1e6,
    }


def benchmark_from_dict(n_nodes: int, n_runs: int = 50) -> Dict[str, float]:
    """Benchmark DAG.from_dict construction."""
    import time as _time
    import statistics as _stats
    jobs = []
    for i in range(n_nodes):
        deps = [f"node_{i-1}"] if i > 0 else []
        jobs.append({"name": f"node_{i}", "dependencies": deps})
    data = {"name": f"bench_{n_nodes}", "jobs": jobs}

    times = []
    for _ in range(n_runs):
        start = _time.perf_counter()
        DAG.from_dict(data)
        times.append(_time.perf_counter() - start)
    return {
        "mean_us": _stats.mean(times) * 1e6,
        "median_us": _stats.median(times) * 1e6,
        "min_us": min(times) * 1e6,
        "max_us": max(times) * 1e6,
    }


if __name__ == "__main__":
    print(f"\n{'='*60}")
    print("DAG Topological Sort Benchmark")
    print(f"{'='*60}")
    for n in [3, 5, 10, 50, 100]:
        dag = build_linear_dag(n)
        result = benchmark_topo_sort(dag)
        print(
            f"  linear {n:4d} nodes  "
            f"mean={result['mean_us']:.1f}us  "
            f"median={result['median_us']:.1f}us"
        )

    print(f"\n{'='*60}")
    print("DAG Parallel Groups Benchmark")
    print(f"{'='*60}")
    for n in [3, 5, 10, 50, 100]:
        dag = build_linear_dag(n)
        result = benchmark_parallel_groups(dag)
        print(
            f"  linear {n:4d} nodes  "
            f"mean={result['mean_us']:.1f}us  "
            f"median={result['median_us']:.1f}us"
        )

    print(f"\n{'='*60}")
    print("DAG Validate Benchmark")
    print(f"{'='*60}")
    for n in [3, 5, 10, 50, 100]:
        dag = build_linear_dag(n)
        result = benchmark_validate(dag)
        print(
            f"  linear {n:4d} nodes  "
            f"mean={result['mean_us']:.1f}us  "
            f"median={result['median_us']:.1f}us"
        )

    print(f"\n{'='*60}")
    print("DAG from_dict Construction Benchmark")
    print(f"{'='*60}")
    for n in [3, 5, 10, 50, 100]:
        result = benchmark_from_dict(n)
        print(
            f"  {n:4d} nodes  "
            f"mean={result['mean_us']:.1f}us  "
            f"median={result['median_us']:.1f}us"
        )

    print(f"\n{'='*60}")
    print("DAG Shape Comparison (10 nodes)")
    print(f"{'='*60}")
    for label, dag in [
        ("linear (10)", build_linear_dag(10)),
        ("parallel (10)", build_parallel_dag(10)),
        ("tree depth=3 (7)", build_binary_tree_dag(3)),
        ("tree depth=4 (15)", build_binary_tree_dag(4)),
    ]:
        topo = dag.topological_sort()
        groups = dag.get_parallel_groups()
        n_groups = len(groups)
        max_group = max(len(g) for g in groups)
        print(
            f"  {label:25s}  topo_order={len(topo)}  "
            f"groups={n_groups}  max_parallel={max_group}"
        )
