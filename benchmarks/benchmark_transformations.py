"""Benchmark transformation function performance."""

import time
import statistics
from typing import Callable, Dict, Any, List, Tuple

import pandas as pd
import numpy as np

from simpleetl.transformations import (
    filter_data,
    map_values,
    aggregate_data,
    join_data,
    union_data,
    transform_chain,
    window_functions,
    string_operations,
    date_operations,
)


def generate_test_data(n_rows: int) -> pd.DataFrame:
    """Generate a test DataFrame with mixed types."""
    np.random.seed(42)
    return pd.DataFrame({
        "id": range(n_rows),
        "name": [f"user_{i}" for i in range(n_rows)],
        "age": np.random.randint(18, 80, n_rows),
        "score": np.random.uniform(0, 100, n_rows).round(2),
        "active": np.random.choice([True, False], n_rows),
        "city": np.random.choice(
            ["NYC", "LA", "Chicago", "Houston", "Phoenix"], n_rows
        ),
        "signup_date": pd.date_range("2020-01-01", periods=n_rows, freq="h"),
    })


def time_call(func: Callable, *args: Any, **kwargs: Any) -> float:
    """Time a single function call, return wall-clock seconds."""
    start = time.perf_counter()
    func(*args, **kwargs)
    return time.perf_counter() - start


def benchmark_fn(
    name: str,
    func: Callable,
    *args: Any,
    n_runs: int = 5,
    **kwargs: Any,
) -> Dict[str, float]:
    """Run a function n_runs times and return timing stats."""
    times = [time_call(func, *args, **kwargs) for _ in range(n_runs)]
    return {
        "name": name,
        "mean_s": statistics.mean(times),
        "median_s": statistics.median(times),
        "min_s": min(times),
        "max_s": max(times),
    }


def run_filter_benchmarks(df: pd.DataFrame) -> List[Dict[str, float]]:
    """Benchmark filter_data at various selectivities."""
    return [
        benchmark_fn(
            "filter (column, min/max)",
            filter_data,
            df,
            column="age",
            min_value=30,
            max_value=60,
        ),
    ]


def run_map_benchmarks(df: pd.DataFrame) -> List[Dict[str, float]]:
    """Benchmark map_values with dict and callable mappings."""
    city_map = {"NYC": "New York", "LA": "Los Angeles", "Chicago": "Chicago",
                "Houston": "Houston", "Phoenix": "Phoenix"}
    return [
        benchmark_fn("map (dict)", map_values, df, "city", city_map),
        benchmark_fn(
            "map (callable)",
            map_values,
            df,
            "city",
            lambda x: x.lower(),
        ),
    ]


def run_aggregate_benchmarks(df: pd.DataFrame) -> List[Dict[str, float]]:
    """Benchmark aggregate_data with various aggregation specs."""
    return [
        benchmark_fn(
            "aggregate (single)",
            aggregate_data,
            df,
            groupby="city",
            agg={"score": "mean"},
        ),
        benchmark_fn(
            "aggregate (multi)",
            aggregate_data,
            df,
            groupby="city",
            agg={"age": ["min", "max", "mean"], "score": ["sum", "count"]},
        ),
    ]


def run_join_benchmarks(df: pd.DataFrame) -> List[Dict[str, float]]:
    """Benchmark join_data with inner join."""
    lookup = pd.DataFrame({
        "city": ["NYC", "LA", "Chicago", "Houston", "Phoenix"],
        "state": ["NY", "CA", "IL", "TX", "AZ"],
        "population": [8_336_817, 3_979_576, 2_693_976,
                       2_320_268, 1_680_992],
    })
    return [
        benchmark_fn("join (inner)", join_data, df, lookup, on="city"),
    ]


def run_union_benchmarks(df: pd.DataFrame) -> List[Dict[str, float]]:
    """Benchmark union_data."""
    return [
        benchmark_fn("union", union_data, df, df),
    ]


def run_window_benchmarks(df: pd.DataFrame) -> List[Dict[str, float]]:
    """Benchmark window_functions."""
    sample = df.head(min(len(df), 100_000)).copy()
    return [
        benchmark_fn(
            "window (rank)",
            window_functions,
            sample,
            partition_by="city",
            order_by="score",
            functions={
                "score_rank": {"function": "rank"},
            },
        ),
        benchmark_fn(
            "window (lag+cumsum)",
            window_functions,
            sample,
            partition_by="city",
            order_by="score",
            functions={
                "prev_score": {
                    "function": "lag",
                    "column": "score",
                    "offset": 1,
                },
                "cum_score": {"function": "cumsum", "column": "score"},
            },
        ),
    ]


def run_string_benchmarks(df: pd.DataFrame) -> List[Dict[str, float]]:
    """Benchmark string_operations."""
    sample = df.head(min(len(df), 100_000)).copy()
    return [
        benchmark_fn(
            "string (lower)",
            string_operations,
            sample,
            column="city",
            operation="lower",
        ),
        benchmark_fn(
            "string (replace)",
            string_operations,
            sample,
            column="name",
            operation="replace",
            old="user_",
            new="u_",
        ),
        benchmark_fn(
            "string (contains)",
            string_operations,
            sample,
            column="city",
            operation="contains",
            pattern="New",
        ),
        benchmark_fn(
            "string (length)",
            string_operations,
            sample,
            column="name",
            operation="length",
        ),
    ]


def run_date_benchmarks(df: pd.DataFrame) -> List[Dict[str, float]]:
    """Benchmark date_operations."""
    sample = df.head(min(len(df), 100_000)).copy()
    return [
        benchmark_fn(
            "date (extract year)",
            date_operations,
            sample,
            column="signup_date",
            operation="extract",
            part="year",
        ),
        benchmark_fn(
            "date (trunc month)",
            date_operations,
            sample,
            column="signup_date",
            operation="trunc",
            freq="month",
        ),
        benchmark_fn(
            "date (format)",
            date_operations,
            sample,
            column="signup_date",
            operation="format",
            fmt="%Y-%m-%d",
        ),
        benchmark_fn(
            "date (is_weekend)",
            date_operations,
            sample,
            column="signup_date",
            operation="is_weekend",
        ),
    ]


def run_chain_benchmarks(df: pd.DataFrame) -> List[Dict[str, float]]:
    """Benchmark transform_chain with varying step counts."""
    city_map = {"NYC": "NY", "LA": "CA", "Chicago": "IL",
                "Houston": "TX", "Phoenix": "AZ"}

    steps_4: List[Tuple[Callable, Dict[str, Any]]] = [
        (filter_data, {"column": "age", "min_value": 25}),
        (filter_data, {"column": "age", "max_value": 65}),
        (map_values, {"column": "city", "mapping": city_map}),
        (filter_data, {"column": "score", "min_value": 20}),
    ]

    steps_9 = steps_4 + [
        (filter_data, {"column": "score", "max_value": 80}),
        (map_values, {"column": "city", "mapping": city_map}),
        (filter_data, {"column": "active", "min_value": 1}),
        (map_values, {"column": "city", "mapping": city_map}),
        (filter_data, {"column": "age", "min_value": 30}),
    ]

    results = []
    for label, steps in [("chain (4 steps)", steps_4),
                         ("chain (9 steps)", steps_9)]:
        results.append(benchmark_fn(label, transform_chain, df, steps))
    return results


if __name__ == "__main__":
    categories = [
        ("filter", run_filter_benchmarks),
        ("map", run_map_benchmarks),
        ("aggregate", run_aggregate_benchmarks),
        ("join", run_join_benchmarks),
        ("union", run_union_benchmarks),
        ("window", run_window_benchmarks),
        ("string", run_string_benchmarks),
        ("date", run_date_benchmarks),
        ("chain", run_chain_benchmarks),
    ]

    for size in [10_000, 100_000, 1_000_000]:
        df = generate_test_data(size)
        print(f"\n{'='*60}")
        print(f"Dataset: {size:,} rows, {len(df.columns)} columns")
        print(f"{'='*60}")
        for _label, runner in categories:
            for result in runner(df):
                print(
                    f"  {result['name']:30s}  "
                    f"mean={result['mean_s']:.4f}s  "
                    f"median={result['median_s']:.4f}s"
                )
