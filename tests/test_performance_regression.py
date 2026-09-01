"""
Performance regression tests.

Verify benchmarked operations stay within acceptable thresholds
documented in docs/performance.md. These catch performance
regressions early.
"""

import pytest

from benchmarks.benchmark_read_write import (
    generate_test_data,
    benchmark_format,
)
from benchmarks.benchmark_transformations import (
    generate_test_data as generate_transform_data,
    run_filter_benchmarks,
    run_map_benchmarks,
    run_aggregate_benchmarks,
    run_join_benchmarks,
    run_union_benchmarks,
    run_string_benchmarks,
    run_date_benchmarks,
    run_chain_benchmarks,
)
from benchmarks.benchmark_dag import (
    build_linear_dag,
    benchmark_topo_sort,
    benchmark_parallel_groups,
    benchmark_validate,
    benchmark_from_dict,
)


# Thresholds derived from docs/performance.md with generous CI margin
READ_WRITE_THRESHOLDS = {
    1_000: 0.050,  # baseline ~0.001s, generous CI margin
    10_000: 0.250,  # baseline ~0.009s, generous CI margin
    100_000: 2.500,  # baseline ~0.085s, generous CI margin
}


class TestReadWriteRegression:
    """Regression tests for read/write performance."""

    @pytest.mark.parametrize("fmt", ["csv", "json", "parquet"])
    def test_1000_rows_within_threshold(self, fmt):
        df = generate_test_data(1_000)
        result = benchmark_format(fmt, df, n_runs=3)
        assert result["mean_s"] < READ_WRITE_THRESHOLDS[1_000], (
            f"{fmt} 1K rows mean {result['mean_s']}s exceeds threshold"
        )

    @pytest.mark.parametrize("fmt", ["csv", "json", "parquet"])
    def test_10000_rows_within_threshold(self, fmt):
        df = generate_test_data(10_000)
        result = benchmark_format(fmt, df, n_runs=3)
        assert result["mean_s"] < READ_WRITE_THRESHOLDS[10_000], (
            f"{fmt} 10K rows mean {result['mean_s']}s exceeds threshold"
        )


class TestTransformationRegression:
    """Regression tests for transformation performance."""

    def test_filter_small_data_fast(self):
        df = generate_transform_data(10_000)
        results = run_filter_benchmarks(df)
        for r in results:
            assert r["mean_s"] < 0.010, (
                f"filter {r['name']} mean {r['mean_s']}s exceeds threshold"
            )

    def test_map_small_data_fast(self):
        df = generate_transform_data(10_000)
        results = run_map_benchmarks(df)
        for r in results:
            assert r["mean_s"] < 0.020, (
                f"map {r['name']} mean {r['mean_s']}s exceeds threshold"
            )

    def test_aggregate_small_data_fast(self):
        df = generate_transform_data(10_000)
        results = run_aggregate_benchmarks(df)
        for r in results:
            assert r["mean_s"] < 0.020, (
                f"aggregate {r['name']} mean {r['mean_s']}s exceeds threshold"
            )

    def test_join_small_data_fast(self):
        df = generate_transform_data(10_000)
        results = run_join_benchmarks(df)
        for r in results:
            assert r["mean_s"] < 0.020, (
                f"join {r['name']} mean {r['mean_s']}s exceeds threshold"
            )

    def test_union_small_data_fast(self):
        df = generate_transform_data(10_000)
        results = run_union_benchmarks(df)
        for r in results:
            assert r["mean_s"] < 0.005, (
                f"union {r['name']} mean {r['mean_s']}s exceeds threshold"
            )

    def test_string_small_data_fast(self):
        df = generate_transform_data(10_000)
        results = run_string_benchmarks(df)
        for r in results:
            assert r["mean_s"] < 0.020, (
                f"string {r['name']} mean {r['mean_s']}s exceeds threshold"
            )

    def test_date_small_data_fast(self):
        df = generate_transform_data(10_000)
        results = run_date_benchmarks(df)
        for r in results:
            assert r["mean_s"] < 0.020, (
                f"date {r['name']} mean {r['mean_s']}s exceeds threshold"
            )

    def test_chain_4_steps_small_data_fast(self):
        df = generate_transform_data(10_000)
        results = run_chain_benchmarks(df)
        for r in results:
            assert r["mean_s"] < 0.020, (
                f"chain {r['name']} mean {r['mean_s']}s exceeds threshold"
            )


class TestDAGRegression:
    """Regression tests for DAG operation performance."""

    def test_topo_sort_10_nodes_fast(self):
        dag = build_linear_dag(10)
        result = benchmark_topo_sort(dag, n_runs=50)
        assert result["mean_us"] < 60.0, (
            f"topo_sort mean {result['mean_us']}us exceeds 60us threshold"
        )

    def test_parallel_groups_10_nodes_fast(self):
        dag = build_linear_dag(10)
        result = benchmark_parallel_groups(dag, n_runs=50)
        assert result["mean_us"] < 80.0, (
            f"parallel_groups mean {result['mean_us']}us exceeds 80us threshold"
        )

    def test_validate_10_nodes_fast(self):
        dag = build_linear_dag(10)
        result = benchmark_validate(dag, n_runs=50)
        assert result["mean_us"] < 60.0, (
            f"validate mean {result['mean_us']}us exceeds 60us threshold"
        )

    def test_from_dict_10_nodes_fast(self):
        result = benchmark_from_dict(10, n_runs=25)
        assert result["mean_us"] < 100.0, (
            f"from_dict mean {result['mean_us']}us exceeds 100us threshold"
        )
