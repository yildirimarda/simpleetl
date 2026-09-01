"""
Performance regression tests — redesigned to be CI-safe.

Strategy:
- Primary: relative ratio against a minimal baseline measured in the
  same session. Generous factors (50–500×) only catch
  order-of-magnitude regressions, not CI noise.
- Optional absolute thresholds via RUN_PERF=1 for local use.
"""

import os

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

# When RUN_PERF=1 is set, also enforce absolute thresholds (local use only).
RUN_PERF = os.environ.get("RUN_PERF") == "1"

# Absolute thresholds derived from docs/performance.md (only checked with RUN_PERF=1)
READ_WRITE_THRESHOLDS = {
    1_000: 0.050,
    10_000: 0.250,
    100_000: 2.500,
}

# Generous relative-ratio thresholds. These compare the target operation
# against a minimal baseline in the same session, catching only massive
# (order-of-magnitude) regressions.
READ_WRITE_RATIO = 200  # 1K rows vs 10-row baseline
TRANSFORM_RATIO = 500  # 10K rows vs 100-row baseline
DAG_RATIO = 100  # 10-node DAG vs 3-node baseline


class TestReadWriteRegression:
    """Regression tests for read/write — ratio-based, CI-safe."""

    @pytest.mark.parametrize("fmt", ["csv", "json", "parquet"])
    def test_1000_rows_ratio_against_baseline(self, fmt):
        # Baseline measured in same session (tiny dataset)
        baseline_df = generate_test_data(10)
        baseline_result = benchmark_format(fmt, baseline_df, n_runs=3)
        baseline_mean = baseline_result["mean_s"]

        target_df = generate_test_data(1_000)
        target_result = benchmark_format(fmt, target_df, n_runs=3)
        target_mean = target_result["mean_s"]

        ratio = target_mean / max(baseline_mean, 1e-9)
        assert ratio < READ_WRITE_RATIO, (
            f"{fmt} 1K/10-row ratio {ratio:.1f}x exceeds generous "
            f"threshold {READ_WRITE_RATIO}x (baseline={baseline_mean:.4f}s, "
            f"target={target_mean:.4f}s)"
        )

        if RUN_PERF:
            assert target_mean < READ_WRITE_THRESHOLDS[1_000], (
                f"{fmt} 1K rows absolute mean {target_mean}s exceeds "
                f"threshold {READ_WRITE_THRESHOLDS[1_000]}s (RUN_PERF=1)"
            )

    @pytest.mark.parametrize("fmt", ["csv", "json", "parquet"])
    def test_10000_rows_ratio_against_baseline(self, fmt):
        baseline_df = generate_test_data(100)
        baseline_result = benchmark_format(fmt, baseline_df, n_runs=3)
        baseline_mean = baseline_result["mean_s"]

        target_df = generate_test_data(10_000)
        target_result = benchmark_format(fmt, target_df, n_runs=3)
        target_mean = target_result["mean_s"]

        ratio = target_mean / max(baseline_mean, 1e-9)
        assert ratio < READ_WRITE_RATIO * 3, (
            f"{fmt} 10K/100-row ratio {ratio:.1f}x exceeds generous "
            f"threshold {READ_WRITE_RATIO * 3}x (baseline={baseline_mean:.4f}s, "
            f"target={target_mean:.4f}s)"
        )

        if RUN_PERF:
            assert target_mean < READ_WRITE_THRESHOLDS[10_000], (
                f"{fmt} 10K rows absolute mean {target_mean}s exceeds "
                f"threshold {READ_WRITE_THRESHOLDS[10_000]}s (RUN_PERF=1)"
            )


class TestTransformationRegression:
    """Regression tests for transformations — ratio-based, CI-safe."""

    def _run_with_baseline(self, runner, rows_target, rows_baseline=100):
        """Run benchmark with a baseline for ratio comparison."""
        baseline_df = generate_transform_data(rows_baseline)
        # For ratio comparison, run a minimal representative benchmark
        # We use filter (fastest, most stable) as the baseline proxy
        baseline_results = run_filter_benchmarks(baseline_df)
        baseline_mean = baseline_results[0]["mean_s"] if baseline_results else 0.001

        target_df = generate_transform_data(rows_target)
        results = runner(target_df)
        return results, baseline_mean

    def test_filter_small_data_ratio(self):
        results, baseline_mean = self._run_with_baseline(
            run_filter_benchmarks, 10_000, 100
        )
        for r in results:
            ratio = r["mean_s"] / max(baseline_mean, 1e-9)
            assert ratio < TRANSFORM_RATIO, (
                f"filter {r['name']} ratio {ratio:.1f}x exceeds generous "
                f"threshold {TRANSFORM_RATIO}x (baseline={baseline_mean:.5f}s, "
                f"mean={r['mean_s']:.5f}s)"
            )
            if RUN_PERF:
                assert r["mean_s"] < 0.010, (
                    f"filter {r['name']} absolute mean {r['mean_s']}s exceeds "
                    "0.010s (RUN_PERF=1)"
                )

    def test_map_small_data_ratio(self):
        results, baseline_mean = self._run_with_baseline(
            run_map_benchmarks, 10_000, 100
        )
        for r in results:
            ratio = r["mean_s"] / max(baseline_mean, 1e-9)
            assert ratio < TRANSFORM_RATIO, (
                f"map {r['name']} ratio {ratio:.1f}x exceeds generous "
                f"threshold {TRANSFORM_RATIO}x (baseline={baseline_mean:.5f}s, "
                f"mean={r['mean_s']:.5f}s)"
            )
            if RUN_PERF:
                assert r["mean_s"] < 0.020, (
                    f"map {r['name']} absolute mean {r['mean_s']}s exceeds "
                    "0.020s (RUN_PERF=1)"
                )

    def test_aggregate_small_data_ratio(self):
        results, baseline_mean = self._run_with_baseline(
            run_aggregate_benchmarks, 10_000, 100
        )
        for r in results:
            ratio = r["mean_s"] / max(baseline_mean, 1e-9)
            assert ratio < TRANSFORM_RATIO, (
                f"aggregate {r['name']} ratio {ratio:.1f}x exceeds generous "
                f"threshold {TRANSFORM_RATIO}x (baseline={baseline_mean:.5f}s, "
                f"mean={r['mean_s']:.5f}s)"
            )
            if RUN_PERF:
                assert r["mean_s"] < 0.020, (
                    f"aggregate {r['name']} absolute mean {r['mean_s']}s exceeds "
                    "0.020s (RUN_PERF=1)"
                )

    def test_join_small_data_ratio(self):
        results, baseline_mean = self._run_with_baseline(
            run_join_benchmarks, 10_000, 100
        )
        for r in results:
            ratio = r["mean_s"] / max(baseline_mean, 1e-9)
            assert ratio < TRANSFORM_RATIO, (
                f"join {r['name']} ratio {ratio:.1f}x exceeds generous "
                f"threshold {TRANSFORM_RATIO}x (baseline={baseline_mean:.5f}s, "
                f"mean={r['mean_s']:.5f}s)"
            )
            if RUN_PERF:
                assert r["mean_s"] < 0.020, (
                    f"join {r['name']} absolute mean {r['mean_s']}s exceeds "
                    "0.020s (RUN_PERF=1)"
                )

    def test_union_small_data_ratio(self):
        results, baseline_mean = self._run_with_baseline(
            run_union_benchmarks, 10_000, 100
        )
        for r in results:
            ratio = r["mean_s"] / max(baseline_mean, 1e-9)
            assert ratio < TRANSFORM_RATIO, (
                f"union {r['name']} ratio {ratio:.1f}x exceeds generous "
                f"threshold {TRANSFORM_RATIO}x (baseline={baseline_mean:.5f}s, "
                f"mean={r['mean_s']:.5f}s)"
            )
            if RUN_PERF:
                assert r["mean_s"] < 0.005, (
                    f"union {r['name']} absolute mean {r['mean_s']}s exceeds "
                    "0.005s (RUN_PERF=1)"
                )

    def test_string_small_data_ratio(self):
        results, baseline_mean = self._run_with_baseline(
            run_string_benchmarks, 10_000, 100
        )
        for r in results:
            ratio = r["mean_s"] / max(baseline_mean, 1e-9)
            assert ratio < TRANSFORM_RATIO, (
                f"string {r['name']} ratio {ratio:.1f}x exceeds generous "
                f"threshold {TRANSFORM_RATIO}x (baseline={baseline_mean:.5f}s, "
                f"mean={r['mean_s']:.5f}s)"
            )
            if RUN_PERF:
                assert r["mean_s"] < 0.020, (
                    f"string {r['name']} absolute mean {r['mean_s']}s exceeds "
                    "0.020s (RUN_PERF=1)"
                )

    def test_date_small_data_ratio(self):
        results, baseline_mean = self._run_with_baseline(
            run_date_benchmarks, 10_000, 100
        )
        for r in results:
            ratio = r["mean_s"] / max(baseline_mean, 1e-9)
            assert ratio < TRANSFORM_RATIO, (
                f"date {r['name']} ratio {ratio:.1f}x exceeds generous "
                f"threshold {TRANSFORM_RATIO}x (baseline={baseline_mean:.5f}s, "
                f"mean={r['mean_s']:.5f}s)"
            )
            if RUN_PERF:
                assert r["mean_s"] < 0.020, (
                    f"date {r['name']} absolute mean {r['mean_s']}s exceeds "
                    "0.020s (RUN_PERF=1)"
                )

    def test_chain_4_steps_small_data_ratio(self):
        results, baseline_mean = self._run_with_baseline(
            run_chain_benchmarks, 10_000, 100
        )
        for r in results:
            ratio = r["mean_s"] / max(baseline_mean, 1e-9)
            assert ratio < TRANSFORM_RATIO, (
                f"chain {r['name']} ratio {ratio:.1f}x exceeds generous "
                f"threshold {TRANSFORM_RATIO}x (baseline={baseline_mean:.5f}s, "
                f"mean={r['mean_s']:.5f}s)"
            )
            if RUN_PERF:
                assert r["mean_s"] < 0.020, (
                    f"chain {r['name']} absolute mean {r['mean_s']}s exceeds "
                    "0.020s (RUN_PERF=1)"
                )


class TestDAGRegression:
    """Regression tests for DAG operations — ratio-based, CI-safe."""

    def test_topo_sort_10_nodes_ratio(self):
        baseline_dag = build_linear_dag(3)
        baseline_result = benchmark_topo_sort(baseline_dag, n_runs=50)
        baseline_mean = baseline_result["mean_us"]

        target_dag = build_linear_dag(10)
        target_result = benchmark_topo_sort(target_dag, n_runs=50)
        target_mean = target_result["mean_us"]

        ratio = target_mean / max(baseline_mean, 0.1)
        assert ratio < DAG_RATIO, (
            f"topo_sort 10-node/3-node ratio {ratio:.1f}x exceeds generous "
            f"threshold {DAG_RATIO}x (baseline={baseline_mean:.2f}us, "
            f"target={target_mean:.2f}us)"
        )
        if RUN_PERF:
            assert target_mean < 60.0, (
                f"topo_sort absolute mean {target_mean}us exceeds 60us (RUN_PERF=1)"
            )

    def test_parallel_groups_10_nodes_ratio(self):
        baseline_dag = build_linear_dag(3)
        baseline_result = benchmark_parallel_groups(baseline_dag, n_runs=50)
        baseline_mean = baseline_result["mean_us"]

        target_dag = build_linear_dag(10)
        target_result = benchmark_parallel_groups(target_dag, n_runs=50)
        target_mean = target_result["mean_us"]

        ratio = target_mean / max(baseline_mean, 0.1)
        assert ratio < DAG_RATIO, (
            f"parallel_groups 10-node/3-node ratio {ratio:.1f}x exceeds generous "
            f"threshold {DAG_RATIO}x (baseline={baseline_mean:.2f}us, "
            f"target={target_mean:.2f}us)"
        )
        if RUN_PERF:
            assert target_mean < 80.0, (
                f"parallel_groups absolute mean {target_mean}us exceeds 80us (RUN_PERF=1)"
            )

    def test_validate_10_nodes_ratio(self):
        baseline_dag = build_linear_dag(3)
        baseline_result = benchmark_validate(baseline_dag, n_runs=50)
        baseline_mean = baseline_result["mean_us"]

        target_dag = build_linear_dag(10)
        target_result = benchmark_validate(target_dag, n_runs=50)
        target_mean = target_result["mean_us"]

        ratio = target_mean / max(baseline_mean, 0.1)
        assert ratio < DAG_RATIO, (
            f"validate 10-node/3-node ratio {ratio:.1f}x exceeds generous "
            f"threshold {DAG_RATIO}x (baseline={baseline_mean:.2f}us, "
            f"target={target_mean:.2f}us)"
        )
        if RUN_PERF:
            assert target_mean < 60.0, (
                f"validate absolute mean {target_mean}us exceeds 60us (RUN_PERF=1)"
            )

    def test_from_dict_10_nodes_ratio(self):
        baseline_result = benchmark_from_dict(3, n_runs=25)
        baseline_mean = baseline_result["mean_us"]

        target_result = benchmark_from_dict(10, n_runs=25)
        target_mean = target_result["mean_us"]

        ratio = target_mean / max(baseline_mean, 0.1)
        assert ratio < DAG_RATIO, (
            f"from_dict 10-node/3-node ratio {ratio:.1f}x exceeds generous "
            f"threshold {DAG_RATIO}x (baseline={baseline_mean:.2f}us, "
            f"target={target_mean:.2f}us)"
        )
        if RUN_PERF:
            assert target_mean < 100.0, (
                f"from_dict absolute mean {target_mean}us exceeds 100us (RUN_PERF=1)"
            )
