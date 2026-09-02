"""Verify performance benchmarks are documented."""

import os


def test_performance_documentation_exists():
    assert os.path.isfile("docs/performance.md")


def test_performance_doc_references_benchmarks():
    with open("docs/performance.md") as f:
        content = f.read()
    assert "benchmark_read_write" in content
    assert "benchmark_transformations" in content
    assert "benchmark_streaming" in content
    assert "benchmark_dag" in content


def test_performance_doc_has_version():
    with open("docs/performance.md") as f:
        content = f.read()
    assert "v0.2.0" in content


def test_benchmark_doc_exists():
    assert os.path.isfile("docs/BENCHMARKS.md")


def test_benchmark_doc_has_pandas_baseline():
    with open("docs/BENCHMARKS.md") as f:
        content = f.read()
    assert "pandas" in content.lower()
    assert "1,000,000" in content


def test_benchmark_doc_has_read_write_results():
    with open("docs/BENCHMARKS.md") as f:
        content = f.read()
    assert "CSV" in content
    assert "JSON" in content
    assert "Parquet" in content
