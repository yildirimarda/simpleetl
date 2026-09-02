"""Verify benchmark scripts explicitly pass engine='pandas'."""

import ast


def _find_engine_pandas_calls(filepath: str) -> list:
    """Parse file and find string arguments containing 'engine*' with 'pandas'."""
    with open(filepath) as f:
        source = f.read()
    tree = ast.parse(source)
    calls = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            # Check keyword args for engine="pandas"
            for kw in node.keywords:
                if kw.arg == "engine" and isinstance(kw.value, ast.Constant) and kw.value.value == "pandas":
                    calls.append((node.func.__name__ if hasattr(node.func, '__name__') else str(node.func), kw.arg))
    return calls


def test_benchmark_read_write_has_engine_pandas():
    with open("benchmarks/benchmark_read_write.py") as f:
        source = f.read()
    assert '"pandas"' in source
    assert '"engine"' in source


def test_benchmark_streaming_has_engine_pandas():
    with open("benchmarks/benchmark_streaming.py") as f:
        source = f.read()
    assert 'engine="pandas"' in source


def test_scripts_benchmark_has_engine_pandas():
    with open("scripts/benchmark.py") as f:
        source = f.read()
    assert '"pandas"' in source
    assert '"engine"' in source
