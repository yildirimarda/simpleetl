"""Benchmark read/write performance for all supported formats."""

import time
import statistics
import tempfile
import os

import pandas as pd
import numpy as np

from simpleetl.formats import FormatFactory


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
    })


def benchmark_format(fmt: str, df: pd.DataFrame, n_runs: int = 5) -> dict:
    """Benchmark write+read for a format. Returns timing stats."""
    times = []
    ext_map = {
        "csv": ".csv",
        "json": ".json",
        "parquet": ".parquet",
    }
    ext = ext_map.get(fmt, f".{fmt}")
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, f"test{ext}")
        for _ in range(n_runs):
            start = time.perf_counter()
            writer = FormatFactory.get_writer(fmt)
            writer.write(df, path)
            reader = FormatFactory.get_reader(fmt)
            _ = reader.read(path)
            elapsed = time.perf_counter() - start
            times.append(elapsed)
    return {
        "format": fmt,
        "rows": len(df),
        "mean_s": statistics.mean(times),
        "median_s": statistics.median(times),
        "min_s": min(times),
        "max_s": max(times),
    }


if __name__ == "__main__":
    for size in [1_000, 10_000, 100_000]:
        df = generate_test_data(size)
        print(f"\n{'='*60}")
        print(f"Dataset: {size:,} rows, {len(df.columns)} columns")
        print(f"{'='*60}")
        for fmt in ["csv", "json", "parquet"]:
            try:
                result = benchmark_format(fmt, df)
                print(
                    f"  {fmt:10s}  mean={result['mean_s']:.3f}s  "
                    f"median={result['median_s']:.3f}s"
                )
            except Exception as e:
                print(f"  {fmt:10s}  ERROR: {e}")
