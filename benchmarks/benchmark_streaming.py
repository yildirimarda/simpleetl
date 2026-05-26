"""Benchmark streaming/chunked read/write performance."""

import time
import tempfile
import os
import tracemalloc
from typing import Iterator

import pandas as pd
import numpy as np

from simpleetl.formats.csv import CSVReader, CSVWriter
from simpleetl.formats.json import JSONReader, JSONWriter
from simpleetl.formats.parquet import ParquetReader, ParquetWriter


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


def chunk_iterator(
    df: pd.DataFrame, chunk_size: int
) -> Iterator[pd.DataFrame]:
    """Yield successive chunks of a DataFrame."""
    for start in range(0, len(df), chunk_size):
        yield df.iloc[start:start + chunk_size].reset_index(drop=True)


def measure_time_and_memory(
    func: callable, *args: object, **kwargs: object
) -> tuple:
    """Return (elapsed_seconds, peak_memory_mb) for a function call."""
    tracemalloc.start()
    start = time.perf_counter()
    func(*args, **kwargs)
    elapsed = time.perf_counter() - start
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return elapsed, peak / (1024 * 1024)


def benchmark_csv(df: pd.DataFrame, chunk_size: int) -> dict:
    """Benchmark CSV chunked vs non-chunked."""
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "test.csv")

        # Non-chunked
        t1, m1 = measure_time_and_memory(
            lambda: CSVWriter().write(df, path)
        )
        t2, m2 = measure_time_and_memory(
            lambda: CSVReader().read(path)
        )
        non_chunked_time = t1 + t2
        non_chunked_mem = max(m1, m2)

        # Chunked
        t1, m1 = measure_time_and_memory(
            lambda: CSVWriter().write_chunks(
                chunk_iterator(df, chunk_size), path
            )
        )
        chunks = []
        t2, m2 = measure_time_and_memory(
            lambda: chunks.extend(CSVReader().read_chunks(path, chunk_size))
        )
        chunked_time = t1 + t2
        chunked_mem = max(m1, m2)

    return {
        "format": "csv",
        "chunk_size": chunk_size,
        "non_chunked_s": non_chunked_time,
        "chunked_s": chunked_time,
        "non_chunked_mem_mb": non_chunked_mem,
        "chunked_mem_mb": chunked_mem,
    }


def benchmark_json(df: pd.DataFrame, chunk_size: int) -> dict:
    """Benchmark JSON chunked vs non-chunked."""
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "test.json")

        # Non-chunked
        t1, m1 = measure_time_and_memory(
            lambda: JSONWriter().write(df, path)
        )
        t2, m2 = measure_time_and_memory(
            lambda: JSONReader().read(path, orient="records", lines=True)
        )
        non_chunked_time = t1 + t2
        non_chunked_mem = max(m1, m2)

        # Chunked: write each chunk as a separate JSON lines file,
        # then read them back individually passing lines=True to the reader.
        chunks_list = list(chunk_iterator(df, chunk_size))
        chunk_paths = [
            os.path.join(tmpdir, f"chunk_{i}.json")
            for i in range(len(chunks_list))
        ]
        t1, m1 = measure_time_and_memory(
            lambda: [
                JSONWriter().write(chunk, chunk_paths[i])
                for i, chunk in enumerate(chunks_list)
            ]
        )
        results = []
        t2, m2 = measure_time_and_memory(
            lambda: results.extend(
                JSONReader().read(p, orient="records", lines=True)
                for p in chunk_paths
            )
        )
        chunked_time = t1 + t2
        chunked_mem = max(m1, m2)

    return {
        "format": "json",
        "chunk_size": chunk_size,
        "non_chunked_s": non_chunked_time,
        "chunked_s": chunked_time,
        "non_chunked_mem_mb": non_chunked_mem,
        "chunked_mem_mb": chunked_mem,
    }


def benchmark_parquet(df: pd.DataFrame, chunk_size: int) -> dict:
    """Benchmark Parquet chunked vs non-chunked."""
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "test.parquet")

        # Non-chunked
        t1, m1 = measure_time_and_memory(
            lambda: ParquetWriter().write(df, path)
        )
        t2, m2 = measure_time_and_memory(
            lambda: ParquetReader().read(path)
        )
        non_chunked_time = t1 + t2
        non_chunked_mem = max(m1, m2)

        # Chunked
        t1, m1 = measure_time_and_memory(
            lambda: ParquetWriter().write_chunks(
                chunk_iterator(df, chunk_size), path
            )
        )
        chunks = []
        t2, m2 = measure_time_and_memory(
            lambda: chunks.extend(
                ParquetReader().read_chunks(path, chunk_size)
            )
        )
        chunked_time = t1 + t2
        chunked_mem = max(m1, m2)

    return {
        "format": "parquet",
        "chunk_size": chunk_size,
        "non_chunked_s": non_chunked_time,
        "chunked_s": chunked_time,
        "non_chunked_mem_mb": non_chunked_mem,
        "chunked_mem_mb": chunked_mem,
    }


if __name__ == "__main__":
    chunk_size = 10_000
    for size in [100_000, 500_000]:
        df = generate_test_data(size)
        print(f"\n{'='*60}")
        print(f"Dataset: {size:,} rows, {len(df.columns)} columns, "
              f"chunk_size={chunk_size:,}")
        print(f"{'='*60}")
        for bench_fn in [benchmark_csv, benchmark_json, benchmark_parquet]:
            try:
                r = bench_fn(df, chunk_size)
                print(
                    f"  {r['format']:10s}  "
                    f"non-chunked: {r['non_chunked_s']:.3f}s "
                    f"({r['non_chunked_mem_mb']:.1f} MB)  |  "
                    f"chunked: {r['chunked_s']:.3f}s "
                    f"({r['chunked_mem_mb']:.1f} MB)"
                )
            except Exception as e:
                print(f"  {bench_fn.__name__:10s}  ERROR: {e}")
