#!/usr/bin/env python3
"""
Performance benchmarking suite for SimpleETL data formats.

Benchmarks read/write performance across different formats and data sizes.
Run with: uv run python scripts/benchmark.py
"""

import time
import os
import sys
import tempfile
import statistics
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from simpleetl.formats import (
    CSVReader, CSVWriter,
    JSONReader, JSONWriter,
    ParquetReader, ParquetWriter,
    AvroReader, AvroWriter,
    OrcReader, OrcWriter,
    XMLReader, XMLWriter,
    ExcelReader, ExcelWriter,
)


def generate_data(rows: int) -> pd.DataFrame:
    """Generate a sample DataFrame with the given number of rows."""
    return pd.DataFrame({
        "id": range(rows),
        "name": [f"user_{i}" for i in range(rows)],
        "age": [20 + (i % 50) for i in range(rows)],
        "score": [round(100 * (i % 100) / 100, 2) for i in range(rows)],
        "city": [f"city_{i % 10}" for i in range(rows)],
    })


def benchmark_format(name: str, reader, writer, df: pd.DataFrame, suffix: str, iterations: int = 3) -> dict:
    """Benchmark a format's write and read performance."""
    write_times = []
    read_times = []
    file_size = 0

    for _ in range(iterations):
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as f:
            temp_path = f.name

        try:
            # Write benchmark
            kwargs = {}
            if name == "JSON":
                kwargs = {"orient": "records", "lines": False}
            elif name == "XML":
                kwargs = {"root_element": "data", "record_element": "row"}
            start = time.perf_counter()
            writer.write(df, temp_path, **kwargs)
            write_times.append(time.perf_counter() - start)
            file_size = os.path.getsize(temp_path)

            # Read benchmark
            start = time.perf_counter()
            df_read = reader.read(temp_path)
            read_times.append(time.perf_counter() - start)

            # Verify data integrity
            assert len(df_read) == len(df), f"Row count mismatch for {name}"
        finally:
            os.unlink(temp_path)

    return {
        "format": name,
        "rows": len(df),
        "file_size_kb": round(file_size / 1024, 1),
        "write_ms": round(statistics.median(write_times) * 1000, 1),
        "read_ms": round(statistics.median(read_times) * 1000, 1),
    }


def print_results(results: list) -> None:
    """Print benchmark results as a formatted table."""
    header = f"{'Format':<10} {'Rows':>8} {'Size(KB)':>10} {'Write(ms)':>10} {'Read(ms)':>10}"
    print(header)
    print("-" * len(header))
    for r in results:
        print(f"{r['format']:<10} {r['rows']:>8} {r['file_size_kb']:>10.1f} {r['write_ms']:>10.1f} {r['read_ms']:>10.1f}")


def main():
    """Run all benchmarks."""
    sizes = [1_000, 10_000, 100_000]
    formats = [
        ("CSV", CSVReader(), CSVWriter(), ".csv"),
        ("JSON", JSONReader(), JSONWriter(), ".json"),
        ("Parquet", ParquetReader(), ParquetWriter(), ".parquet"),
        ("Avro", AvroReader(), AvroWriter(), ".avro"),
        ("ORC", OrcReader(), OrcWriter(), ".orc"),
        ("Excel", ExcelReader(), ExcelWriter(), ".xlsx"),
    ]

    for size in sizes:
        print(f"\n{'='*52}")
        print(f"  Benchmark: {size:,} rows")
        print(f"{'='*52}")

        df = generate_data(size)
        results = []

        for name, reader, writer, suffix in formats:
            try:
                result = benchmark_format(name, reader, writer, df, suffix)
                results.append(result)
            except Exception as e:
                print(f"  {name}: FAILED ({e})")

        print_results(results)


if __name__ == "__main__":
    main()
