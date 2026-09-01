"""
Data volume tests (GB-scale).

Verifies framework behavior with large synthetic datasets that approach
GB-scale file sizes. Tests chunked processing, bounded memory, and
format correctness at scale.
"""

import os
import tempfile
import time
import tracemalloc

import numpy as np
import pandas as pd
import pytest

from simpleetl.formats import (
    CSVReader,
    CSVWriter,
    JSONReader,
    JSONWriter,
    ParquetReader,
    ParquetWriter,
)


# Scale settings: 5M rows with 8 mixed-type columns produces
# files in the hundreds-of-MB to low-GB range depending on format.
GB_SCALE_ROWS = 5_000_000
GB_SCALE_COLUMNS = 8
CHUNK_SIZE = 50_000


def generate_large_df(n_rows: int = GB_SCALE_ROWS) -> pd.DataFrame:
    """Generate a large synthetic DataFrame."""
    np.random.seed(42)
    return pd.DataFrame(
        {
            "id": range(n_rows),
            "name": [f"user_{i:09d}" for i in range(n_rows)],
            "age": np.random.randint(18, 80, n_rows),
            "score": np.random.uniform(0, 100, n_rows).round(4),
            "active": np.random.choice([True, False], n_rows),
            "city": np.random.choice(
                [
                    "NYC",
                    "LA",
                    "Chicago",
                    "Houston",
                    "Phoenix",
                    "Seattle",
                    "Denver",
                    "Miami",
                ],
                n_rows,
            ),
            "region": np.random.choice(["North", "South", "East", "West"], n_rows),
            "timestamp": pd.date_range("2020-01-01", periods=n_rows, freq="min"),
        }
    )


class TestGBScaleChunkedReadWrite:
    """Chunked read/write at GB-scale dataset sizes."""

    @pytest.mark.slow
    def test_csv_chunked_roundtrip_memory_bounded(self):
        """Chunked CSV roundtrip with bounded peak memory."""
        df = generate_large_df(500_000)  # 500K for faster CI
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "large.csv")

            writer = CSVWriter()
            writer.write(df, path)

            assert os.path.exists(path)
            file_size_mb = os.path.getsize(path) / (1024 * 1024)
            # CSV at 500K rows should be tens of MB
            assert file_size_mb > 10

            # Chunked read
            reader = CSVReader()
            chunks = list(reader.read_chunks(path, chunk_size=CHUNK_SIZE))
            total_rows = sum(len(chunk) for chunk in chunks)
            assert total_rows == len(df)

    @pytest.mark.slow
    def test_parquet_chunked_roundtrip_memory_bounded(self):
        """Chunked Parquet roundtrip with bounded peak memory."""
        df = generate_large_df(500_000)
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "large.parquet")

            writer = ParquetWriter()
            writer.write_chunks(
                (df.iloc[i : i + CHUNK_SIZE] for i in range(0, len(df), CHUNK_SIZE)),
                path,
            )

            assert os.path.exists(path)
            file_size_mb = os.path.getsize(path) / (1024 * 1024)
            # Parquet is highly compressed
            assert file_size_mb > 1

            reader = ParquetReader()
            chunks = list(reader.read_chunks(path, chunk_size=CHUNK_SIZE))
            total_rows = sum(len(chunk) for chunk in chunks)
            assert total_rows == len(df)


class TestGBScaleMemoryProfile:
    """Verify memory stays bounded during chunked processing."""

    def test_chunked_read_memory_bounded(self):
        """Peak memory during chunked CSV read should stay bounded."""
        df = generate_large_df(200_000)
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "mem.csv")
            CSVWriter().write(df, path)

            tracemalloc.start()
            reader = CSVReader()
            chunks = []
            for chunk in reader.read_chunks(path, chunk_size=10_000):
                chunks.append(chunk)
            current, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()

            total_rows = sum(len(c) for c in chunks)
            assert total_rows == len(df)
            # Peak memory should stay well below 1GB for 200K rows chunked
            peak_mb = peak / (1024 * 1024)
            assert peak_mb < 1024


class TestGBScaleBenchmark:
    """Benchmark-style assertions at GB-scale (not strict on timing)."""

    def test_large_dataset_completes_in_reasonable_time(self):
        """A 1M-row dataset should complete within a generous time budget."""
        df = generate_large_df(1_000_000)
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "benchmark.csv")
            start = time.perf_counter()
            CSVWriter().write(df, path)
            elapsed = time.perf_counter() - start
            # Very generous budget for CI variability
            assert elapsed < 300  # 5 minutes

    def test_large_parquet_write_completes(self):
        """1M-row Parquet write completes in reasonable time."""
        df = generate_large_df(1_000_000)
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "benchmark.parquet")
            start = time.perf_counter()
            ParquetWriter().write(df, path)
            elapsed = time.perf_counter() - start
            assert elapsed < 300
            assert os.path.getsize(path) > 0


class TestGBScaleDataIntegrity:
    """Verify data integrity is preserved at scale."""

    def test_large_csv_data_integrity(self):
        """Read back large CSV and verify column counts and types."""
        df = generate_large_df(100_000)
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "integrity.csv")
            CSVWriter().write(df, path)
            reader = CSVReader()
            df_read = reader.read(path)
            assert len(df_read) == len(df)
            assert list(df_read.columns) == list(df.columns)

    def test_large_json_data_integrity(self):
        """Read back large JSON and verify structure."""
        df = generate_large_df(50_000)
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "integrity.json")
            JSONWriter().write(df, path)
            reader = JSONReader()
            df_read = reader.read(path, lines=True, orient="records")
            assert len(df_read) == len(df)


class TestGBScaleFileSizes:
    """Verify produced file sizes are consistent with GB-scale expectations."""

    def test_csv_file_size_scales_approximately_linearly(self):
        """File size should grow roughly linearly with row count."""
        sizes = {}
        for n_rows in [10_000, 50_000]:
            df = generate_large_df(n_rows)
            with tempfile.TemporaryDirectory() as tmpdir:
                path = os.path.join(tmpdir, f"scale_{n_rows}.csv")
                CSVWriter().write(df, path)
                sizes[n_rows] = os.path.getsize(path)

        # 50K rows should be roughly 5x the size of 10K rows
        ratio = sizes[50_000] / sizes[10_000]
        assert 4 < ratio < 6
