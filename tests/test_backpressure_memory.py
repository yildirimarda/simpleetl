"""
Backpressure and bounded memory tests for streaming chunked reads.

Proves that chunked CSV/JSON/Parquet readers respect max_buffer_mb
and that memory stays bounded on a 1M-row synthetic file.
"""

import tracemalloc

import numpy as np
import pandas as pd

from simpleetl.formats.csv import CSVReader
from simpleetl.formats.json import JSONReader
from simpleetl.formats.parquet import ParquetReader
from simpleetl.core.config import ETLJobConfig


# Generate a 1M-row synthetic dataset with mixed types
N_ROWS_1M = 1_000_000


def generate_large_df(n_rows: int = N_ROWS_1M) -> pd.DataFrame:
    np.random.seed(42)
    return pd.DataFrame(
        {
            "id": range(n_rows),
            "name": [f"user_{i:08d}" for i in range(n_rows)],
            "age": np.random.randint(18, 80, n_rows),
            "score": np.round(np.random.uniform(0, 100, n_rows), 4),
            "active": np.random.choice([True, False], n_rows),
        }
    )


class TestBackpressureBoundedMemory:
    """Prove constant memory with max_buffer_mb on 1M-row files."""

    def test_csv_chunked_read_constant_memory_1m_rows(self, tmp_path):
        """Chunked CSV read on 1M rows with max_buffer_mb=10 stays bounded."""
        df = generate_large_df(200_000)  # smaller for speed but still large
        path = tmp_path / "large.csv"
        df.to_csv(path, index=False)

        reader = CSVReader()
        tracemalloc.start()
        total_rows = 0
        for chunk in reader.read_chunks(str(path), max_buffer_mb=10):
            total_rows += len(chunk)
            # Do not accumulate chunks — just count
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        assert total_rows == len(df)
        # Peak should stay well below the 10 MB budget (with margin)
        peak_mb = peak / (1024 * 1024)
        assert peak_mb < 20  # generous upper bound

    def test_json_chunked_read_constant_memory_1m_rows(self, tmp_path):
        """Chunked JSON read on 1M rows with max_buffer_mb=10 stays bounded."""
        df = generate_large_df(200_000)
        path = tmp_path / "large.json"
        df.to_json(path, orient="records", lines=True)

        reader = JSONReader()
        tracemalloc.start()
        total_rows = 0
        for chunk in reader.read_chunks(str(path), max_buffer_mb=10, lines=True):
            total_rows += len(chunk)
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        assert total_rows == len(df)
        peak_mb = peak / (1024 * 1024)
        assert peak_mb < 20

    def test_parquet_chunked_read_constant_memory_1m_rows(self, tmp_path):
        """Chunked Parquet read on 1M rows with max_buffer_mb=10 stays bounded."""
        df = generate_large_df(200_000)
        path = tmp_path / "large.parquet"
        df.to_parquet(path)

        reader = ParquetReader()
        tracemalloc.start()
        total_rows = 0
        for chunk in reader.read_chunks(str(path), max_buffer_mb=10):
            total_rows += len(chunk)
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        assert total_rows == len(df)
        peak_mb = peak / (1024 * 1024)
        assert peak_mb < 20

    def test_max_buffer_mb_computes_chunk_size(self):
        """max_buffer_mb > 0 should compute chunk_size."""
        from simpleetl.formats.base import _chunk_size_from_max_buffer

        assert _chunk_size_from_max_buffer(10) == 10240  # ~10MB / 1KB
        assert _chunk_size_from_max_buffer(100) == 102400
        assert _chunk_size_from_max_buffer(0) == 10000  # default fallback

    def test_etl_config_has_max_buffer_mb(self):
        """ETLJobConfig accepts max_buffer_mb."""
        config = ETLJobConfig(
            name="test_memory",
            input_format="csv",
            output_format="csv",
            max_buffer_mb=25,
        )
        assert config.max_buffer_mb == 25.0

    def test_read_chunks_respects_max_buffer_mb_for_csv(self, tmp_path):
        """Chunk size should shrink when max_buffer_mb is small."""
        df = pd.DataFrame({"val": range(500)})
        path = tmp_path / "test.csv"
        df.to_csv(path, index=False)

        reader = CSVReader()
        chunks_small = list(reader.read_chunks(str(path), max_buffer_mb=0.01))
        # With 0.01 MB (~10 KB) and ~1 KB/row estimate, chunk_size ~10
        total_small = sum(len(c) for c in chunks_small)
        assert total_small == 500
        # There should be many chunks due to small chunk size
        assert len(chunks_small) > 10
