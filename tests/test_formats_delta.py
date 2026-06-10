"""Tests for Delta Lake format reader/writer (Phase 8.5).

Uses a real Delta table written to a temp directory — no Spark required.
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

deltalake = pytest.importorskip("deltalake", reason="deltalake not installed")

from simpleetl.formats.delta import DeltaLakeReader, DeltaLakeWriter  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "score": [95.0, 82.0, 77.0],
        }
    )


def _write_delta(path: str, df: pd.DataFrame, mode: str = "overwrite") -> None:
    DeltaLakeWriter().write(df, path, mode=mode)


# ---------------------------------------------------------------------------
# DeltaLakeWriter
# ---------------------------------------------------------------------------


class TestDeltaLakeWriter:
    def test_write_creates_delta_log(self, tmp_path):
        path = str(tmp_path / "delta")
        _write_delta(path, _sample_df())
        assert (Path(path) / "_delta_log").exists()

    def test_write_overwrite_mode(self, tmp_path):
        path = str(tmp_path / "delta")
        _write_delta(path, _sample_df())
        df2 = pd.DataFrame({"id": [99], "name": ["Zara"], "score": [100.0]})
        _write_delta(path, df2, mode="overwrite")

        dt = deltalake.DeltaTable(path)
        result = dt.to_pandas()
        assert len(result) == 1
        assert result["name"].iloc[0] == "Zara"

    def test_write_append_mode(self, tmp_path):
        path = str(tmp_path / "delta")
        _write_delta(path, _sample_df())
        df2 = pd.DataFrame({"id": [4], "name": ["Dave"], "score": [88.0]})
        _write_delta(path, df2, mode="append")

        dt = deltalake.DeltaTable(path)
        result = dt.to_pandas()
        assert len(result) == 4

    def test_write_invalid_mode(self, tmp_path):
        path = str(tmp_path / "delta")
        with pytest.raises(ValueError, match="Invalid mode"):
            DeltaLakeWriter().write(_sample_df(), path, mode="bad")

    def test_write_partition_by(self, tmp_path):
        path = str(tmp_path / "delta")
        df = pd.DataFrame(
            {"id": [1, 2, 3, 4], "region": ["us", "us", "eu", "eu"], "val": range(4)}
        )
        DeltaLakeWriter().write(df, path, partition_by=["region"])
        # Partitioned files should be created
        assert any(
            (Path(path) / d).is_dir()
            for d in ("region=us", "region=eu")
        )

    def test_write_import_error(self, tmp_path):
        original = sys.modules.get("deltalake")
        sys.modules["deltalake"] = None  # type: ignore[assignment]
        try:
            with pytest.raises(ImportError, match="deltalake"):
                from simpleetl.formats.delta import _require_deltalake
                _require_deltalake()
        finally:
            if original is not None:
                sys.modules["deltalake"] = original
            else:
                del sys.modules["deltalake"]


# ---------------------------------------------------------------------------
# DeltaLakeReader
# ---------------------------------------------------------------------------


class TestDeltaLakeReader:
    def test_read_returns_dataframe(self, tmp_path):
        path = str(tmp_path / "delta")
        _write_delta(path, _sample_df())
        df = DeltaLakeReader().read(path)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3

    def test_read_column_subset(self, tmp_path):
        path = str(tmp_path / "delta")
        _write_delta(path, _sample_df())
        df = DeltaLakeReader().read(path, columns=["id", "name"])
        assert list(df.columns) == ["id", "name"]

    def test_read_time_travel_version(self, tmp_path):
        path = str(tmp_path / "delta")
        _write_delta(path, _sample_df())
        df2 = pd.DataFrame({"id": [4], "name": ["Dave"], "score": [88.0]})
        _write_delta(path, df2, mode="append")

        # version 0 is the original 3-row write
        df_v0 = DeltaLakeReader().read(path, version=0)
        assert len(df_v0) == 3

    def test_read_latest_version(self, tmp_path):
        path = str(tmp_path / "delta")
        _write_delta(path, _sample_df())
        df2 = pd.DataFrame({"id": [4], "name": ["Dave"], "score": [88.0]})
        _write_delta(path, df2, mode="append")

        df_latest = DeltaLakeReader().read(path)
        assert len(df_latest) == 4


# ---------------------------------------------------------------------------
# DeltaLakeReader.read_chunks
# ---------------------------------------------------------------------------


class TestDeltaLakeReaderChunks:
    def test_read_chunks_yields_dataframes(self, tmp_path):
        path = str(tmp_path / "delta")
        _write_delta(path, _sample_df())
        chunks = list(DeltaLakeReader().read_chunks(path, chunk_size=2))
        assert all(isinstance(c, pd.DataFrame) for c in chunks)

    def test_read_chunks_total_rows(self, tmp_path):
        path = str(tmp_path / "delta")
        _write_delta(path, _sample_df())
        chunks = list(DeltaLakeReader().read_chunks(path, chunk_size=2))
        total = sum(len(c) for c in chunks)
        assert total == 3

    def test_read_chunks_column_subset(self, tmp_path):
        path = str(tmp_path / "delta")
        _write_delta(path, _sample_df())
        chunks = list(
            DeltaLakeReader().read_chunks(path, chunk_size=10, columns=["id"])
        )
        for chunk in chunks:
            assert list(chunk.columns) == ["id"]
