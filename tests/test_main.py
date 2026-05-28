"""
Tests for the __main__ module (python -m simpleetl entry point).
"""

import runpy
import tempfile
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import simpleetl


class TestMainModule:
    """Test the __main__ entry point."""

    def test_main_module_invokes_cli_main(self):
        """Test that __main__.py calls cli.main() when executed as __main__."""
        with patch("simpleetl.cli.main") as mock_main:
            runpy.run_path(
                "/Users/ardayildirim/Downloads/simpleetl/src/simpleetl/__main__.py",
                run_name="__main__",
            )
            mock_main.assert_called_once()


class TestTopLevelAPI:
    """Tests for the top-level read() and write() convenience functions."""

    def test_read_csv_auto_detect(self):
        """Top-level read() detects format from file extension for CSV."""
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
            f.write("id,name\n1,Alice\n2,Bob\n")
            f.flush()
            df = simpleetl.read(f.name)
            assert len(df) == 2
            assert list(df.columns) == ["id", "name"]
        Path(f.name).unlink()

    def test_read_parquet_auto_detect(self):
        """Top-level read() detects format from file extension for Parquet."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = f"{tmpdir}/test.parquet"
            df = pd.DataFrame({"id": [1, 2], "value": [10, 20]})
            df.to_parquet(path)

            result = simpleetl.read(path)
            assert len(result) == 2
            assert list(result.columns) == ["id", "value"]

    def test_read_explicit_format(self):
        """Top-level read() accepts explicit format parameter."""
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            # Write as parquet, read with explicit format
            df = pd.DataFrame({"id": [1, 2], "name": ["test", "test2"]})
            df.to_parquet(f.name)
            result = simpleetl.read(f.name, format="parquet")
            assert len(result) == 2
            assert list(result.columns) == ["id", "name"]
        Path(f.name).unlink()

    def test_write_csv_auto_detect(self):
        """Top-level write() detects format from file extension for CSV."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = f"{tmpdir}/output.csv"
            df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
            simpleetl.write(df, path)
            assert Path(path).exists()

    def test_write_parquet_auto_detect(self):
        """Top-level write() detects format from file extension for Parquet."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = f"{tmpdir}/output.parquet"
            df = pd.DataFrame({"id": [1, 2], "value": [100, 200]})
            simpleetl.write(df, path)
            assert Path(path).exists()

    def test_write_explicit_format(self):
        """Top-level write() accepts explicit format parameter."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = f"{tmpdir}/data.json"
            df = pd.DataFrame({"id": [1], "name": ["test"]})
            simpleetl.write(df, path, format="json")
            assert Path(path).exists()
