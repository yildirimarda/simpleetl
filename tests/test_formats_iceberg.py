"""Tests for Apache Iceberg format reader/writer (v1.2, PLAN 9.3).

Uses a real Iceberg table backed by a SQLite ``SqlCatalog`` in a temp
warehouse directory — no Spark or external catalog required.
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

pyiceberg = pytest.importorskip("pyiceberg", reason="pyiceberg not installed")

from simpleetl.formats.factory import FormatFactory  # noqa: E402
from simpleetl.formats.iceberg import (  # noqa: E402
    IcebergReader,
    IcebergWriter,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TABLE = "analytics.people"


def _sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "score": [95.0, 82.0, 77.0],
        }
    )


def _write(
    warehouse: Path,
    df: pd.DataFrame,
    mode: str = "append",
    table: str = TABLE,
) -> None:
    IcebergWriter().write(df, str(warehouse), table=table, mode=mode)


def _load_table(warehouse: Path, table: str = TABLE):
    """Load the table directly with pyiceberg for verification.

    The SQLite catalog scopes tables by catalog name, so this must use
    the same name as the writer default (``"simpleetl"``).
    """
    from pyiceberg.catalog.sql import SqlCatalog

    catalog = SqlCatalog(
        "simpleetl",
        uri=f"sqlite:///{warehouse}/catalog.db",
        warehouse=f"file://{warehouse}",
    )
    return catalog.load_table(table)


# ---------------------------------------------------------------------------
# IcebergWriter
# ---------------------------------------------------------------------------


class TestIcebergWriter:
    def test_write_creates_catalog_and_table(self, tmp_path):
        _write(tmp_path / "wh", _sample_df())
        assert (tmp_path / "wh" / "catalog.db").exists()
        assert len(_load_table(tmp_path / "wh").scan().to_pandas()) == 3

    def test_write_append_mode(self, tmp_path):
        wh = tmp_path / "wh"
        _write(wh, _sample_df())
        df2 = pd.DataFrame({"id": [4], "name": ["Dave"], "score": [88.0]})
        _write(wh, df2, mode="append")
        assert len(_load_table(wh).scan().to_pandas()) == 4

    def test_write_overwrite_mode(self, tmp_path):
        wh = tmp_path / "wh"
        _write(wh, _sample_df())
        df2 = pd.DataFrame({"id": [99], "name": ["Zara"], "score": [100.0]})
        _write(wh, df2, mode="overwrite")

        result = _load_table(wh).scan().to_pandas()
        assert len(result) == 1
        assert result["name"].iloc[0] == "Zara"

    def test_write_error_mode_new_table(self, tmp_path):
        wh = tmp_path / "wh"
        _write(wh, _sample_df(), mode="error")
        assert len(_load_table(wh).scan().to_pandas()) == 3

    def test_write_error_mode_existing_table(self, tmp_path):
        wh = tmp_path / "wh"
        _write(wh, _sample_df())
        with pytest.raises(ValueError, match="already exists"):
            _write(wh, _sample_df(), mode="error")

    def test_write_invalid_mode(self, tmp_path):
        with pytest.raises(ValueError, match="Invalid mode"):
            _write(tmp_path / "wh", _sample_df(), mode="bad")

    def test_write_uri_destination(self, tmp_path):
        wh = tmp_path / "wh"
        uri = f"iceberg://{wh}?table={TABLE}"
        IcebergWriter().write(_sample_df(), uri)
        assert len(_load_table(wh).scan().to_pandas()) == 3

    def test_write_bare_table_uses_default_namespace(self, tmp_path):
        wh = tmp_path / "wh"
        _write(wh, _sample_df(), table="people")
        result = _load_table(wh, "default.people").scan().to_pandas()
        assert len(result) == 3

    def test_write_missing_table_raises(self, tmp_path):
        with pytest.raises(ValueError, match="No Iceberg table"):
            IcebergWriter().write(_sample_df(), str(tmp_path / "wh"))

    def test_write_import_error(self):
        original = sys.modules.get("pyiceberg")
        sys.modules["pyiceberg"] = None  # type: ignore[assignment]
        try:
            with pytest.raises(ImportError, match=r"simpleetl\[iceberg\]"):
                from simpleetl.formats.iceberg import _require_pyiceberg

                _require_pyiceberg()
        finally:
            if original is not None:
                sys.modules["pyiceberg"] = original
            else:
                del sys.modules["pyiceberg"]


# ---------------------------------------------------------------------------
# IcebergReader
# ---------------------------------------------------------------------------


class TestIcebergReader:
    def test_read_returns_dataframe(self, tmp_path):
        wh = tmp_path / "wh"
        _write(wh, _sample_df())
        df = IcebergReader().read(str(wh), table=TABLE)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3

    def test_read_uri_source(self, tmp_path):
        wh = tmp_path / "wh"
        _write(wh, _sample_df())
        df = IcebergReader().read(f"iceberg://{wh}?table={TABLE}")
        assert len(df) == 3

    def test_read_constructor_defaults(self, tmp_path):
        wh = tmp_path / "wh"
        _write(wh, _sample_df())
        reader = IcebergReader(str(wh), table=TABLE)
        assert len(reader.read()) == 3

    def test_read_column_subset(self, tmp_path):
        wh = tmp_path / "wh"
        _write(wh, _sample_df())
        df = IcebergReader().read(str(wh), table=TABLE, columns=["id", "name"])
        assert list(df.columns) == ["id", "name"]

    def test_read_row_filter(self, tmp_path):
        wh = tmp_path / "wh"
        _write(wh, _sample_df())
        df = IcebergReader().read(str(wh), table=TABLE, row_filter="id > 1")
        assert sorted(df["id"].tolist()) == [2, 3]

    def test_read_snapshot_time_travel(self, tmp_path):
        wh = tmp_path / "wh"
        _write(wh, _sample_df())
        df2 = pd.DataFrame({"id": [4], "name": ["Dave"], "score": [88.0]})
        _write(wh, df2, mode="append")

        first_snapshot = _load_table(wh).history()[0].snapshot_id
        df_old = IcebergReader().read(str(wh), table=TABLE, snapshot_id=first_snapshot)
        assert len(df_old) == 3

    def test_read_latest_snapshot(self, tmp_path):
        wh = tmp_path / "wh"
        _write(wh, _sample_df())
        df2 = pd.DataFrame({"id": [4], "name": ["Dave"], "score": [88.0]})
        _write(wh, df2, mode="append")

        assert len(IcebergReader().read(str(wh), table=TABLE)) == 4

    def test_read_missing_table_raises(self, tmp_path):
        with pytest.raises(ValueError, match="No Iceberg table"):
            IcebergReader().read(str(tmp_path / "wh"))


# ---------------------------------------------------------------------------
# IcebergReader.read_chunks
# ---------------------------------------------------------------------------


class TestIcebergReaderChunks:
    def test_read_chunks_yields_dataframes(self, tmp_path):
        wh = tmp_path / "wh"
        _write(wh, _sample_df())
        chunks = list(IcebergReader().read_chunks(str(wh), chunk_size=2, table=TABLE))
        assert all(isinstance(c, pd.DataFrame) for c in chunks)

    def test_read_chunks_respects_chunk_size(self, tmp_path):
        wh = tmp_path / "wh"
        _write(wh, _sample_df())
        chunks = list(IcebergReader().read_chunks(str(wh), chunk_size=2, table=TABLE))
        assert all(len(c) <= 2 for c in chunks)
        assert sum(len(c) for c in chunks) == 3

    def test_read_chunks_single_chunk(self, tmp_path):
        wh = tmp_path / "wh"
        _write(wh, _sample_df())
        chunks = list(IcebergReader().read_chunks(str(wh), chunk_size=100, table=TABLE))
        assert sum(len(c) for c in chunks) == 3

    def test_read_chunks_column_subset(self, tmp_path):
        wh = tmp_path / "wh"
        _write(wh, _sample_df())
        chunks = list(
            IcebergReader().read_chunks(
                str(wh), chunk_size=10, table=TABLE, columns=["id"]
            )
        )
        for chunk in chunks:
            assert list(chunk.columns) == ["id"]


# ---------------------------------------------------------------------------
# Factory integration
# ---------------------------------------------------------------------------


class TestFactoryIceberg:
    def test_get_reader_for_iceberg_uri(self):
        reader = FormatFactory.get_reader("iceberg:///data/wh?table=ns.t")
        assert isinstance(reader, IcebergReader)

    def test_get_writer_for_iceberg_uri(self):
        writer = FormatFactory.get_writer("iceberg:///data/wh?table=ns.t")
        assert isinstance(writer, IcebergWriter)

    def test_detect_format(self):
        info = FormatFactory.detect_format("iceberg:///data/wh?table=ns.t")
        assert info["format"] == "iceberg"

    def test_supported_formats_includes_iceberg(self):
        formats = FormatFactory.supported_formats()
        assert "iceberg" in formats
        assert formats["iceberg"] == "iceberg://"

    def test_factory_roundtrip(self, tmp_path):
        uri = f"iceberg://{tmp_path / 'wh'}?table={TABLE}"
        FormatFactory.get_writer(uri).write(_sample_df(), uri)
        df = FormatFactory.get_reader(uri).read(uri)
        assert len(df) == 3
