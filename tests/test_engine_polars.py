"""
Tests for polars interop helpers and the CSV/Parquet polars fast paths.
"""

import logging
import sys
from unittest import mock

import pandas as pd
import pytest

import simpleetl.formats.parquet as parquet_module
from simpleetl.core.engine import (
    from_polars,
    is_polars_available,
    polars_sql_transform,
    polars_transform,
    to_polars,
    validate_engine,
)
from simpleetl.formats import (
    CSVReader,
    CSVWriter,
    ParquetReader,
    ParquetWriter,
)

pl = pytest.importorskip("polars")


@pytest.fixture
def df() -> pd.DataFrame:
    """Sample DataFrame with mixed dtypes."""
    return pd.DataFrame(
        {
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, 30, 35],
            "score": [1.5, 2.5, 3.5],
        }
    )


class FakeFilesystem:
    """Minimal fsspec-style filesystem backed by a local file."""

    def __init__(self, local_path: str) -> None:
        self.local_path = local_path
        self.opened: list = []

    def open(self, path: str, mode: str):
        """Record the access and open the backing local file."""
        self.opened.append((path, mode))
        return open(self.local_path, mode)


class TestInterop:
    """Test pandas <-> polars conversion helpers."""

    def test_to_polars(self, df):
        """to_polars returns an equivalent polars DataFrame."""
        pldf = to_polars(df)
        assert isinstance(pldf, pl.DataFrame)
        assert pldf.columns == ["name", "age", "score"]
        assert pldf.height == 3

    def test_roundtrip(self, df):
        """to_polars followed by from_polars is lossless."""
        result = from_polars(to_polars(df))
        pd.testing.assert_frame_equal(result, df)

    def test_from_polars_lazyframe(self, df):
        """from_polars collects LazyFrames before converting."""
        lazy = to_polars(df).lazy()
        result = from_polars(lazy)
        pd.testing.assert_frame_equal(result, df)

    def test_is_polars_available_true(self):
        """is_polars_available is True when polars is installed."""
        assert is_polars_available() is True

    def test_is_polars_available_false(self, monkeypatch):
        """is_polars_available is False when polars import fails."""
        monkeypatch.setitem(sys.modules, "polars", None)
        assert is_polars_available() is False

    def test_to_polars_import_error(self, df, monkeypatch):
        """to_polars raises a helpful ImportError without polars."""
        monkeypatch.setitem(sys.modules, "polars", None)
        with pytest.raises(ImportError, match=r"simpleetl\[polars\]"):
            to_polars(df)

    def test_from_polars_import_error(self, df, monkeypatch):
        """from_polars raises a helpful ImportError without polars."""
        pldf = to_polars(df)
        monkeypatch.setitem(sys.modules, "polars", None)
        with pytest.raises(ImportError, match=r"simpleetl\[polars\]"):
            from_polars(pldf)


class TestValidateEngine:
    """Test the engine name validator."""

    def test_valid_engines(self):
        """Both supported engine names validate unchanged."""
        assert validate_engine("pandas") == "pandas"
        assert validate_engine("polars") == "polars"

    def test_invalid_engine(self):
        """Unknown engine names raise ValueError."""
        with pytest.raises(ValueError, match="Unknown engine 'spark'"):
            validate_engine("spark")


class TestPolarsTransform:
    """Test the polars_transform escape hatch."""

    def test_eager_return(self, df):
        """fn may return an eager polars DataFrame."""
        result = polars_transform(
            df, lambda pldf: pldf.filter(pl.col("age") > 25)
        )
        assert isinstance(result, pd.DataFrame)
        assert list(result["name"]) == ["Bob", "Charlie"]

    def test_lazyframe_return(self, df):
        """fn may return a LazyFrame, which is collected."""
        result = polars_transform(
            df,
            lambda pldf: pldf.lazy().with_columns(
                (pl.col("age") * 2).alias("double_age")
            ),
        )
        assert list(result["double_age"]) == [50, 60, 70]

    def test_bad_return_type(self, df):
        """fn returning anything else raises TypeError."""
        with pytest.raises(
            TypeError, match="DataFrame or LazyFrame, got dict"
        ):
            polars_transform(df, lambda pldf: {"not": "a frame"})

    def test_import_error(self, df, monkeypatch):
        """polars_transform raises ImportError without polars."""
        monkeypatch.setitem(sys.modules, "polars", None)
        with pytest.raises(ImportError, match=r"simpleetl\[polars\]"):
            polars_transform(df, lambda pldf: pldf)


class TestPolarsSqlTransform:
    """Test the polars SQL escape hatch."""

    def test_query(self, df):
        """Query referencing the default 'df' table name."""
        result = polars_sql_transform(
            df, "SELECT name, age FROM df WHERE age >= 30 ORDER BY age"
        )
        assert list(result["name"]) == ["Bob", "Charlie"]
        assert list(result.columns) == ["name", "age"]

    def test_custom_table_name(self, df):
        """The frame can be registered under a custom name."""
        result = polars_sql_transform(
            df,
            "SELECT SUM(age) AS total FROM people",
            table_name="people",
        )
        assert result["total"].iloc[0] == 90

    def test_import_error(self, df, monkeypatch):
        """polars_sql_transform raises ImportError without polars."""
        monkeypatch.setitem(sys.modules, "polars", None)
        with pytest.raises(ImportError, match=r"simpleetl\[polars\]"):
            polars_sql_transform(df, "SELECT * FROM df")


class TestCSVPolarsEngine:
    """Test the polars fast path in the CSV reader and writer."""

    def test_roundtrip_matches_pandas_engine(self, df, tmp_path):
        """polars-engine CSV roundtrip equals the pandas-engine one."""
        pandas_path = str(tmp_path / "pandas.csv")
        polars_path = str(tmp_path / "polars.csv")
        writer = CSVWriter()
        writer.write(df, pandas_path)
        writer.write(df, polars_path, engine="polars")

        reader = CSVReader()
        df_pandas = reader.read(pandas_path)
        df_polars = reader.read(polars_path, engine="polars")
        pd.testing.assert_frame_equal(df_polars, df_pandas)
        pd.testing.assert_frame_equal(df_polars, df)

    def test_reader_usecols(self, df, tmp_path):
        """usecols maps to polars column selection."""
        path = str(tmp_path / "data.csv")
        CSVWriter().write(df, path)
        result = CSVReader().read(
            path, engine="polars", usecols=["name", "score"]
        )
        assert list(result.columns) == ["name", "score"]
        assert list(result["name"]) == ["Alice", "Bob", "Charlie"]

    def test_writer_columns_option(self, df, tmp_path):
        """The to_csv 'columns' option selects columns in polars too."""
        path = str(tmp_path / "data.csv")
        CSVWriter().write(df, path, engine="polars", columns=["name", "age"])
        result = pd.read_csv(path)
        assert list(result.columns) == ["name", "age"]

    def test_writer_sep_option(self, df, tmp_path):
        """The to_csv 'sep' option maps to the polars separator."""
        path = str(tmp_path / "data.csv")
        CSVWriter().write(df, path, engine="polars", sep="|")
        result = pd.read_csv(path, sep="|")
        pd.testing.assert_frame_equal(result, df)

    def test_reader_untranslatable_kwarg_falls_back(
        self, df, tmp_path, caplog
    ):
        """Unmapped kwargs fall back to pandas with a debug log."""
        path = str(tmp_path / "data.csv")
        CSVWriter().write(df, path)
        with caplog.at_level(logging.DEBUG, logger="simpleetl.formats.csv"):
            result = CSVReader().read(
                path, engine="polars", dtype={"age": "int64"}
            )
        assert "no polars equivalent" in caplog.text
        pd.testing.assert_frame_equal(result, df)

    def test_writer_untranslatable_kwarg_falls_back(
        self, df, tmp_path, caplog
    ):
        """Unmapped writer kwargs fall back to pandas with a debug log."""
        path = str(tmp_path / "data.csv")
        with caplog.at_level(logging.DEBUG, logger="simpleetl.formats.csv"):
            CSVWriter().write(
                df, path, engine="polars", lineterminator="\n"
            )
        assert "no polars equivalent" in caplog.text
        pd.testing.assert_frame_equal(pd.read_csv(path), df)

    def test_reader_missing_polars_warns(
        self, df, tmp_path, monkeypatch, caplog
    ):
        """engine='polars' without polars warns and uses pandas."""
        path = str(tmp_path / "data.csv")
        CSVWriter().write(df, path)
        monkeypatch.setitem(sys.modules, "polars", None)
        with caplog.at_level(
            logging.WARNING, logger="simpleetl.formats.csv"
        ):
            result = CSVReader().read(path, engine="polars")
        assert "falling back to pandas" in caplog.text
        pd.testing.assert_frame_equal(result, df)

    def test_writer_missing_polars_warns(
        self, df, tmp_path, monkeypatch, caplog
    ):
        """Writer warns and falls back to pandas without polars."""
        path = str(tmp_path / "data.csv")
        monkeypatch.setitem(sys.modules, "polars", None)
        with caplog.at_level(
            logging.WARNING, logger="simpleetl.formats.csv"
        ):
            CSVWriter().write(df, path, engine="polars")
        assert "falling back to pandas" in caplog.text
        pd.testing.assert_frame_equal(pd.read_csv(path), df)

    def test_unknown_engine_raises(self, df, tmp_path):
        """Unknown engine names raise ValueError on read and write."""
        path = str(tmp_path / "data.csv")
        with pytest.raises(ValueError, match="Unknown engine"):
            CSVReader().read(path, engine="spark")
        with pytest.raises(ValueError, match="Unknown engine"):
            CSVWriter().write(df, path, engine="spark")

    def test_reader_cloud_path_falls_back(self, df, tmp_path, caplog):
        """Cloud paths skip the polars fast path and use fsspec."""
        local = str(tmp_path / "data.csv")
        CSVWriter().write(df, local)
        fs = FakeFilesystem(local)
        with caplog.at_level(logging.DEBUG, logger="simpleetl.formats.csv"):
            result = CSVReader().read(
                "s3://bucket/data.csv", engine="polars", filesystem=fs
            )
        assert "local paths only" in caplog.text
        assert fs.opened == [("s3://bucket/data.csv", "r")]
        pd.testing.assert_frame_equal(result, df)

    def test_writer_cloud_path_falls_back(self, df, tmp_path):
        """Cloud destinations skip the polars fast path."""
        local = str(tmp_path / "out.csv")
        fs = FakeFilesystem(local)
        CSVWriter().write(
            df, "s3://bucket/out.csv", engine="polars", filesystem=fs
        )
        assert fs.opened == [("s3://bucket/out.csv", "w")]
        pd.testing.assert_frame_equal(pd.read_csv(local), df)

    def test_read_chunks_ignores_polars_engine(self, df, tmp_path):
        """Chunked reads accept engine='polars' but stay on pandas."""
        path = str(tmp_path / "data.csv")
        CSVWriter().write(df, path)
        chunks = list(
            CSVReader().read_chunks(path, chunk_size=2, engine="polars")
        )
        assert len(chunks) == 2
        result = pd.concat(chunks, ignore_index=True)
        pd.testing.assert_frame_equal(result, df)

    def test_read_chunks_unknown_engine_raises(self, tmp_path):
        """Chunked reads validate the engine name too."""
        with pytest.raises(ValueError, match="Unknown engine"):
            list(
                CSVReader().read_chunks(
                    str(tmp_path / "x.csv"), engine="dask"
                )
            )

    def test_write_chunks_ignores_polars_engine(self, df, tmp_path):
        """Chunked writes accept engine='polars' but stay on pandas."""
        path = str(tmp_path / "data.csv")
        CSVWriter().write_chunks(
            iter([df.iloc[:2], df.iloc[2:]]), path, engine="polars"
        )
        pd.testing.assert_frame_equal(pd.read_csv(path), df)


class TestParquetPolarsEngine:
    """Test the polars fast path in the Parquet reader and writer."""

    def test_roundtrip_matches_pandas_engine(self, df, tmp_path):
        """polars-engine Parquet roundtrip equals the pandas-engine one."""
        pandas_path = str(tmp_path / "pandas.parquet")
        polars_path = str(tmp_path / "polars.parquet")
        writer = ParquetWriter()
        writer.write(df, pandas_path)
        writer.write(df, polars_path, engine="polars")

        reader = ParquetReader()
        df_pandas = reader.read(pandas_path)
        df_polars = reader.read(polars_path, engine="polars")
        pd.testing.assert_frame_equal(df_polars, df_pandas)
        pd.testing.assert_frame_equal(df_polars, df)

    def test_cross_engine_reads(self, df, tmp_path):
        """Files written by either engine are readable by the other."""
        path = str(tmp_path / "data.parquet")
        ParquetWriter().write(df, path, engine="polars")
        pd.testing.assert_frame_equal(ParquetReader().read(path), df)

    def test_reader_columns_option(self, df, tmp_path):
        """The 'columns' option maps directly to polars."""
        path = str(tmp_path / "data.parquet")
        ParquetWriter().write(df, path)
        result = ParquetReader().read(
            path, engine="polars", columns=["name", "age"]
        )
        assert list(result.columns) == ["name", "age"]

    def test_writer_compression_option(self, df, tmp_path):
        """The 'compression' option maps directly to polars."""
        path = str(tmp_path / "data.parquet")
        ParquetWriter().write(
            df, path, engine="polars", compression="zstd"
        )
        pd.testing.assert_frame_equal(ParquetReader().read(path), df)

    def test_reader_untranslatable_kwarg_falls_back(
        self, df, tmp_path, caplog
    ):
        """Unmapped kwargs fall back to pandas with a debug log."""
        path = str(tmp_path / "data.parquet")
        ParquetWriter().write(df, path)
        with caplog.at_level(
            logging.DEBUG, logger="simpleetl.formats.parquet"
        ):
            result = ParquetReader().read(
                path, engine="polars", filters=None
            )
        assert "no polars equivalent" in caplog.text
        pd.testing.assert_frame_equal(result, df)

    def test_writer_untranslatable_kwarg_falls_back(
        self, df, tmp_path, caplog
    ):
        """Unmapped writer kwargs fall back to pandas with a debug log."""
        path = str(tmp_path / "data.parquet")
        with caplog.at_level(
            logging.DEBUG, logger="simpleetl.formats.parquet"
        ):
            ParquetWriter().write(df, path, engine="polars", index=False)
        assert "no polars equivalent" in caplog.text
        pd.testing.assert_frame_equal(ParquetReader().read(path), df)

    def test_reader_missing_polars_warns(
        self, df, tmp_path, monkeypatch, caplog
    ):
        """engine='polars' without polars warns and uses pandas."""
        path = str(tmp_path / "data.parquet")
        ParquetWriter().write(df, path)
        monkeypatch.setitem(sys.modules, "polars", None)
        with caplog.at_level(
            logging.WARNING, logger="simpleetl.formats.parquet"
        ):
            result = ParquetReader().read(path, engine="polars")
        assert "falling back to pandas" in caplog.text
        pd.testing.assert_frame_equal(result, df)

    def test_writer_missing_polars_warns(
        self, df, tmp_path, monkeypatch, caplog
    ):
        """Writer warns and falls back to pandas without polars."""
        path = str(tmp_path / "data.parquet")
        monkeypatch.setitem(sys.modules, "polars", None)
        with caplog.at_level(
            logging.WARNING, logger="simpleetl.formats.parquet"
        ):
            ParquetWriter().write(df, path, engine="polars")
        assert "falling back to pandas" in caplog.text
        pd.testing.assert_frame_equal(ParquetReader().read(path), df)

    def test_unknown_engine_raises(self, df, tmp_path):
        """Unknown engine names raise ValueError on read and write."""
        path = str(tmp_path / "data.parquet")
        with pytest.raises(ValueError, match="Unknown engine"):
            ParquetReader().read(path, engine="dask")
        with pytest.raises(ValueError, match="Unknown engine"):
            ParquetWriter().write(df, path, engine="dask")

    def test_reader_cloud_path_falls_back(self, df, caplog):
        """Cloud paths skip the polars fast path and use pandas."""
        fake_fs = FakeFilesystem("unused")
        with mock.patch.object(
            parquet_module.pd, "read_parquet", return_value=df
        ) as read_parquet:
            with caplog.at_level(
                logging.DEBUG, logger="simpleetl.formats.parquet"
            ):
                result = ParquetReader().read(
                    "s3://bucket/data.parquet",
                    engine="polars",
                    filesystem=fake_fs,
                )
        assert "local paths only" in caplog.text
        read_parquet.assert_called_once()
        assert read_parquet.call_args.kwargs["filesystem"] is fake_fs
        pd.testing.assert_frame_equal(result, df)

    def test_writer_cloud_path_falls_back(self, df):
        """Cloud destinations skip the polars fast path."""
        fake_fs = FakeFilesystem("unused")
        with mock.patch.object(
            parquet_module.pd.DataFrame, "to_parquet"
        ) as to_parquet:
            ParquetWriter().write(
                df,
                "s3://bucket/out.parquet",
                engine="polars",
                filesystem=fake_fs,
            )
        to_parquet.assert_called_once()
        assert to_parquet.call_args.kwargs["filesystem"] is fake_fs

    def test_read_chunks_ignores_polars_engine(self, df, tmp_path):
        """Chunked reads accept engine='polars' but stay on pyarrow."""
        path = str(tmp_path / "data.parquet")
        ParquetWriter().write(df, path)
        chunks = list(
            ParquetReader().read_chunks(path, chunk_size=2, engine="polars")
        )
        result = pd.concat(chunks, ignore_index=True)
        pd.testing.assert_frame_equal(result, df)

    def test_write_chunks_ignores_polars_engine(self, df, tmp_path):
        """Chunked writes accept engine='polars' but stay on pyarrow."""
        path = str(tmp_path / "data.parquet")
        ParquetWriter().write_chunks(
            iter([df.iloc[:2], df.iloc[2:]]), path, engine="polars"
        )
        result = ParquetReader().read(path)
        pd.testing.assert_frame_equal(
            result.reset_index(drop=True), df
        )
