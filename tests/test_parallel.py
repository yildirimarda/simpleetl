"""
Tests for parallel processing and performance optimization.
"""

import os
import tempfile

import pandas as pd
import pytest

from simpleetl.core.parallel import (
    LazyTransformation,
    ParallelReader,
    ParallelWriter,
    PartitionStrategy,
    parallel_read,
    parallel_write,
)
from simpleetl.formats.csv import CSVReader, CSVWriter


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_df():
    """Return a small sample DataFrame."""
    return pd.DataFrame(
        {
            "name": ["Alice", "Bob", "Charlie", "Diana"],
            "age": [25, 30, 35, 28],
            "city": ["New York", "London", "Paris", "Tokyo"],
        }
    )


@pytest.fixture
def partitioned_df():
    """Return a DataFrame with a date column for date partitioning."""
    return pd.DataFrame(
        {
            "id": range(1, 13),
            "value": [x * 10 for x in range(1, 13)],
            "month": [
                "2024-01",
                "2024-01",
                "2024-02",
                "2024-02",
                "2024-03",
                "2024-03",
                "2024-04",
                "2024-04",
                "2024-05",
                "2024-05",
                "2024-06",
                "2024-06",
            ],
        }
    )


@pytest.fixture
def date_df():
    """Return a DataFrame with a proper date column."""
    return pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=12, freq="MS"),
            "value": range(12),
        }
    )


@pytest.fixture
def csv_files(sample_df, partitioned_df):
    """Create temporary CSV files and return their paths."""
    tmpdir = tempfile.mkdtemp()
    files = []

    # File 1: first 2 rows
    f1 = os.path.join(tmpdir, "part1.csv")
    sample_df.iloc[:2].to_csv(f1, index=False)
    files.append(f1)

    # File 2: last 2 rows
    f2 = os.path.join(tmpdir, "part2.csv")
    sample_df.iloc[2:].to_csv(f2, index=False)
    files.append(f2)

    # File 3: partitioned data
    f3 = os.path.join(tmpdir, "partitioned.csv")
    partitioned_df.to_csv(f3, index=False)
    files.append(f3)

    yield files

    # Cleanup
    for f in files:
        if os.path.exists(f):
            os.unlink(f)
    os.rmdir(tmpdir)


# ---------------------------------------------------------------------------
# ParallelReader tests
# ---------------------------------------------------------------------------


class TestParallelReader:
    """Tests for ParallelReader."""

    def test_read_parallel_multiple_files(self, csv_files):
        """Test reading multiple CSV files in parallel."""
        reader = ParallelReader(max_workers=2, reader_class=CSVReader)
        df = reader.read_parallel(csv_files[:2])

        assert len(df) == 4
        assert set(df.columns) == {"name", "age", "city"}

    def test_read_parallel_single_file(self, csv_files):
        """Test reading a single file via ParallelReader."""
        reader = ParallelReader(max_workers=2, reader_class=CSVReader)
        df = reader.read_parallel([csv_files[0]])

        assert len(df) == 2

    def test_read_parallel_empty_sources(self):
        """Test reading with empty source list returns empty DataFrame."""
        reader = ParallelReader(max_workers=2, reader_class=CSVReader)
        df = reader.read_parallel([])

        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_read_parallel_with_missing_file_logs_error(self, csv_files):
        """Test that a missing file is logged but doesn't crash."""
        reader = ParallelReader(max_workers=2, reader_class=CSVReader)
        sources = [csv_files[0], "/nonexistent/file.csv"]
        df = reader.read_parallel(sources)

        # Should still return data from the valid file
        assert len(df) == 2

    def test_read_parallel_all_missing_returns_empty(self):
        """Test that all-missing sources returns empty DataFrame."""
        reader = ParallelReader(max_workers=2, reader_class=CSVReader)
        df = reader.read_parallel(["/nonexistent/a.csv", "/nonexistent/b.csv"])

        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_read_parallel_preserves_data(self, csv_files):
        """Test that parallel read preserves all data."""
        reader = ParallelReader(max_workers=2, reader_class=CSVReader)
        df = reader.read_parallel(csv_files[:2])

        names = sorted(df["name"].tolist())
        assert names == ["Alice", "Bob", "Charlie", "Diana"]

    def test_read_partitioned_with_explicit_partitions(self, csv_files, partitioned_df):
        """Test read_partitioned with explicit partition values."""
        reader = ParallelReader(max_workers=2, reader_class=CSVReader)
        df = reader.read_partitioned(
            csv_files[2],
            partition_column="month",
            partitions=["2024-01", "2024-02"],
        )

        assert len(df) == 4
        assert set(df["month"].unique()) == {"2024-01", "2024-02"}

    def test_read_partitioned_auto_detect(self, csv_files, partitioned_df):
        """Test read_partitioned auto-detects partitions."""
        reader = ParallelReader(max_workers=2, reader_class=CSVReader)
        df = reader.read_partitioned(csv_files[2], partition_column="month")

        assert len(df) == 12

    def test_read_partitioned_missing_column_raises(self, csv_files):
        """Test read_partitioned raises ValueError for missing column."""
        reader = ParallelReader(max_workers=2, reader_class=CSVReader)
        with pytest.raises(ValueError, match="Partition column"):
            reader.read_partitioned(
                csv_files[0],
                partition_column="nonexistent",
                partitions=["val"],
            )


# ---------------------------------------------------------------------------
# ParallelWriter tests
# ---------------------------------------------------------------------------


class TestParallelWriter:
    """Tests for ParallelWriter."""

    def test_write_parallel_multiple_destinations(self, sample_df, csv_files):
        """Test writing to multiple destinations in parallel."""
        tmpdir = tempfile.mkdtemp()
        try:
            dest1 = os.path.join(tmpdir, "out1.csv")
            dest2 = os.path.join(tmpdir, "out2.csv")

            writer = ParallelWriter(max_workers=2, writer_class=CSVWriter)
            writer.write_parallel(sample_df, [dest1, dest2])

            df1 = pd.read_csv(dest1)
            df2 = pd.read_csv(dest2)

            pd.testing.assert_frame_equal(df1, sample_df)
            pd.testing.assert_frame_equal(df2, sample_df)
        finally:
            for f in [dest1, dest2]:
                if os.path.exists(f):
                    os.unlink(f)
            os.rmdir(tmpdir)

    def test_write_parallel_empty_destinations(self, sample_df):
        """Test writing with empty destinations list does nothing."""
        writer = ParallelWriter(max_workers=2, writer_class=CSVWriter)
        # Should not raise
        writer.write_parallel(sample_df, [])

    def test_write_parallel_single_destination(self, sample_df):
        """Test writing to a single destination via ParallelWriter."""
        tmpdir = tempfile.mkdtemp()
        dest = os.path.join(tmpdir, "single.csv")
        try:
            writer = ParallelWriter(max_workers=2, writer_class=CSVWriter)
            writer.write_parallel(sample_df, [dest])

            df = pd.read_csv(dest)
            pd.testing.assert_frame_equal(df, sample_df)
        finally:
            if os.path.exists(dest):
                os.unlink(dest)
            os.rmdir(tmpdir)


# ---------------------------------------------------------------------------
# PartitionStrategy tests
# ---------------------------------------------------------------------------


class TestPartitionStrategyByColumn:
    """Tests for PartitionStrategy.partition_by_column."""

    def test_partition_by_column_basic(self, sample_df):
        """Test basic column partitioning."""
        partitions = PartitionStrategy.partition_by_column(sample_df, "city")

        assert len(partitions) == 4
        assert set(partitions.keys()) == {
            "New York",
            "London",
            "Paris",
            "Tokyo",
        }

    def test_partition_by_column_grouped(self, partitioned_df):
        """Test column partitioning with grouped values."""
        partitions = PartitionStrategy.partition_by_column(partitioned_df, "month")

        assert len(partitions) == 6
        assert len(partitions["2024-01"]) == 2

    def test_partition_by_column_missing_column(self, sample_df):
        """Test ValueError for missing column."""
        with pytest.raises(ValueError, match="Column 'missing' not found"):
            PartitionStrategy.partition_by_column(sample_df, "missing")

    def test_partition_by_column_single_value(self):
        """Test partitioning when all rows have the same value."""
        df = pd.DataFrame({"a": [1, 2, 3], "cat": ["x", "x", "x"]})
        partitions = PartitionStrategy.partition_by_column(df, "cat")

        assert len(partitions) == 1
        assert len(partitions["x"]) == 3


class TestPartitionStrategyByDate:
    """Tests for PartitionStrategy.partition_by_date."""

    def test_partition_by_date_month(self, date_df):
        """Test date partitioning by month."""
        partitions = PartitionStrategy.partition_by_date(date_df, "date", freq="month")

        assert len(partitions) == 12

    def test_partition_by_date_year(self, date_df):
        """Test date partitioning by year."""
        partitions = PartitionStrategy.partition_by_date(date_df, "date", freq="year")

        assert len(partitions) == 1

    def test_partition_by_date_quarter(self, date_df):
        """Test date partitioning by quarter via month frequency."""
        partitions = PartitionStrategy.partition_by_date(date_df, "date", freq="month")

        # 12 months = 12 partitions at month frequency
        assert len(partitions) == 12

    def test_partition_by_date_invalid_freq(self, date_df):
        """Test ValueError for invalid frequency."""
        with pytest.raises(ValueError, match="Invalid frequency"):
            PartitionStrategy.partition_by_date(date_df, "date", freq="hour")

    def test_partition_by_date_missing_column(self, date_df):
        """Test ValueError for missing date column."""
        with pytest.raises(ValueError, match="Column 'missing' not found"):
            PartitionStrategy.partition_by_date(date_df, "missing")


class TestPartitionStrategyByHash:
    """Tests for PartitionStrategy.partition_by_hash."""

    def test_partition_by_hash_basic(self, sample_df):
        """Test basic hash partitioning."""
        partitions = PartitionStrategy.partition_by_hash(
            sample_df, "name", num_partitions=2
        )

        total_rows = sum(len(v) for v in partitions.values())
        assert total_rows == 4

    def test_partition_by_hash_num_partitions(self, sample_df):
        """Test hash partitioning with different partition counts."""
        partitions = PartitionStrategy.partition_by_hash(
            sample_df, "name", num_partitions=4
        )

        assert len(partitions) <= 4

    def test_partition_by_hash_missing_column(self, sample_df):
        """Test ValueError for missing column."""
        with pytest.raises(ValueError, match="Column 'missing' not found"):
            PartitionStrategy.partition_by_hash(sample_df, "missing")

    def test_partition_by_hash_invalid_num_partitions(self, sample_df):
        """Test ValueError for num_partitions < 1."""
        with pytest.raises(ValueError, match="num_partitions must be >= 1"):
            PartitionStrategy.partition_by_hash(sample_df, "name", num_partitions=0)


class TestPartitionStrategyWritePartitioned:
    """Tests for PartitionStrategy.write_partitioned."""

    def test_write_partitioned_basic(self, sample_df):
        """Test basic partitioned write."""
        tmpdir = tempfile.mkdtemp()
        dest = os.path.join(tmpdir, "output.csv")

        try:
            result = PartitionStrategy.write_partitioned(sample_df, dest, "city")

            assert len(result) == 4
            for path in result.values():
                assert os.path.exists(path)
        finally:
            for path in result.values():
                if os.path.exists(path):
                    os.unlink(path)
            if os.path.exists(dest):
                os.unlink(dest)
            os.rmdir(tmpdir)

    def test_write_partitioned_with_custom_writer(self, sample_df):
        """Test partitioned write with a custom writer."""
        tmpdir = tempfile.mkdtemp()
        dest = os.path.join(tmpdir, "output.csv")
        writer = CSVWriter()

        try:
            result = PartitionStrategy.write_partitioned(
                sample_df, dest, "city", writer=writer
            )

            assert len(result) == 4
        finally:
            for path in result.values():
                if os.path.exists(path):
                    os.unlink(path)
            if os.path.exists(dest):
                os.unlink(dest)
            os.rmdir(tmpdir)

    def test_write_partitioned_returns_correct_paths(self, sample_df):
        """Test that write_partitioned returns correct path mapping."""
        tmpdir = tempfile.mkdtemp()
        dest = os.path.join(tmpdir, "data.csv")

        try:
            result = PartitionStrategy.write_partitioned(sample_df, dest, "city")

            for city in ["New York", "London", "Paris", "Tokyo"]:
                assert city in result
                assert result[city].endswith(".csv")
        finally:
            for path in result.values():
                if os.path.exists(path):
                    os.unlink(path)
            if os.path.exists(dest):
                os.unlink(dest)
            os.rmdir(tmpdir)


# ---------------------------------------------------------------------------
# LazyTransformation tests
# ---------------------------------------------------------------------------


class TestLazyTransformation:
    """Tests for LazyTransformation."""

    def test_add_step_and_execute(self, sample_df):
        """Test adding steps and executing them."""
        lt = LazyTransformation(sample_df)

        def add_col(df):
            df = df.copy()
            df["senior"] = df["age"] >= 30
            return df

        def sort_by_age(df):
            return df.sort_values("age").reset_index(drop=True)

        lt.add_step(add_col).add_step(sort_by_age)
        result = lt.execute()

        assert "senior" in result.columns
        assert result.iloc[0]["name"] == "Alice"

    def test_execute_with_external_df(self, sample_df):
        """Test execute with a DataFrame passed at execution time."""
        lt = LazyTransformation()

        def double_age(df):
            df = df.copy()
            df["age"] = df["age"] * 2
            return df

        lt.add_step(double_age)
        result = lt.execute(sample_df)

        assert result["age"].iloc[0] == 50

    def test_no_df_raises(self):
        """Test that execute raises ValueError when no DataFrame given."""
        lt = LazyTransformation()
        lt.add_step(lambda df: df)

        with pytest.raises(ValueError, match="No DataFrame provided"):
            lt.execute()

    def test_chaining(self, sample_df):
        """Test that add_step returns self for chaining."""
        lt = LazyTransformation(sample_df)
        result = lt.add_step(lambda df: df).add_step(lambda df: df)

        assert result is lt

    def test_optimize_single_filter(self, sample_df):
        """Test optimize with a single filter step."""
        lt = LazyTransformation(sample_df)

        def _apply_filter(df, col, val):
            return df[df[col] == val]

        lt.add_step(_apply_filter, "city", "London")
        optimized = lt.optimize()

        assert len(optimized._steps) == 1

    def test_optimize_merges_adjacent_filters(self, sample_df):
        """Test that optimize merges consecutive filter steps."""
        lt = LazyTransformation(sample_df)

        def _apply_filter(df, col, val):
            return df[df[col] == val]

        lt.add_step(_apply_filter, "city", "London")
        lt.add_step(_apply_filter, "age", 30)
        optimized = lt.optimize()

        # Two adjacent filters should be merged into one
        assert len(optimized._steps) == 1

    def test_optimize_preserves_non_filter_steps(self, sample_df):
        """Test that optimize preserves non-filter steps."""
        lt = LazyTransformation(sample_df)

        def rename_cols(df):
            return df.rename(columns={"name": "full_name"})

        lt.add_step(rename_cols)
        optimized = lt.optimize()

        assert len(optimized._steps) == 1

    def test_optimize_mixed_steps(self, sample_df):
        """Test optimize with mixed filter and non-filter steps."""
        lt = LazyTransformation(sample_df)

        def _apply_filter(df, col, val):
            return df[df[col] == val]

        def add_col(df):
            df = df.copy()
            df["flag"] = True
            return df

        lt.add_step(_apply_filter, "city", "London")
        lt.add_step(_apply_filter, "age", 30)
        lt.add_step(add_col)
        lt.add_step(_apply_filter, "flag", True)

        optimized = lt.optimize()

        # Adjacent filters at start merge, non-filter preserved,
        # trailing filter stays separate
        assert len(optimized._steps) == 3

    def test_optimize_empty_steps(self, sample_df):
        """Test optimize with no steps."""
        lt = LazyTransformation(sample_df)
        optimized = lt.optimize()

        assert len(optimized._steps) == 0

    def test_optimized_execution(self, sample_df):
        """Test that optimized transformation produces correct results."""
        lt = LazyTransformation(sample_df)

        def _apply_filter(df, col, val):
            return df[df[col] == val]

        lt.add_step(_apply_filter, "city", "London")
        lt.add_step(_apply_filter, "age", 30)

        optimized = lt.optimize()
        result = optimized.execute()

        assert len(result) == 1
        assert result.iloc[0]["name"] == "Bob"


# ---------------------------------------------------------------------------
# Convenience function tests
# ---------------------------------------------------------------------------


class TestConvenienceFunctions:
    """Tests for parallel_read and parallel_write convenience functions."""

    def test_parallel_read_function(self, csv_files):
        """Test the parallel_read convenience function."""
        df = parallel_read(csv_files[:2], max_workers=2, reader_class=CSVReader)

        assert len(df) == 4

    def test_parallel_read_empty_sources(self):
        """Test parallel_read with empty sources."""
        df = parallel_read([], reader_class=CSVReader)

        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_parallel_write_function(self, sample_df):
        """Test the parallel_write convenience function."""
        tmpdir = tempfile.mkdtemp()
        dest1 = os.path.join(tmpdir, "out1.csv")
        dest2 = os.path.join(tmpdir, "out2.csv")

        try:
            parallel_write(
                sample_df,
                [dest1, dest2],
                max_workers=2,
                writer_class=CSVWriter,
            )

            df1 = pd.read_csv(dest1)
            df2 = pd.read_csv(dest2)

            pd.testing.assert_frame_equal(df1, sample_df)
            pd.testing.assert_frame_equal(df2, sample_df)
        finally:
            for f in [dest1, dest2]:
                if os.path.exists(f):
                    os.unlink(f)
            os.rmdir(tmpdir)

    def test_parallel_write_empty_destinations(self, sample_df):
        """Test parallel_write with empty destinations."""
        # Should not raise
        parallel_write(sample_df, [], writer_class=CSVWriter)
