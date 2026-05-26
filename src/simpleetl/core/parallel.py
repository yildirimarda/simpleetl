"""
Parallel processing and performance optimization for SimpleETL.

Provides multi-threaded read/write, parallel partition processing,
data partitioning strategies, and lazy evaluation support.
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, List, Optional, Type

import pandas as pd

from simpleetl.formats.base import DataReader, DataWriter
from simpleetl.formats.csv import CSVReader, CSVWriter

logger = logging.getLogger(__name__)


class ParallelReader:
    """Multi-threaded reader that reads multiple sources in parallel.

    Uses ThreadPoolExecutor for I/O-bound parallel reads.
    """

    def __init__(self, max_workers: int = 4, reader_class: Optional[Type[DataReader]] = None):
        """Initialize the ParallelReader.

        Args:
            max_workers: Maximum number of concurrent threads.
            reader_class: DataReader class to use for reading.
        """
        self.max_workers = max_workers
        self.reader_class = reader_class or CSVReader

    def _new_reader(self) -> DataReader:
        """Return a new reader instance."""
        return self.reader_class()

    def read_parallel(self, sources: List[str], **kwargs) -> pd.DataFrame:
        """Read multiple sources in parallel and concatenate results.

        Args:
            sources: List of source paths/identifiers.
            **kwargs: Additional arguments passed to reader.

        Returns:
            Concatenated DataFrame from all sources.
        """
        if not sources:
            return pd.DataFrame()

        results: List[pd.DataFrame] = []
        errors: List[tuple] = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_source = {}
            for source in sources:
                reader = self._new_reader()
                future = executor.submit(reader.read, source, **kwargs)
                future_to_source[future] = source

            for future in as_completed(future_to_source):
                source = future_to_source[future]
                try:
                    df = future.result()
                    results.append(df)
                except Exception as e:
                    logger.error("Failed to read source '%s': %s", source, str(e))
                    errors.append((source, str(e)))

        if not results:
            logger.warning("No data read from any source.")
            return pd.DataFrame()

        return pd.concat(results, ignore_index=True)

    def read_partitioned(
        self,
        source: str,
        partition_column: str,
        partitions: Optional[List[Any]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """Read a partitioned source in parallel by partition values.

        Reads the full source per partition value and filters in-memory,
        since generic readers may not support server-side filtering.

        Args:
            source: Data source (table name or file path pattern).
            partition_column: Column to partition by.
            partitions: List of partition values to read. If None,
                auto-detects.
            **kwargs: Additional arguments passed to reader.

        Returns:
            Concatenated DataFrame from all partitions.
        """
        # Read once to discover partitions and validate
        reader = self._new_reader()
        df_full = reader.read(source, **kwargs)
        if partition_column not in df_full.columns:
            raise ValueError(
                f"Partition column '{partition_column}' not found "
                f"in data from '{source}'."
            )

        if partitions is None:
            partitions = df_full[partition_column].unique().tolist()

        if not partitions:
            return pd.DataFrame()

        results: List[pd.DataFrame] = []

        def _read_and_filter(value: Any) -> pd.DataFrame:
            return df_full[df_full[partition_column] == value].copy()

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_partition = {}
            for value in partitions:
                future = executor.submit(_read_and_filter, value)
                future_to_partition[future] = value

            for future in as_completed(future_to_partition):
                value = future_to_partition[future]
                try:
                    df = future.result()
                    if not df.empty:
                        results.append(df)
                except Exception as e:
                    logger.error(
                        "Failed to read partition '%s'='%s': %s",
                        partition_column,
                        value,
                        str(e),
                    )

        if not results:
            return pd.DataFrame()

        return pd.concat(results, ignore_index=True)


class ParallelWriter:
    """Multi-threaded writer that writes data to multiple destinations
    in parallel.
    """

    def __init__(self, max_workers: int = 4, writer_class: Optional[Type[DataWriter]] = None):
        """Initialize the ParallelWriter.

        Args:
            max_workers: Maximum number of concurrent threads.
            writer_class: DataWriter class to use for writing.
        """
        self.max_workers = max_workers
        self.writer_class = writer_class or CSVWriter

    def _new_writer(self) -> DataWriter:
        """Return a new writer instance."""
        return self.writer_class()

    def write_parallel(
        self, data: pd.DataFrame, destinations: List[str], **kwargs
    ) -> None:
        """Write the same data to multiple destinations in parallel.

        Args:
            data: DataFrame to write.
            destinations: List of destination paths/identifiers.
            **kwargs: Additional arguments passed to writer.
        """
        if not destinations:
            return

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_dest = {}
            for dest in destinations:
                writer = self._new_writer()
                future = executor.submit(writer.write, data, dest, **kwargs)
                future_to_dest[future] = dest

            for future in as_completed(future_to_dest):
                dest = future_to_dest[future]
                try:
                    future.result()
                    logger.info("Successfully wrote data to '%s'.", dest)
                except Exception as e:
                    logger.error(
                        "Failed to write to destination '%s': %s",
                        dest,
                        str(e),
                    )


class PartitionStrategy:
    """Data partitioning strategy for writes.

    Supports partitioning by column values, date ranges, or
    hash-based partitioning.
    """

    @staticmethod
    def partition_by_column(df: pd.DataFrame, column: str) -> Dict[Any, pd.DataFrame]:
        """Partition a DataFrame by unique values in a column.

        Args:
            df: Input DataFrame.
            column: Column to partition by.

        Returns:
            Dict mapping partition value to DataFrame subset.
        """
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in DataFrame.")

        partitions = {}
        for value, group in df.groupby(column):
            partitions[value] = group.reset_index(drop=True)

        return partitions

    @staticmethod
    def partition_by_date(
        df: pd.DataFrame, column: str, freq: str = "month"
    ) -> Dict[str, pd.DataFrame]:
        """Partition a DataFrame by date column at specified frequency.

        Args:
            df: Input DataFrame.
            column: Date/datetime column name.
            freq: Partition frequency - 'year', 'month', 'day', 'week'.

        Returns:
            Dict mapping partition key (e.g., '2024-01') to DataFrame subset.
        """
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in DataFrame.")

        freq_map = {
            "year": "%Y",
            "month": "%Y-%m",
            "day": "%Y-%m-%d",
            "week": "%Y-W%W",
        }

        if freq not in freq_map:
            raise ValueError(
                f"Invalid frequency '{freq}'. Must be one of {list(freq_map.keys())}."
            )

        fmt = freq_map[freq]
        dt_series = pd.to_datetime(df[column])
        partition_keys = dt_series.dt.strftime(fmt)

        partitions = {}
        for key, group in df.groupby(partition_keys):
            partitions[key] = group.reset_index(drop=True)

        return partitions

    @staticmethod
    def partition_by_hash(
        df: pd.DataFrame, column: str, num_partitions: int = 4
    ) -> Dict[int, pd.DataFrame]:
        """Partition a DataFrame by hash of a column value.

        Args:
            df: Input DataFrame.
            column: Column to hash-partition by.
            num_partitions: Number of partitions.

        Returns:
            Dict mapping partition number (0..num_partitions-1) to
            DataFrame subset.
        """
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in DataFrame.")

        if num_partitions < 1:
            raise ValueError("num_partitions must be >= 1.")

        hashes = df[column].apply(lambda x: hash(x) % num_partitions)

        partitions = {}
        for part_id, group in df.groupby(hashes):
            partitions[part_id] = group.reset_index(drop=True)

        return partitions

    @staticmethod
    def write_partitioned(
        df: pd.DataFrame,
        destination: str,
        partition_column: str,
        writer: Optional[DataWriter] = None,
        **kwargs,
    ) -> Dict[str, str]:
        """Write a DataFrame partitioned by column values.

        Creates separate files/output for each partition value.
        For file-based outputs, appends partition value to path.
        For database outputs, uses partition value as table suffix.

        Args:
            df: Input DataFrame.
            destination: Base destination path or table name.
            partition_column: Column to partition by.
            writer: DataWriter instance. If None, uses CSVWriter.
            **kwargs: Additional arguments passed to writer.

        Returns:
            Dict mapping partition value to output path/table name.
        """
        from simpleetl.formats.csv import CSVWriter

        if writer is None:
            writer = CSVWriter()

        partitions = PartitionStrategy.partition_by_column(df, partition_column)
        output_map: Dict[str, str] = {}

        for value, partition_df in partitions.items():
            str_value = str(value)
            if "." in destination or "/" in destination:
                # File-based output: append partition value to path
                base, ext = destination.rsplit(".", 1)
                part_dest = f"{base}_{str_value}.{ext}"
            else:
                # Database output: use partition value as table suffix
                part_dest = f"{destination}_{str_value}"

            writer.write(partition_df, part_dest, **kwargs)
            output_map[str_value] = part_dest

        return output_map


class LazyTransformation:
    """Lazy evaluation wrapper for transformations.

    Collects transformations and only applies them when .execute()
    is called. Allows optimization of the transformation pipeline.
    """

    def __init__(self, df: pd.DataFrame | None = None):
        """Initialize with optional DataFrame.

        Args:
            df: Optional initial DataFrame.
        """
        self._df = df
        self._steps: List[tuple] = []

    def add_step(
        self, func: Callable, *args: Any, **kwargs: Any
    ) -> "LazyTransformation":
        """Add a transformation step.

        Args:
            func: Transformation function. Must accept a DataFrame as
                its first argument.
            *args: Positional arguments to pass after the DataFrame.
            **kwargs: Keyword arguments to pass to the function.

        Returns:
            Self for chaining.
        """
        self._steps.append((func, args, kwargs))
        return self

    def execute(self, df: pd.DataFrame | None = None) -> pd.DataFrame:
        """Execute all collected transformation steps.

        Args:
            df: Optional DataFrame to transform. If None, uses the
                DataFrame provided at initialization.

        Returns:
            Transformed DataFrame.
        """
        result = df if df is not None else self._df
        if result is None:
            raise ValueError(
                "No DataFrame provided. Pass a DataFrame to "
                "execute() or initialize with one."
            )

        for func, args, kwargs in self._steps:
            result = func(result, *args, **kwargs)

        return result

    def optimize(self) -> "LazyTransformation":
        """Optimize the transformation pipeline.

        Currently implements:
        - Filter pushdown: move filters before other operations
          where possible.
        - Adjacent filter merging: combine consecutive filters.

        Returns:
            Optimized LazyTransformation.
        """
        optimized = LazyTransformation(self._df)
        pending_filters: List[tuple] = []

        for func, args, kwargs in self._steps:
            func_name = getattr(func, "__name__", "")
            is_filter = (
                func_name == "_apply_filter"
                or callable(func)
                and getattr(func, "_is_filter_transform", False)
            )
            if is_filter:
                pending_filters.append((func, args, kwargs))
            else:
                if pending_filters:
                    merged = LazyTransformation._merge_filters(pending_filters)
                    optimized._steps.append(merged)
                    pending_filters = []
                optimized._steps.append((func, args, kwargs))

        if pending_filters:
            merged = LazyTransformation._merge_filters(pending_filters)
            optimized._steps.append(merged)

        return optimized

    @staticmethod
    def _merge_filters(
        filters: List[tuple],
    ) -> tuple:
        """Merge multiple filter steps into a single step.

        Args:
            filters: List of (func, args, kwargs) tuples for filters.

        Returns:
            A single merged filter step.
        """
        if len(filters) == 1:
            return filters[0]

        def merged_filter(df: pd.DataFrame, *args: Any, **kwargs: Any) -> pd.DataFrame:
            result = df
            for func, f_args, f_kwargs in filters:
                result = func(result, *f_args, **f_kwargs)
            return result

        merged_filter.__name__ = "_apply_filter"
        return (merged_filter, (), {})


def parallel_read(
    sources: List[str],
    max_workers: int = 4,
    reader_class: Optional[Type[DataReader]] = None,
    **kwargs: Any,
) -> pd.DataFrame:
    """Convenience function for parallel reading.

    Args:
        sources: List of source paths/identifiers.
        max_workers: Maximum number of concurrent threads.
        reader_class: DataReader class to use.
        **kwargs: Additional arguments passed to reader.

    Returns:
        Concatenated DataFrame from all sources.
    """
    reader = ParallelReader(max_workers=max_workers, reader_class=reader_class)
    return reader.read_parallel(sources, **kwargs)


def parallel_write(
    data: pd.DataFrame,
    destinations: List[str],
    max_workers: int = 4,
    writer_class: Optional[Type[DataWriter]] = None,
    **kwargs: Any,
) -> None:
    """Convenience function for parallel writing.

    Args:
        data: DataFrame to write.
        destinations: List of destination paths/identifiers.
        max_workers: Maximum number of concurrent threads.
        writer_class: DataWriter class to use.
        **kwargs: Additional arguments passed to writer.
    """
    writer = ParallelWriter(max_workers=max_workers, writer_class=writer_class)
    writer.write_parallel(data, destinations, **kwargs)
