"""
CSV format reader and writer using pandas.
"""

import pandas as pd
from typing import Iterator
from .base import DataReader, DataWriter
from ..core.filesystem import is_cloud_path, get_filesystem


class CSVReader(DataReader):
    """Read data from CSV files, including cloud storage paths."""

    def read(self, source: str, **kwargs) -> pd.DataFrame:
        """
        Read data from a CSV file.

        Supports local paths and cloud storage (S3, GCS, ABFS) via fsspec.

        Args:
            source: Path to the CSV file.
            **kwargs: Additional arguments to pass to pandas.read_csv.
                Supports 'filesystem' for an fsspec filesystem instance.

        Returns:
            pandas DataFrame containing the data.
        """
        if is_cloud_path(source):
            filesystem = kwargs.pop('filesystem', None)
            if filesystem is None:
                filesystem = get_filesystem(source)
            with filesystem.open(source, 'r') as f:
                return pd.read_csv(f, **kwargs)
        return pd.read_csv(source, **kwargs)

    def read_chunks(
        self, source: str, chunk_size: int = 10000, **kwargs
    ) -> Iterator[pd.DataFrame]:
        """
        Read CSV data in chunks.

        Args:
            source: Path to the CSV file.
            chunk_size: Number of rows per chunk.
            **kwargs: Additional arguments to pass to pandas.read_csv.

        Yields:
            pandas DataFrame chunks.
        """
        if is_cloud_path(source):
            filesystem = kwargs.pop('filesystem', None)
            if filesystem is None:
                filesystem = get_filesystem(source)
            with filesystem.open(source, 'r') as f:
                for chunk in pd.read_csv(
                    f, chunksize=chunk_size, **kwargs
                ):
                    yield chunk
        else:
            for chunk in pd.read_csv(
                source, chunksize=chunk_size, **kwargs
            ):
                yield chunk


class CSVWriter(DataWriter):
    """Write data to CSV files, including cloud storage paths."""

    def write(self, data: pd.DataFrame, destination: str, **kwargs) -> None:
        """
        Write data to a CSV file.

        Supports local paths and cloud storage (S3, GCS, ABFS) via fsspec.

        Args:
            data: pandas DataFrame to write.
            destination: Path to the output CSV file.
            **kwargs: Additional arguments to pass to pandas.DataFrame.to_csv.
                Supports 'filesystem' for an fsspec filesystem instance.
        """
        if is_cloud_path(destination):
            filesystem = kwargs.pop('filesystem', None)
            if filesystem is None:
                filesystem = get_filesystem(destination)
            with filesystem.open(destination, 'w') as f:
                data.to_csv(f, index=False, **kwargs)
        else:
            data.to_csv(destination, index=False, **kwargs)

    def write_chunks(
        self, data_iterator: Iterator[pd.DataFrame], destination: str,
        **kwargs
    ) -> None:
        """
        Write CSV data in chunks. Appends after the first chunk.

        Args:
            data_iterator: Iterator yielding pandas DataFrames.
            destination: Path to the output CSV file.
            **kwargs: Additional arguments.
        """
        first = True
        if is_cloud_path(destination):
            filesystem = kwargs.pop('filesystem', None)
            if filesystem is None:
                filesystem = get_filesystem(destination)
            with filesystem.open(destination, 'w') as f:
                for chunk in data_iterator:
                    chunk.to_csv(
                        f, index=False, header=first, mode='a', **kwargs
                    )
                    first = False
        else:
            for chunk in data_iterator:
                chunk.to_csv(
                    destination, index=False, header=first, mode='a', **kwargs
                )
                first = False