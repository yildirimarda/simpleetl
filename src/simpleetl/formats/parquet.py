"""
Parquet format reader and writer using PyArrow.
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Iterator
from .base import DataReader, DataWriter
from ..core.filesystem import is_cloud_path, get_filesystem


class ParquetReader(DataReader):
    """Read data from Parquet files, including cloud storage paths."""

    def read(self, source: str, **kwargs) -> pd.DataFrame:
        """
        Read data from a Parquet file.

        Supports local paths and cloud storage (S3, GCS, ABFS) via PyArrow's
        native filesystem support.

        Args:
            source: Path to the Parquet file.
            **kwargs: Additional arguments to pass to pandas.read_parquet.
                Supports 'filesystem' for an fsspec filesystem instance.

        Returns:
            pandas DataFrame containing the data.
        """
        if 'engine' not in kwargs:
            kwargs['engine'] = 'pyarrow'

        if is_cloud_path(source):
            filesystem = kwargs.pop('filesystem', None)
            if filesystem is None:
                filesystem = get_filesystem(source)
            return pd.read_parquet(source, filesystem=filesystem, **kwargs)

        return pd.read_parquet(source, **kwargs)

    def read_chunks(
        self, source: str, chunk_size: int = 10000, **kwargs
    ) -> Iterator[pd.DataFrame]:
        """
        Read Parquet data in row-group chunks.

        Args:
            source: Path to the Parquet file.
            chunk_size: Approximate number of rows per chunk (batch_size).
            **kwargs: Additional arguments. Supports 'columns' for column selection.

        Yields:
            pandas DataFrame chunks.
        """
        columns = kwargs.pop('columns', None)

        if is_cloud_path(source):
            filesystem = kwargs.pop('filesystem', None)
            if filesystem is None:
                filesystem = get_filesystem(source)
            parquet_file = pq.ParquetFile(
                source, filesystem=filesystem
            )
        else:
            parquet_file = pq.ParquetFile(source)

        for batch in parquet_file.iter_batches(
            batch_size=chunk_size, columns=columns
        ):
            yield batch.to_pandas()


class ParquetWriter(DataWriter):
    """Write data to Parquet files, including cloud storage paths."""

    def write(self, data: pd.DataFrame, destination: str, **kwargs) -> None:
        """
        Write data to a Parquet file.

        Supports local paths and cloud storage (S3, GCS, ABFS) via PyArrow's
        native filesystem support.

        Args:
            data: pandas DataFrame to write.
            destination: Path to the output Parquet file.
            **kwargs: Additional arguments to pass to pandas.DataFrame.to_parquet.
                Supports 'filesystem' for an fsspec filesystem instance.
        """
        if 'engine' not in kwargs:
            kwargs['engine'] = 'pyarrow'
        if 'compression' not in kwargs:
            kwargs['compression'] = 'snappy'

        if is_cloud_path(destination):
            filesystem = kwargs.pop('filesystem', None)
            if filesystem is None:
                filesystem = get_filesystem(destination)
            data.to_parquet(destination, filesystem=filesystem, **kwargs)
        else:
            data.to_parquet(destination, **kwargs)

    def write_chunks(
        self, data_iterator: Iterator[pd.DataFrame], destination: str,
        **kwargs
    ) -> None:
        """
        Write Parquet data in chunks using PyArrow writer.

        Args:
            data_iterator: Iterator yielding pandas DataFrames.
            destination: Path to the output Parquet file.
            **kwargs: Additional arguments (compression, etc.).
        """
        compression = kwargs.pop('compression', 'snappy')

        if is_cloud_path(destination):
            filesystem = kwargs.pop('filesystem', None)
            if filesystem is None:
                filesystem = get_filesystem(destination)
            dst = destination
        else:
            filesystem = None
            dst = destination

        writer = None
        try:
            for chunk in data_iterator:
                table = pa.Table.from_pandas(chunk)
                if writer is None:
                    writer = pq.ParquetWriter(
                        dst, table.schema,
                        compression=compression,
                        filesystem=filesystem,
                        **kwargs,
                    )
                writer.write_table(table)
        finally:
            if writer is not None:
                writer.close()
