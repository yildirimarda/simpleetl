"""
Parquet format reader and writer using PyArrow, with an optional polars
fast path.
"""

import logging

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Any, Dict, Iterator, Optional
from .base import DataReader, DataWriter, _chunk_size_from_max_buffer
from ..core.engine import is_polars_available, validate_engine
from ..core.filesystem import is_cloud_path, get_filesystem

logger = logging.getLogger(__name__)

# pandas.read_parquet kwargs that translate directly to polars.read_parquet.
# Anything else triggers a fallback to the pandas path (with a debug log).
_POLARS_READ_KWARGS = {
    "columns": "columns",
}

# pandas.DataFrame.to_parquet kwargs that translate directly to
# polars.DataFrame.write_parquet.
_POLARS_WRITE_KWARGS = {
    "compression": "compression",
}


class ParquetReader(DataReader):
    """Read data from Parquet files, including cloud storage paths."""

    def read(self, source: str, engine: str = "pandas", **kwargs) -> pd.DataFrame:
        """
        Read data from a Parquet file.

        Supports local paths and cloud storage (S3, GCS, ABFS) via PyArrow's
        native filesystem support.

        Args:
            source: Path to the Parquet file.
            engine: ``"pandas"`` (default, reads via pyarrow) or
                ``"polars"``.  The polars fast path is used only for
                plain local paths and for kwargs that translate cleanly
                (``columns``); otherwise it falls back to pandas with a
                debug log.  If polars is not installed, a warning is
                logged and pandas is used.
            **kwargs: Additional arguments to pass to pandas.read_parquet.
                Supports 'filesystem' for an fsspec filesystem instance.

        Returns:
            pandas DataFrame containing the data.

        Raises:
            ValueError: If *engine* is not ``"pandas"`` or ``"polars"``.
        """
        validate_engine(engine)
        if engine == "polars":
            result = self._read_polars(source, kwargs)
            if result is not None:
                return result
        kwargs["engine"] = "pyarrow"

        if is_cloud_path(source):
            filesystem = kwargs.pop("filesystem", None)
            if filesystem is None:
                filesystem = get_filesystem(source)
            return pd.read_parquet(source, filesystem=filesystem, **kwargs)

        return pd.read_parquet(source, **kwargs)

    def _read_polars(
        self, source: str, kwargs: Dict[str, Any]
    ) -> Optional[pd.DataFrame]:
        """Try the polars Parquet fast path; return None to fall back."""
        if not is_polars_available():
            logger.warning(
                "engine='polars' requested but polars is not installed; "
                "falling back to pandas. Install it with: "
                "pip install simpleetl[polars]"
            )
            return None
        if is_cloud_path(source):
            logger.debug(
                "polars Parquet fast path supports local paths only; "
                "falling back to pandas for %s",
                source,
            )
            return None
        pl_kwargs: Dict[str, Any] = {}
        for key, value in kwargs.items():
            mapped = _POLARS_READ_KWARGS.get(key)
            if mapped is None:
                logger.debug(
                    "Parquet option %r has no polars equivalent; "
                    "falling back to pandas.",
                    key,
                )
                return None
            pl_kwargs[mapped] = value
        import polars as pl

        return pl.read_parquet(source, **pl_kwargs).to_pandas()

    def read_chunks(
        self,
        source: Any,
        chunk_size: int = 10000,
        max_buffer_mb: float = 0,
        engine: str = "pandas",
        **kwargs,
    ) -> Iterator[pd.DataFrame]:
        """
        Read Parquet data in row-group chunks.

        Chunked reads always use PyArrow; ``engine="polars"`` is accepted
        but ignored (with a debug log).

        Args:
            source: Path to the Parquet file.
            chunk_size: Approximate number of rows per chunk (batch_size).
            engine: ``"pandas"`` (default) or ``"polars"``.
            max_buffer_mb: When > 0, chunk_size is derived from this limit.
            **kwargs: Additional arguments. Supports 'columns' for column selection.

        Yields:
            pandas DataFrame chunks.

        Raises:
            ValueError: If *engine* is not ``"pandas"`` or ``"polars"``.
        """
        validate_engine(engine)
        if max_buffer_mb > 0:
            chunk_size = _chunk_size_from_max_buffer(max_buffer_mb)
        if engine == "polars":
            logger.debug(
                "Chunked Parquet reads always use pyarrow; ignoring engine='polars'."
            )
        columns = kwargs.pop("columns", None)

        if is_cloud_path(source):
            filesystem = kwargs.pop("filesystem", None)
            if filesystem is None:
                filesystem = get_filesystem(source)
            parquet_file = pq.ParquetFile(source, filesystem=filesystem)
        else:
            parquet_file = pq.ParquetFile(source)

        for batch in parquet_file.iter_batches(batch_size=chunk_size, columns=columns):
            yield batch.to_pandas()


class ParquetWriter(DataWriter):
    """Write data to Parquet files, including cloud storage paths."""

    def write(
        self, data: pd.DataFrame, destination: str, engine: str = "pandas", **kwargs
    ) -> None:
        """
        Write data to a Parquet file.

        Supports local paths and cloud storage (S3, GCS, ABFS) via PyArrow's
        native filesystem support.

        Args:
            data: pandas DataFrame to write.
            destination: Path to the output Parquet file.
            engine: ``"pandas"`` (default, writes via pyarrow) or
                ``"polars"``.  The polars fast path is used only for
                plain local paths and for kwargs that translate cleanly
                (``compression``); otherwise it falls back to pandas with
                a debug log.  If polars is not installed, a warning is
                logged and pandas is used.
            **kwargs: Additional arguments to pass to pandas.DataFrame.to_parquet.
                Supports 'filesystem' for an fsspec filesystem instance.

        Raises:
            ValueError: If *engine* is not ``"pandas"`` or ``"polars"``.
        """
        from .transactional_sink import execute_atomic

        validate_engine(engine)
        return execute_atomic(self, data, destination, engine=engine, **kwargs)

    def _do_write(
        self, data: pd.DataFrame, destination: str, engine: str = "pandas", **kwargs
    ) -> None:
        filesystem = kwargs.pop("filesystem", None)
        validate_engine(engine)
        if engine == "polars" and self._write_polars(data, destination, kwargs):
            return
        kwargs["engine"] = "pyarrow"
        if "compression" not in kwargs:
            kwargs["compression"] = "snappy"

        if is_cloud_path(destination):
            if filesystem is None:
                filesystem = get_filesystem(destination)
            data.to_parquet(destination, filesystem=filesystem, **kwargs)
        else:
            data.to_parquet(destination, **kwargs)

    def _write_polars(
        self, data: pd.DataFrame, destination: str, kwargs: Dict[str, Any]
    ) -> bool:
        """Try the polars Parquet fast path; return False to fall back."""
        if not is_polars_available():
            logger.warning(
                "engine='polars' requested but polars is not installed; "
                "falling back to pandas. Install it with: "
                "pip install simpleetl[polars]"
            )
            return False
        if is_cloud_path(destination):
            logger.debug(
                "polars Parquet fast path supports local paths only; "
                "falling back to pandas for %s",
                destination,
            )
            return False
        pl_kwargs: Dict[str, Any] = {"compression": "snappy"}
        for key, value in kwargs.items():
            mapped = _POLARS_WRITE_KWARGS.get(key)
            if mapped is None:
                logger.debug(
                    "Parquet option %r has no polars equivalent; "
                    "falling back to pandas.",
                    key,
                )
                return False
            pl_kwargs[mapped] = value
        import polars as pl

        pl.from_pandas(data).write_parquet(destination, **pl_kwargs)
        return True

    def write_chunks(
        self,
        data_iterator: Iterator[pd.DataFrame],
        destination: str,
        engine: str = "pandas",
        **kwargs,
    ) -> None:
        """
        Write Parquet data in chunks using PyArrow writer.

        Chunked writes always use PyArrow; ``engine="polars"`` is accepted
        but ignored (with a debug log).

        Args:
            data_iterator: Iterator yielding pandas DataFrames.
            destination: Path to the output Parquet file.
            engine: ``"pandas"`` (default) or ``"polars"``.
            **kwargs: Additional arguments (compression, etc.).

        Raises:
            ValueError: If *engine* is not ``"pandas"`` or ``"polars"``.
        """
        validate_engine(engine)
        if engine == "polars":
            logger.debug(
                "Chunked Parquet writes always use pyarrow; ignoring engine='polars'."
            )
        compression = kwargs.pop("compression", "snappy")

        if is_cloud_path(destination):
            filesystem = kwargs.pop("filesystem", None)
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
                        dst,
                        table.schema,
                        compression=compression,
                        filesystem=filesystem,
                        **kwargs,
                    )
                writer.write_table(table)
        finally:
            if writer is not None:
                writer.close()
