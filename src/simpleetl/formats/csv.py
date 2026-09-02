"""
CSV format reader and writer using pandas, with an optional polars fast path.
"""

import logging

import pandas as pd
from typing import Any, Dict, Iterator, Optional
from .base import DataReader, DataWriter, _chunk_size_from_max_buffer
from ..core.engine import is_polars_available, validate_engine
from ..core.filesystem import is_cloud_path, get_filesystem

logger = logging.getLogger(__name__)

# pandas.read_csv kwargs that translate directly to polars.read_csv.
# Anything else triggers a fallback to the pandas path (with a debug log).
_POLARS_READ_KWARGS = {
    "usecols": "columns",
    "sep": "separator",
    "delimiter": "separator",
    "nrows": "n_rows",
}

# pandas.DataFrame.to_csv kwargs that translate directly to
# polars.DataFrame.write_csv.  'columns' is handled separately via a select.
_POLARS_WRITE_KWARGS = {
    "sep": "separator",
    "header": "include_header",
}


class CSVReader(DataReader):
    """Read data from CSV files, including cloud storage paths."""

    def read(self, source: str, engine: str = "pandas", **kwargs) -> pd.DataFrame:
        """
        Read data from a CSV file.

        Supports local paths and cloud storage (S3, GCS, ABFS) via fsspec.

        Args:
            source: Path to the CSV file.
            engine: ``"pandas"`` (default) or ``"polars"``.  The polars
                fast path is used only for plain local paths and for
                kwargs that translate cleanly (``usecols``, ``sep``,
                ``delimiter``, ``nrows``); otherwise it falls back to
                pandas with a debug log.  If polars is not installed, a
                warning is logged and pandas is used.
            **kwargs: Additional arguments to pass to pandas.read_csv.
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
        if is_cloud_path(source):
            filesystem = kwargs.pop("filesystem", None)
            if filesystem is None:
                filesystem = get_filesystem(source)
            with filesystem.open(source, "r") as f:
                return pd.read_csv(f, **kwargs)
        return pd.read_csv(source, **kwargs)

    def _read_polars(
        self, source: str, kwargs: Dict[str, Any]
    ) -> Optional[pd.DataFrame]:
        """Try the polars CSV fast path; return None to fall back."""
        if not is_polars_available():
            logger.warning(
                "engine='polars' requested but polars is not installed; "
                "falling back to pandas. Install it with: "
                "pip install simpleetl[polars]"
            )
            return None
        if is_cloud_path(source):
            logger.debug(
                "polars CSV fast path supports local paths only; "
                "falling back to pandas for %s",
                source,
            )
            return None
        pl_kwargs: Dict[str, Any] = {}
        for key, value in kwargs.items():
            mapped = _POLARS_READ_KWARGS.get(key)
            if mapped is None:
                logger.debug(
                    "CSV option %r has no polars equivalent; falling back to pandas.",
                    key,
                )
                return None
            pl_kwargs[mapped] = value
        import polars as pl

        return pl.read_csv(source, **pl_kwargs).to_pandas()

    def read_chunks(
        self,
        source: Any,
        chunk_size: int = 10000,
        max_buffer_mb: float = 0,
        engine: str = "pandas",
        **kwargs,
    ) -> Iterator[pd.DataFrame]:
        """
        Read CSV data in chunks.

        Chunked reads always use pandas; ``engine="polars"`` is accepted
        but ignored (with a debug log).

        Args:
            source: Path to the CSV file.
            chunk_size: Number of rows per chunk.
            engine: ``"pandas"`` (default) or ``"polars"``.
            max_buffer_mb: Maximum memory in MB for a single chunk.
                When > 0, chunk_size is derived from this limit.
            **kwargs: Additional arguments to pass to pandas.read_csv.

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
                "Chunked CSV reads always use pandas; ignoring engine='polars'."
            )
        if is_cloud_path(source):
            filesystem = kwargs.pop("filesystem", None)
            if filesystem is None:
                filesystem = get_filesystem(source)
            with filesystem.open(source, "r") as f:
                for chunk in pd.read_csv(f, chunksize=chunk_size, **kwargs):
                    yield chunk
        else:
            for chunk in pd.read_csv(source, chunksize=chunk_size, **kwargs):
                yield chunk


class CSVWriter(DataWriter):
    """Write data to CSV files, including cloud storage paths."""

    def write(
        self, data: pd.DataFrame, destination: str, engine: str = "pandas", **kwargs
    ) -> None:
        """
        Write data to a CSV file.

        Supports local paths and cloud storage (S3, GCS, ABFS) via fsspec.

        Args:
            data: pandas DataFrame to write.
            destination: Path to the output CSV file.
            engine: ``"pandas"`` (default) or ``"polars"``.  The polars
                fast path is used only for plain local paths and for
                kwargs that translate cleanly (``sep``, ``header``,
                ``columns``); otherwise it falls back to pandas with a
                debug log.  If polars is not installed, a warning is
                logged and pandas is used.
            **kwargs: Additional arguments to pass to pandas.DataFrame.to_csv.
                Supports 'filesystem' for an fsspec filesystem instance.

        Raises:
            ValueError: If *engine* is not ``"pandas"`` or ``"polars"``.
        """
        validate_engine(engine)
        if engine == "polars" and self._write_polars(data, destination, kwargs):
            return
        if is_cloud_path(destination):
            filesystem = kwargs.pop("filesystem", None)
            if filesystem is None:
                filesystem = get_filesystem(destination)
            with filesystem.open(destination, "w") as f:
                data.to_csv(f, index=False, **kwargs)
        else:
            data.to_csv(destination, index=False, **kwargs)

    def _write_polars(
        self, data: pd.DataFrame, destination: str, kwargs: Dict[str, Any]
    ) -> bool:
        """Try the polars CSV fast path; return False to fall back."""
        if not is_polars_available():
            logger.warning(
                "engine='polars' requested but polars is not installed; "
                "falling back to pandas. Install it with: "
                "pip install simpleetl[polars]"
            )
            return False
        if is_cloud_path(destination):
            logger.debug(
                "polars CSV fast path supports local paths only; "
                "falling back to pandas for %s",
                destination,
            )
            return False
        pl_kwargs: Dict[str, Any] = {}
        columns = None
        for key, value in kwargs.items():
            if key == "columns":
                columns = value
                continue
            mapped = _POLARS_WRITE_KWARGS.get(key)
            if mapped is None:
                logger.debug(
                    "CSV option %r has no polars equivalent; falling back to pandas.",
                    key,
                )
                return False
            pl_kwargs[mapped] = value
        import polars as pl

        frame = pl.from_pandas(data)
        if columns is not None:
            frame = frame.select(list(columns))
        frame.write_csv(destination, **pl_kwargs)
        return True

    def write_chunks(
        self,
        data_iterator: Iterator[pd.DataFrame],
        destination: str,
        engine: str = "pandas",
        **kwargs,
    ) -> None:
        """
        Write CSV data in chunks. Appends after the first chunk.

        Chunked writes always use pandas; ``engine="polars"`` is accepted
        but ignored (with a debug log).

        Args:
            data_iterator: Iterator yielding pandas DataFrames.
            destination: Path to the output CSV file.
            engine: ``"pandas"`` (default) or ``"polars"``.
            **kwargs: Additional arguments.

        Raises:
            ValueError: If *engine* is not ``"pandas"`` or ``"polars"``.
        """
        validate_engine(engine)
        if engine == "polars":
            logger.debug(
                "Chunked CSV writes always use pandas; ignoring engine='polars'."
            )
        first = True
        if is_cloud_path(destination):
            filesystem = kwargs.pop("filesystem", None)
            if filesystem is None:
                filesystem = get_filesystem(destination)
            with filesystem.open(destination, "w") as f:
                for chunk in data_iterator:
                    chunk.to_csv(f, index=False, header=first, mode="a", **kwargs)
                    first = False
        else:
            for chunk in data_iterator:
                chunk.to_csv(destination, index=False, header=first, mode="a", **kwargs)
                first = False
