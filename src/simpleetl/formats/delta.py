"""
Delta Lake format reader and writer for SimpleETL.

Requires the ``deltalake`` optional dependency (pure-Python, no Spark needed)::

    pip install simpleetl[delta]
    # or
    pip install deltalake

Supports reading the current snapshot or any historical version (time travel)
and writing with ``append``, ``overwrite``, or ``error`` semantics.
"""

from typing import Any, Dict, Iterator, List, Optional

import pandas as pd

from .base import DataReader, DataWriter


def _require_deltalake() -> None:
    try:
        import deltalake  # noqa: F401
    except ImportError:
        raise ImportError(
            "deltalake is required for Delta Lake format support. "
            "Install it with: pip install simpleetl[delta]"
        )


class DeltaLakeReader(DataReader):
    """Read data from a Delta Lake table.

    Supports both local directories and cloud paths (``s3://``, ``gs://``,
    ``az://``) as well as time travel via *version* or *timestamp*.

    Example::

        reader = DeltaLakeReader()

        # Current snapshot
        df = reader.read("/data/delta/sales")

        # Time travel — read version 5
        df = reader.read("/data/delta/sales", version=5)
    """

    def read(
        self,
        source: str,
        *,
        version: Optional[int] = None,
        timestamp: Optional[str] = None,
        columns: Optional[List[str]] = None,
        storage_options: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """Read a Delta table into a pandas DataFrame.

        Args:
            source: Path to the Delta table root directory.
            version: Table version to read (time travel by version).
            timestamp: ISO-8601 timestamp string for time travel
                (e.g. ``"2024-01-15T00:00:00"``).
            columns: Subset of columns to read.  Reads all columns when
                *None*.
            storage_options: Cloud storage credentials forwarded to the
                ``deltalake`` library (e.g. ``{"AWS_REGION": "us-east-1"}``).

        Returns:
            DataFrame containing the requested snapshot.

        Raises:
            ImportError: If ``deltalake`` is not installed.
        """
        _require_deltalake()
        import deltalake  # noqa: PLC0415

        dt_kwargs: Dict[str, Any] = {}
        if version is not None:
            dt_kwargs["version"] = version
        if timestamp is not None:
            dt_kwargs["timestamp"] = timestamp
        if storage_options:
            dt_kwargs["storage_options"] = storage_options

        dt = deltalake.DeltaTable(source, **dt_kwargs)
        return dt.to_pandas(columns=columns)

    def read_chunks(
        self,
        source: str,
        chunk_size: int = 10000,
        max_buffer_mb: float = 0,
        *,
        version: Optional[int] = None,
        timestamp: Optional[str] = None,
        columns: Optional[List[str]] = None,
        storage_options: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> Iterator[pd.DataFrame]:
        """Stream a Delta table in Arrow record-batch chunks.

        Args:
            source: Path to the Delta table root directory.
            chunk_size: Approximate number of rows per chunk.
            version: Table version for time travel.
            timestamp: Timestamp string for time travel.
            columns: Subset of columns to read.
            storage_options: Cloud storage credentials.

        Yields:
            DataFrame chunks.
        """
        _require_deltalake()
        import deltalake  # noqa: PLC0415

        dt_kwargs: Dict[str, Any] = {}
        if version is not None:
            dt_kwargs["version"] = version
        if timestamp is not None:
            dt_kwargs["timestamp"] = timestamp
        if storage_options:
            dt_kwargs["storage_options"] = storage_options

        dt = deltalake.DeltaTable(source, **dt_kwargs)
        dataset = dt.to_pyarrow_dataset()
        scanner = dataset.scanner(columns=columns, batch_size=chunk_size)
        for batch in scanner.to_batches():
            yield batch.to_pandas()


class DeltaLakeWriter(DataWriter):
    """Write a DataFrame to a Delta Lake table.

    Example::

        writer = DeltaLakeWriter()
        writer.write(df, "/data/delta/sales", mode="append")
        writer.write(df, "/data/delta/sales", mode="overwrite")

        # Partitioned write
        writer.write(df, "/data/delta/sales",
                     mode="append", partition_by=["year", "month"])
    """

    def write(
        self,
        data: pd.DataFrame,
        destination: str,
        *,
        mode: str = "append",
        partition_by: Optional[List[str]] = None,
        schema_mode: Optional[str] = None,
        storage_options: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> None:
        """Write *data* to a Delta table.

        Args:
            data: DataFrame to write.
            destination: Path to the Delta table root directory.
            mode: Write mode — ``"append"`` (default), ``"overwrite"``, or
                ``"error"`` (fail if data already exists at *destination*).
            partition_by: List of column names to partition the table by.
            schema_mode: Schema evolution mode forwarded to ``deltalake``
                (e.g. ``"overwrite"`` or ``"merge"``).
            storage_options: Cloud storage credentials.

        Raises:
            ValueError: If *mode* is not recognised.
            ImportError: If ``deltalake`` is not installed.
        """
        from .transactional_sink import execute_atomic

        _require_deltalake()
        import pyarrow as pa  # noqa: PLC0415

        valid_modes = {"append", "overwrite", "error"}
        if mode not in valid_modes:
            raise ValueError(
                f"Invalid mode '{mode}'. Must be one of {sorted(valid_modes)}."
            )

        table = pa.Table.from_pandas(data, preserve_index=False)

        write_kwargs: Dict[str, Any] = {"mode": mode}
        if partition_by:
            write_kwargs["partition_by"] = partition_by
        if schema_mode is not None:
            write_kwargs["schema_mode"] = schema_mode
        if storage_options:
            write_kwargs["storage_options"] = storage_options

        # Delta Lake uses directory-based tables; we wrap the write
        # in atomic rename at the directory level for exactly-once.
        execute_atomic(
            self,
            (table, write_kwargs),
            destination,
            mode=mode,
            partition_by=partition_by,
            schema_mode=schema_mode,
            storage_options=storage_options,
        )

    def _do_write(
        self,
        data: Any,
        destination: str,
        **kwargs: Any,
    ) -> None:
        _require_deltalake()
        import deltalake  # noqa: PLC0415
        table, write_kwargs = data
        deltalake.write_deltalake(destination, table, **write_kwargs)
