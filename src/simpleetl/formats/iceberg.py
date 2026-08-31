"""
Apache Iceberg format reader and writer for SimpleETL.

Requires the ``pyiceberg`` optional dependency (no Spark needed)::

    pip install simpleetl[iceberg]
    # or
    pip install pyiceberg

For zero-infrastructure local usage a SQLite-backed ``SqlCatalog`` is
created automatically inside the warehouse directory
(``sqlite:///<warehouse>/catalog.db``).  For production catalogs (REST,
AWS Glue, Hive) pass an explicit ``catalog_config`` dict, which is
forwarded to :func:`pyiceberg.catalog.load_catalog`.

Sources are addressed either as a plain warehouse path plus a ``table``
keyword, or as a single URI string usable in configs::

    iceberg:///path/to/warehouse?table=namespace.table
"""

import os
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Tuple
from urllib.parse import parse_qs

import pandas as pd

from .base import DataReader, DataWriter

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog
    from pyiceberg.table import Table

#: URI scheme prefix recognised by the factory and by ``_parse_source``.
URI_PREFIX = "iceberg://"

_DEFAULT_NAMESPACE = "default"


def _require_pyiceberg() -> None:
    try:
        import pyiceberg  # noqa: F401
    except ImportError:
        raise ImportError(
            "pyiceberg is required for Apache Iceberg format support. "
            "Install it with: pip install simpleetl[iceberg]"
        )


def _parse_source(source: str) -> Tuple[str, Optional[str]]:
    """Split a source string into ``(path, table_identifier)``.

    Accepts both plain paths (``/data/warehouse``) and URIs of the form
    ``iceberg:///data/warehouse?table=namespace.table``.  The table part
    is *None* when the source does not carry a ``table`` query parameter.
    """
    path = source
    table: Optional[str] = None
    if path.startswith(URI_PREFIX):
        path = path[len(URI_PREFIX) :]
    if "?" in path:
        path, query = path.split("?", 1)
        values = parse_qs(query).get("table")
        if values:
            table = values[0]
    return path, table


def _qualify(identifier: str) -> str:
    """Prefix bare table names with the ``default`` namespace."""
    if "." not in identifier:
        return f"{_DEFAULT_NAMESPACE}.{identifier}"
    return identifier


def _build_catalog(
    warehouse_path: Optional[str],
    catalog_name: str,
    catalog_config: Optional[Dict[str, str]],
) -> "Catalog":
    """Create or connect to a pyiceberg catalog.

    When *catalog_config* is provided it is forwarded verbatim to
    :func:`pyiceberg.catalog.load_catalog` (REST, Glue, Hive, ...).
    Otherwise a local SQLite-backed ``SqlCatalog`` is created inside
    *warehouse_path*.

    Raises:
        ValueError: If neither a warehouse path nor a catalog config is
            available.
        ImportError: If ``pyiceberg`` is not installed.
    """
    _require_pyiceberg()

    if catalog_config:
        from pyiceberg.catalog import load_catalog  # noqa: PLC0415

        return load_catalog(catalog_name, **catalog_config)

    if not warehouse_path:
        raise ValueError(
            "An Iceberg warehouse path is required when no catalog_config "
            "is provided. Pass a warehouse directory as the source, e.g. "
            "'iceberg:///data/warehouse?table=ns.table'."
        )

    from pyiceberg.catalog.sql import SqlCatalog  # noqa: PLC0415

    warehouse = os.path.abspath(os.path.expanduser(warehouse_path))
    os.makedirs(warehouse, exist_ok=True)
    return SqlCatalog(
        catalog_name,
        uri=f"sqlite:///{warehouse}/catalog.db",
        warehouse=f"file://{warehouse}",
    )


class _IcebergFormatBase:
    """Shared catalog and table-identifier handling for Iceberg formats."""

    def __init__(
        self,
        warehouse_path: Optional[str] = None,
        *,
        table: Optional[str] = None,
        catalog_name: str = "simpleetl",
        catalog_config: Optional[Dict[str, str]] = None,
    ) -> None:
        """Configure catalog access.

        Args:
            warehouse_path: Local warehouse directory for the default
                SQLite-backed ``SqlCatalog``.  May also be supplied later
                as the ``source``/``destination`` of each call.
            table: Default table identifier (``"namespace.table"``).
                Bare names are placed in the ``default`` namespace.
            catalog_name: Name under which the catalog is registered.
                The SQLite catalog scopes tables by this name, so reads
                must use the same *catalog_name* as the writes
                (default: ``"simpleetl"``).
            catalog_config: Properties for a real catalog (REST, Glue,
                Hive), forwarded to ``pyiceberg.catalog.load_catalog``.
                When set, no local SqlCatalog is created and the source
                string may simply be the table identifier.
        """
        self.warehouse_path = warehouse_path
        self.table = table
        self.catalog_name = catalog_name
        self.catalog_config = catalog_config

    def _resolve(
        self, source: Optional[str], table: Optional[str]
    ) -> Tuple["Catalog", str]:
        """Resolve a call into ``(catalog, qualified_identifier)``.

        Table precedence: explicit *table* argument, then a ``?table=``
        query parameter in *source*, then the constructor default.
        """
        warehouse = self.warehouse_path
        path: Optional[str] = None
        uri_table: Optional[str] = None
        if source:
            path, uri_table = _parse_source(source)

        identifier = table or uri_table or self.table
        if self.catalog_config:
            # With a real catalog the source string itself may be the
            # table identifier (there is no local warehouse path).
            if identifier is None:
                identifier = path
        elif path:
            warehouse = path

        if not identifier:
            raise ValueError(
                "No Iceberg table specified. Pass table='namespace.table' "
                "or use an 'iceberg://<warehouse>?table=<namespace>.<table>' "
                "source string."
            )

        catalog = _build_catalog(warehouse, self.catalog_name, self.catalog_config)
        return catalog, _qualify(identifier)


class IcebergReader(_IcebergFormatBase, DataReader):
    """Read data from an Apache Iceberg table.

    Supports column projection, row filtering, and time travel by
    snapshot id.  See the module docstring for catalog configuration.

    Example::

        reader = IcebergReader()

        # Local warehouse + table identifier
        df = reader.read("/data/warehouse", table="analytics.sales")

        # Single-string config form
        df = reader.read("iceberg:///data/warehouse?table=analytics.sales")

        # Projection, filter, and time travel
        df = reader.read(
            "/data/warehouse",
            table="analytics.sales",
            columns=["id", "amount"],
            row_filter="amount > 100",
            snapshot_id=5891234567890,
        )
    """

    def read(
        self,
        source: Optional[str] = None,
        *,
        table: Optional[str] = None,
        columns: Optional[List[str]] = None,
        row_filter: Optional[str] = None,
        snapshot_id: Optional[int] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """Read an Iceberg table into a pandas DataFrame.

        Args:
            source: Warehouse path or ``iceberg://`` URI.  Optional when
                the constructor already received a warehouse path or a
                ``catalog_config``.
            table: Table identifier (``"namespace.table"``).  Overrides
                any ``?table=`` parameter in *source*.
            columns: Subset of columns to read.  Reads all when *None*.
            row_filter: Iceberg row-filter expression string
                (e.g. ``"id > 100 and region == 'eu'"``).
            snapshot_id: Snapshot id for time travel.  Reads the current
                snapshot when *None*.

        Returns:
            DataFrame containing the requested snapshot.

        Raises:
            ValueError: If no table identifier can be resolved.
            ImportError: If ``pyiceberg`` is not installed.
        """
        _require_pyiceberg()

        catalog, identifier = self._resolve(source, table)
        iceberg_table = catalog.load_table(identifier)
        scan_kwargs = _scan_kwargs(columns, row_filter, snapshot_id)
        return iceberg_table.scan(**scan_kwargs).to_pandas()

    def read_chunks(
        self,
        source: Optional[str] = None,
        chunk_size: int = 10000,
        *,
        table: Optional[str] = None,
        columns: Optional[List[str]] = None,
        row_filter: Optional[str] = None,
        snapshot_id: Optional[int] = None,
        **kwargs: Any,
    ) -> Iterator[pd.DataFrame]:
        """Read an Iceberg table in chunks of at most *chunk_size* rows.

        pyiceberg scans do not stream natively, so the scan result is
        first materialised as a pyarrow Table and then yielded as
        record-batch slices.  Peak memory therefore covers the full
        (filtered/projected) scan in Arrow form.

        Args:
            source: Warehouse path or ``iceberg://`` URI.
            chunk_size: Maximum number of rows per chunk.
            table: Table identifier (``"namespace.table"``).
            columns: Subset of columns to read.
            row_filter: Iceberg row-filter expression string.
            snapshot_id: Snapshot id for time travel.

        Yields:
            DataFrame chunks.
        """
        _require_pyiceberg()

        catalog, identifier = self._resolve(source, table)
        iceberg_table = catalog.load_table(identifier)
        scan_kwargs = _scan_kwargs(columns, row_filter, snapshot_id)
        arrow_table = iceberg_table.scan(**scan_kwargs).to_arrow()
        for batch in arrow_table.to_batches(max_chunksize=chunk_size):
            yield batch.to_pandas()


def _scan_kwargs(
    columns: Optional[List[str]],
    row_filter: Optional[str],
    snapshot_id: Optional[int],
) -> Dict[str, Any]:
    """Build keyword arguments for ``Table.scan`` from optional values."""
    scan_kwargs: Dict[str, Any] = {}
    if columns is not None:
        scan_kwargs["selected_fields"] = tuple(columns)
    if row_filter is not None:
        scan_kwargs["row_filter"] = row_filter
    if snapshot_id is not None:
        scan_kwargs["snapshot_id"] = snapshot_id
    return scan_kwargs


class IcebergWriter(_IcebergFormatBase, DataWriter):
    """Write a DataFrame to an Apache Iceberg table.

    The namespace and table are created automatically when absent, using
    the schema of the written DataFrame (via pyarrow).

    Partitioning note: creating *partitioned* tables requires explicit
    pyiceberg ``PartitionSpec`` transforms keyed by Iceberg field ids,
    which cannot be derived reliably from a DataFrame, so this writer
    always creates unpartitioned tables.  To write into a partitioned
    table, create it with pyiceberg first — appends respect the
    existing partition spec.

    Example::

        writer = IcebergWriter()
        writer.write(df, "/data/warehouse", table="analytics.sales")
        writer.write(
            df,
            "iceberg:///data/warehouse?table=analytics.sales",
            mode="overwrite",
        )
    """

    def write(
        self,
        data: pd.DataFrame,
        destination: Optional[str] = None,
        *,
        table: Optional[str] = None,
        mode: str = "append",
        **kwargs: Any,
    ) -> None:
        """Write *data* to an Iceberg table.

        Args:
            data: DataFrame to write.
            destination: Warehouse path or ``iceberg://`` URI.  Optional
                when the constructor already received a warehouse path
                or a ``catalog_config``.
            table: Table identifier (``"namespace.table"``).  Overrides
                any ``?table=`` parameter in *destination*.
            mode: Write mode — ``"append"`` (default), ``"overwrite"``
                (replace the table contents), or ``"error"`` (fail if
                the table already exists).

        Raises:
            ValueError: If *mode* is not recognised, no table identifier
                can be resolved, or the table exists and *mode* is
                ``"error"``.
            ImportError: If ``pyiceberg`` is not installed.
        """
        _require_pyiceberg()
        import pyarrow as pa  # noqa: PLC0415

        valid_modes = {"append", "overwrite", "error"}
        if mode not in valid_modes:
            raise ValueError(
                f"Invalid mode '{mode}'. Must be one of {sorted(valid_modes)}."
            )

        catalog, identifier = self._resolve(destination, table)
        if mode == "error" and catalog.table_exists(identifier):
            raise ValueError(f"Table '{identifier}' already exists and mode='error'.")

        arrow_table = pa.Table.from_pandas(data, preserve_index=False)

        namespace = identifier.rsplit(".", 1)[0]
        catalog.create_namespace_if_not_exists(namespace)
        iceberg_table: "Table" = catalog.create_table_if_not_exists(
            identifier, schema=arrow_table.schema
        )

        if mode == "overwrite":
            iceberg_table.overwrite(arrow_table)
        else:
            iceberg_table.append(arrow_table)
