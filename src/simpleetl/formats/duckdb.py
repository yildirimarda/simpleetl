"""
DuckDB format reader and writer for SimpleETL.

Requires the ``duckdb`` optional dependency::

    pip install simpleetl[duckdb]
    # or
    pip install duckdb

DuckDBReader executes a SQL query against a DuckDB database file and returns
a pandas DataFrame.  DuckDBWriter persists a DataFrame into a DuckDB table.
"""

from typing import Iterator, Optional

import pandas as pd

from .base import DataReader, DataWriter


def _require_duckdb():
    try:
        import duckdb  # noqa: F401
    except ImportError:
        raise ImportError(
            "duckdb is required for DuckDB format support. "
            "Install it with: pip install simpleetl[duckdb]"
        )


class DuckDBReader(DataReader):
    """Read data from a DuckDB database file via SQL.

    Args:
        read_only: Open the database in read-only mode (default: True).

    Example::

        reader = DuckDBReader()
        df = reader.read("mydb.duckdb", query="SELECT * FROM sales WHERE year = 2024")
    """

    def __init__(self, *, read_only: bool = True) -> None:
        self.read_only = read_only

    def read(
        self,
        source: str,
        *,
        query: Optional[str] = None,
        table: Optional[str] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """Read from a DuckDB file.

        Args:
            source: Path to the DuckDB database file, or ``":memory:"`` for
                an in-process database.
            query: SQL query to execute.  Mutually exclusive with *table*.
            table: Table name to read in full (``SELECT * FROM <table>``).
                Mutually exclusive with *query*.

        Returns:
            Query result as a pandas DataFrame.

        Raises:
            ValueError: If neither *query* nor *table* is provided.
            ImportError: If ``duckdb`` is not installed.
        """
        _require_duckdb()
        import duckdb  # noqa: PLC0415

        if not query and not table:
            raise ValueError(
                "DuckDBReader requires either 'query' or 'table' parameter."
            )

        sql = query if query else f"SELECT * FROM {table}"
        conn = duckdb.connect(source, read_only=self.read_only)
        try:
            return conn.execute(sql).df()
        finally:
            conn.close()

    def read_chunks(
        self,
        source: str,
        chunk_size: int = 10000,
        max_buffer_mb: float = 0,
        *,
        query: Optional[str] = None,
        table: Optional[str] = None,
        **kwargs,
    ) -> Iterator[pd.DataFrame]:
        """Stream query results in chunks.

        Args:
            source: Path to the DuckDB database file.
            chunk_size: Number of rows per chunk.
            query: SQL query to execute.
            table: Table name to read.

        Yields:
            DataFrame chunks of at most *chunk_size* rows.
        """
        _require_duckdb()
        import duckdb  # noqa: PLC0415

        if not query and not table:
            raise ValueError(
                "DuckDBReader requires either 'query' or 'table' parameter."
            )

        sql = query if query else f"SELECT * FROM {table}"
        conn = duckdb.connect(source, read_only=self.read_only)
        try:
            result = conn.execute(sql)
            col_names = [desc[0] for desc in result.description]
            while True:
                batch = result.fetchmany(chunk_size)
                if not batch:
                    break
                yield pd.DataFrame(batch, columns=col_names)
        finally:
            conn.close()


class DuckDBWriter(DataWriter):
    """Write a DataFrame into a DuckDB database table.

    Args:
        None

    Example::

        writer = DuckDBWriter()
        writer.write(df, "mydb.duckdb", table_name="sales", mode="append")
    """

    def write(
        self,
        data: pd.DataFrame,
        destination: str,
        *,
        table_name: str = "data",
        mode: str = "append",
        **kwargs,
    ) -> None:
        """Write *data* to a DuckDB table.

        Args:
            data: DataFrame to write.
            destination: Path to the DuckDB database file, or ``":memory:"``.
            table_name: Target table name (default: ``"data"``).
            mode: One of ``"append"`` (default), ``"replace"``, or
                ``"error"`` (fail if table already exists).

        Raises:
            ValueError: If *mode* is not recognised or the table exists and
                *mode* is ``"error"``.
            ImportError: If ``duckdb`` is not installed.
        """
        _require_duckdb()
        import duckdb  # noqa: PLC0415

        valid_modes = {"append", "replace", "error"}
        if mode not in valid_modes:
            raise ValueError(
                f"Invalid mode '{mode}'. Must be one of {sorted(valid_modes)}."
            )

        conn = duckdb.connect(destination)
        try:
            conn.register("_simpleetl_input", data)

            tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
            table_exists = table_name in tables

            if mode == "error" and table_exists:
                raise ValueError(
                    f"Table '{table_name}' already exists and mode='error'."
                )
            elif mode == "replace":
                conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                conn.execute(
                    f"CREATE TABLE {table_name} AS SELECT * FROM _simpleetl_input"
                )
            elif table_exists:
                conn.execute(f"INSERT INTO {table_name} SELECT * FROM _simpleetl_input")
            else:
                conn.execute(
                    f"CREATE TABLE {table_name} AS SELECT * FROM _simpleetl_input"
                )
        finally:
            conn.close()
