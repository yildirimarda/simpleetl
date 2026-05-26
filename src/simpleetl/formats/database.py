"""
Database format reader and writer using SQLAlchemy with connection pooling.
"""

import logging
from typing import Any, Iterable, Iterator, List, Optional, Union

import pandas as pd
import sqlalchemy
from sqlalchemy import text

from simpleetl.core.connection import (
    ConnectionConfig,
    ConnectionPool,
    get_engine,
)

from .base import DataReader, DataWriter

logger = logging.getLogger(__name__)


class DatabaseReader(DataReader):
    """Read data from databases using SQL queries with connection pooling."""

    def read(  # type: ignore[override]
        self,
        source: Union[str, sqlalchemy.engine.Engine, ConnectionPool],
        **kwargs,
    ) -> pd.DataFrame | Iterable[pd.DataFrame]:
        """
        Read data from a database.

        Args:
            source: Database connection string, SQLAlchemy engine,
                ConnectionPool, or SQLAlchemy URL.
            **kwargs: Additional arguments to pass to pandas.read_sql.
                - sql: SQL query string (required for engine/ConnectionPool).
                - table: Table name to read (alternative to sql).
                - chunksize: If set, returns an iterator of DataFrames.
                - params: Parameters for parameterized queries.

        Returns:
            pandas DataFrame containing the data, or an iterator of DataFrames
            if chunksize is specified.

        Raises:
            ValueError: If the source type is invalid or sql is missing
                for engine-based sources.
        """
        sql = kwargs.pop("sql", None)
        table = kwargs.pop("table", None)
        chunksize = kwargs.pop("chunksize", None)
        params = kwargs.pop("params", None)

        # Determine the engine: source may be a URL string, engine, or pool.
        # If source is a non-URL string (table name), we still need an engine
        # from kwargs or raise an error.
        engine = self._resolve_engine(source)

        if sql:
            return pd.read_sql(
                sql, engine, params=params, chunksize=chunksize, **kwargs,
            )
        if table:
            return pd.read_sql_table(
                table, engine, chunksize=chunksize, **kwargs,
            )

        raise ValueError(
            "Must provide 'sql' or 'table' parameter"
        )

    def read_chunks(
        self,
        source: Union[str, sqlalchemy.engine.Engine, ConnectionPool],
        chunk_size: int = 10000,
        **kwargs,
    ) -> Iterator[pd.DataFrame]:
        """
        Read database data in chunks.

        Args:
            source: Database connection string, SQLAlchemy engine,
                or ConnectionPool.
            chunk_size: Number of rows per chunk.
            **kwargs: Must include 'sql' parameter.

        Yields:
            pandas DataFrame chunks.
        """
        sql = kwargs.pop("sql", None)
        if sql is None:
            raise ValueError("Must provide 'sql' parameter for chunked reading")

        chunk_iter = self.read(
            source, sql=sql, chunksize=chunk_size, **kwargs,
        )
        for chunk in chunk_iter:  # type: ignore[misc]
            yield chunk  # type: ignore[misc]

    @staticmethod
    def incremental_query(
        table: str,
        watermark_column: str,
        last_value: Any,
        additional_columns: Optional[List[str]] = None,
        order_by: Optional[str] = None,
    ) -> str:
        """
        Build a watermark-based incremental query for ETL loads.

        Args:
            table: Table name to query.
            watermark_column: Column used for watermark tracking
                (e.g., 'updated_at', 'id').
            last_value: The last watermark value from the previous run.
            additional_columns: Specific columns to select. Defaults to *.
            order_by: Column to order by. Defaults to watermark_column.

        Returns:
            A SQL query string with the watermark value formatted in.
        """
        columns = ", ".join(additional_columns) if additional_columns else "*"
        order = order_by or watermark_column
        if isinstance(last_value, str):
            formatted_value = f"'{last_value}'"
        else:
            formatted_value = str(last_value)
        return (
            f"SELECT {columns} FROM {table} "
            f"WHERE {watermark_column} > {formatted_value} "
            f"ORDER BY {order}"
        )

    @staticmethod
    def _resolve_engine(
        source: Union[str, sqlalchemy.engine.Engine, ConnectionPool],
    ) -> sqlalchemy.engine.Engine:
        """Resolve various source types to a SQLAlchemy engine."""
        if isinstance(source, ConnectionPool):
            return source.engine
        if isinstance(source, sqlalchemy.engine.Engine):
            return source
        if isinstance(source, str):
            # Only treat as URL if it looks like a connection string
            if source.startswith(("sqlite://", "postgresql://", "mysql://", "mssql://")):
                return get_engine(ConnectionConfig(url=source))
            raise ValueError(
                f"String source must be a connection URL "
                f"(sqlite://, postgresql://, mysql://, mssql://). Got: {source}"
            )
        raise ValueError(
            "Invalid source type. Must be connection string, "
            "URL, engine, or ConnectionPool."
        )


class DatabaseWriter(DataWriter):
    """Write data to databases with connection pooling and UPSERT support."""

    def write(
        self,
        data: pd.DataFrame,
        destination: Union[str, sqlalchemy.engine.Engine, ConnectionPool],
        **kwargs,
    ) -> None:
        """
        Write data to a database.

        Args:
            data: pandas DataFrame to write.
            destination: Database connection string, SQLAlchemy engine,
                or ConnectionPool.
            **kwargs: Additional arguments for database writing.
                - table_name: Target table name (default: 'data').
                - if_exists: Behavior when table exists
                  ('fail', 'replace', 'append'). Default: 'fail'.
                - index: Whether to write the DataFrame index. Default: False.
                - chunksize: Rows per batch insert. Default: None (all at once).
                - dtype: Dict of column types for the database.
                - method: Insert method ('multi' for multi-row INSERT).
        """
        engine = self._resolve_engine(destination)

        table_name = kwargs.pop("table_name", "data")
        if_exists = kwargs.pop("if_exists", "fail")
        index = kwargs.pop("index", False)
        chunksize = kwargs.pop("chunksize", None)

        data.to_sql(
            name=table_name,
            con=engine,
            if_exists=if_exists,
            index=index,
            chunksize=chunksize,
            **kwargs,
        )

    def write_chunks(
        self,
        data_iterator: Iterator[pd.DataFrame],
        destination: Union[str, sqlalchemy.engine.Engine, ConnectionPool],
        **kwargs,
    ) -> None:
        """
        Write database data in chunks. First chunk uses 'replace',
        rest use 'append'.

        Args:
            data_iterator: Iterator yielding pandas DataFrames.
            destination: Database connection string, SQLAlchemy engine,
                or ConnectionPool.
            **kwargs: Additional arguments.
        """
        engine = self._resolve_engine(destination)

        table_name = kwargs.pop("table_name", "data")
        index = kwargs.pop("index", False)

        first = True
        for chunk in data_iterator:
            chunk.to_sql(
                name=table_name,
                con=engine,
                if_exists="replace" if first else "append",
                index=index,
                **kwargs,
            )
            first = False

    def merge(
        self,
        data: pd.DataFrame,
        destination: Union[str, sqlalchemy.engine.Engine, ConnectionPool],
        table_name: str,
        key_columns: List[str],
        update_columns: Optional[List[str]] = None,
        **kwargs,
    ) -> int:
        """
        Perform an UPSERT (INSERT or UPDATE) operation.

        Supports PostgreSQL ON CONFLICT, MySQL REPLACE, and SQLite REPLACE
        syntax depending on the database dialect.

        Args:
            data: pandas DataFrame containing the data to upsert.
            destination: Database connection string, SQLAlchemy engine,
                or ConnectionPool.
            table_name: Target table name.
            key_columns: Columns that form the unique key for conflict
                detection.
            update_columns: Columns to update on conflict. Defaults to all
                non-key columns.
            **kwargs: Additional arguments (e.g., schema).

        Returns:
            Number of rows affected.
        """
        engine = self._resolve_engine(destination)
        dialect = engine.dialect.name

        if update_columns is None:
            update_columns = [
                col for col in data.columns if col not in key_columns
            ]

        if dialect == "postgresql":
            return self._merge_postgresql(
                engine, data, table_name, key_columns, update_columns, **kwargs,
            )
        if dialect == "mysql":
            return self._merge_mysql(
                engine, data, table_name, key_columns, update_columns, **kwargs,
            )
        if dialect == "sqlite":
            return self._merge_sqlite(
                engine, data, table_name, key_columns, update_columns, **kwargs,
            )

        # Fallback: delete + insert pattern
        return self._merge_generic(
            engine, data, table_name, key_columns, update_columns, **kwargs,
        )

    @staticmethod
    def _merge_postgresql(
        engine: sqlalchemy.engine.Engine,
        data: pd.DataFrame,
        table_name: str,
        key_columns: List[str],
        update_columns: List[str],
        **kwargs,
    ) -> int:
        """PostgreSQL UPSERT using ON CONFLICT ... DO UPDATE."""
        schema = kwargs.pop("schema", None)
        full_table = f"{schema}.{table_name}" if schema else table_name

        columns = list(data.columns)
        col_list = ", ".join(columns)
        val_placeholders = ", ".join([f":{c}" for c in columns])
        conflict_cols = ", ".join(key_columns)
        update_clause = ", ".join(
            [f"{c} = EXCLUDED.{c}" for c in update_columns]
        )

        sql = (
            f"INSERT INTO {full_table} ({col_list}) VALUES ({val_placeholders}) "
            f"ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_clause}"
        )

        rows_affected = 0
        with engine.begin() as conn:
            for _, row in data.iterrows():
                params = {c: row[c] for c in columns}
                result = conn.execute(text(sql), params)
                rows_affected += result.rowcount

        logger.info("PostgreSQL UPSERT affected %d rows", rows_affected)
        return rows_affected

    @staticmethod
    def _merge_mysql(
        engine: sqlalchemy.engine.Engine,
        data: pd.DataFrame,
        table_name: str,
        key_columns: List[str],
        update_columns: List[str],
        **kwargs,
    ) -> int:
        """MySQL UPSERT using INSERT ... ON DUPLICATE KEY UPDATE."""
        columns = list(data.columns)
        col_list = ", ".join(columns)
        val_placeholders = ", ".join([f":{c}" for c in columns])
        update_clause = ", ".join(
            [f"{c} = VALUES({c})" for c in update_columns]
        )

        sql = (
            f"INSERT INTO {table_name} ({col_list}) VALUES ({val_placeholders}) "
            f"ON DUPLICATE KEY UPDATE {update_clause}"
        )

        rows_affected = 0
        with engine.begin() as conn:
            for _, row in data.iterrows():
                params = {c: row[c] for c in columns}
                result = conn.execute(text(sql), params)
                rows_affected += result.rowcount

        logger.info("MySQL UPSERT affected %d rows", rows_affected)
        return rows_affected

    @staticmethod
    def _merge_sqlite(
        engine: sqlalchemy.engine.Engine,
        data: pd.DataFrame,
        table_name: str,
        key_columns: List[str],
        update_columns: List[str],
        **kwargs,
    ) -> int:
        """SQLite UPSERT using INSERT ... ON CONFLICT ... DO UPDATE (3.24+)."""
        columns = list(data.columns)
        col_list = ", ".join(columns)
        val_placeholders = ", ".join([f":{c}" for c in columns])
        conflict_cols = ", ".join(key_columns)
        update_clause = ", ".join(
            [f"{c} = excluded.{c}" for c in update_columns]
        )

        sql = (
            f"INSERT INTO {table_name} ({col_list}) VALUES ({val_placeholders}) "
            f"ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_clause}"
        )

        rows_affected = 0
        with engine.begin() as conn:
            for _, row in data.iterrows():
                params = {c: row[c] for c in columns}
                result = conn.execute(text(sql), params)
                rows_affected += result.rowcount

        logger.info("SQLite UPSERT affected %d rows", rows_affected)
        return rows_affected

    @staticmethod
    def _merge_generic(
        engine: sqlalchemy.engine.Engine,
        data: pd.DataFrame,
        table_name: str,
        key_columns: List[str],
        update_columns: List[str],
        **kwargs,
    ) -> int:
        """Generic UPSERT fallback using DELETE + INSERT in a transaction."""
        schema = kwargs.pop("schema", None)
        full_table = f"{schema}.{table_name}" if schema else table_name

        rows_affected = 0
        with engine.begin() as conn:
            for _, row in data.iterrows():
                where_clause = " AND ".join(
                    [f"{c} = :key_{c}" for c in key_columns]
                )
                delete_sql = f"DELETE FROM {full_table} WHERE {where_clause}"

                key_params = {f"key_{c}": row[c] for c in key_columns}
                result = conn.execute(text(delete_sql), key_params)
                deleted = result.rowcount

                columns = list(data.columns)
                col_list = ", ".join(columns)
                val_placeholders = ", ".join([f":{c}" for c in columns])
                insert_sql = (
                    f"INSERT INTO {full_table} ({col_list}) "
                    f"VALUES ({val_placeholders})"
                )
                insert_params = {c: row[c] for c in columns}
                conn.execute(text(insert_sql), insert_params)
                rows_affected += deleted + 1

        logger.info("Generic UPSERT affected %d rows", rows_affected)
        return rows_affected

    @staticmethod
    def _resolve_engine(
        source: Union[str, sqlalchemy.engine.Engine, ConnectionPool],
    ) -> sqlalchemy.engine.Engine:
        """Resolve various destination types to a SQLAlchemy engine."""
        if isinstance(source, ConnectionPool):
            return source.engine
        if isinstance(source, sqlalchemy.engine.Engine):
            return source
        if isinstance(source, str):
            # Only treat as URL if it looks like a connection string
            if source.startswith(("sqlite://", "postgresql://", "mysql://", "mssql://")):
                return get_engine(ConnectionConfig(url=source))
            raise ValueError(
                f"String source must be a connection URL "
                f"(sqlite://, postgresql://, mysql://, mssql://). Got: {source}"
            )
        raise ValueError(
            "Invalid destination type. Must be connection string, "
            "URL, engine, or ConnectionPool."
        )
