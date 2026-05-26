"""
Tests for connection management module.

Uses SQLite for testing pooling, SSL params, and connection lifecycle.
"""

import os
import tempfile

import pandas as pd
import pytest
import sqlalchemy

from simpleetl.core.connection import (
    ConnectionConfig,
    ConnectionPool,
    _retry_operation,
    _sanitize_url,
    dispose_all,
    dispose_engine,
    get_connection,
    get_engine,
)
from simpleetl.formats.database import DatabaseReader, DatabaseWriter


# ---------------------------------------------------------------------------
# ConnectionConfig tests
# ---------------------------------------------------------------------------


class TestConnectionConfig:
    """Tests for the ConnectionConfig dataclass."""

    def test_defaults(self):
        """Test default configuration values."""
        config = ConnectionConfig(url="sqlite:///test.db")
        assert config.pool_size == 5
        assert config.max_overflow == 10
        assert config.pool_timeout == 30
        assert config.pool_recycle == 3600
        assert config.connect_timeout == 10
        assert config.read_timeout == 30
        assert config.write_timeout == 30
        assert config.retry_count == 3
        assert config.retry_delay == 1.0
        assert config.ssl_mode is None
        assert config.ssl_ca is None
        assert config.ssl_cert is None
        assert config.ssl_key is None

    def test_custom_values(self):
        """Test custom configuration values."""
        config = ConnectionConfig(
            url="postgresql://user:pass@localhost/db",
            pool_size=20,
            max_overflow=30,
            ssl_mode="require",
            connect_timeout=60,
            retry_count=5,
        )
        assert config.pool_size == 20
        assert config.max_overflow == 30
        assert config.ssl_mode == "require"
        assert config.connect_timeout == 60
        assert config.retry_count == 5

    def test_to_engine_kwargs_basic(self):
        """Test engine kwargs generation for basic config (PostgreSQL)."""
        config = ConnectionConfig(url="postgresql://localhost/db")
        kwargs = config.to_engine_kwargs()
        assert kwargs["pool_size"] == 5
        assert kwargs["max_overflow"] == 10
        assert kwargs["pool_timeout"] == 30
        assert kwargs["pool_recycle"] == 3600

    def test_to_engine_kwargs_sqlite_omits_pool_args(self):
        """Test that SQLite omits pool_size/max_overflow/pool_timeout."""
        config = ConnectionConfig(url="sqlite:///test.db")
        kwargs = config.to_engine_kwargs()
        assert "pool_size" not in kwargs
        assert "max_overflow" not in kwargs
        assert "pool_timeout" not in kwargs
        assert kwargs["pool_recycle"] == 3600

    def test_to_engine_kwargs_with_ssl(self):
        """Test engine kwargs include SSL configuration."""
        config = ConnectionConfig(
            url="postgresql://localhost/db",
            ssl_ca="/path/to/ca.pem",
            ssl_cert="/path/to/cert.pem",
            ssl_key="/path/to/key.pem",
            ssl_mode="verify-full",
        )
        kwargs = config.to_engine_kwargs()
        connect_args = kwargs["connect_args"]
        assert "ssl" in connect_args
        ssl = connect_args["ssl"]
        assert ssl["sslmode"] == "verify-full"
        assert ssl["sslca"] == "/path/to/ca.pem"
        assert ssl["sslcert"] == "/path/to/cert.pem"
        assert ssl["sslkey"] == "/path/to/key.pem"

    def test_to_engine_kwargs_ssl_mode_only(self):
        """Test SSL mode without cert files."""
        config = ConnectionConfig(
            url="postgresql://localhost/db",
            ssl_mode="require",
        )
        kwargs = config.to_engine_kwargs()
        connect_args = kwargs["connect_args"]
        assert connect_args["ssl"]["sslmode"] == "require"

    def test_to_engine_kwargs_no_ssl(self):
        """Test that no SSL args are added when SSL is not configured."""
        config = ConnectionConfig(url="sqlite:///test.db")
        kwargs = config.to_engine_kwargs()
        connect_args = kwargs.get("connect_args", {})
        assert "ssl" not in connect_args

    def test_to_engine_kwargs_connect_timeout(self):
        """Test connect_timeout is included in connect_args for non-SQLite."""
        config = ConnectionConfig(
            url="postgresql://localhost/test",
            connect_timeout=15,
        )
        kwargs = config.to_engine_kwargs()
        assert kwargs["connect_args"]["connect_timeout"] == 15

    def test_to_engine_kwargs_custom_connect_args(self):
        """Test that custom connect_args are preserved."""
        config = ConnectionConfig(
            url="sqlite:///test.db",
            connect_args={"check_same_thread": False},
        )
        kwargs = config.to_engine_kwargs()
        assert kwargs["connect_args"]["check_same_thread"] is False


# ---------------------------------------------------------------------------
# Engine management tests
# ---------------------------------------------------------------------------


class TestEngineManagement:
    """Tests for engine creation, caching, and disposal."""

    def setup_method(self):
        """Clear engine registry before each test."""
        dispose_all()

    def teardown_method(self):
        """Clear engine registry after each test."""
        dispose_all()

    def test_get_engine_creates_engine(self):
        """Test that get_engine creates a SQLAlchemy engine."""
        config = ConnectionConfig(url="sqlite:///:memory:")
        engine = get_engine(config)
        assert isinstance(engine, sqlalchemy.engine.Engine)

    def test_get_engine_caches_by_url(self):
        """Test that engines are cached by URL."""
        config = ConnectionConfig(url="sqlite:///:memory:")
        engine1 = get_engine(config)
        engine2 = get_engine(config)
        assert engine1 is engine2

    def test_get_engine_different_urls(self):
        """Test that different URLs create different engines."""
        config1 = ConnectionConfig(url="sqlite:///:memory:")
        # Use file-based SQLite for second to get a different URL
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            url = f"sqlite:///{f.name}"
        try:
            config2 = ConnectionConfig(url=url)
            engine1 = get_engine(config1)
            engine2 = get_engine(config2)
            assert engine1 is not engine2
        finally:
            dispose_engine(url)
            os.unlink(f.name)

    def test_dispose_engine(self):
        """Test that dispose_engine removes engine from registry."""
        config = ConnectionConfig(url="sqlite:///:memory:")
        engine = get_engine(config)
        dispose_engine(config.url)
        # After disposal, a new engine should be created
        engine2 = get_engine(config)
        assert engine is not engine2

    def test_dispose_all(self):
        """Test that dispose_all clears all engines."""
        config = ConnectionConfig(url="sqlite:///:memory:")
        get_engine(config)
        dispose_all()
        # Registry should be empty; new engine created
        engine = get_engine(config)
        assert isinstance(engine, sqlalchemy.engine.Engine)

    def test_get_connection(self):
        """Test getting a connection from an engine."""
        config = ConnectionConfig(url="sqlite:///:memory:")
        engine = get_engine(config)
        conn = get_connection(engine)
        assert isinstance(conn, sqlalchemy.engine.Connection)
        conn.close()


# ---------------------------------------------------------------------------
# ConnectionPool tests
# ---------------------------------------------------------------------------


class TestConnectionPool:
    """Tests for the ConnectionPool manager."""

    def setup_method(self):
        """Clear engine registry before each test."""
        dispose_all()

    def teardown_method(self):
        """Clear engine registry after each test."""
        dispose_all()

    def test_pool_creation(self):
        """Test ConnectionPool creation."""
        config = ConnectionConfig(url="sqlite:///:memory:")
        pool = ConnectionPool(config)
        assert pool.config is config
        assert pool._engine is None

    def test_pool_engine_property(self):
        """Test that pool.engine creates and returns an engine."""
        config = ConnectionConfig(url="sqlite:///:memory:")
        pool = ConnectionPool(config)
        engine = pool.engine
        assert isinstance(engine, sqlalchemy.engine.Engine)

    def test_pool_get_connection(self):
        """Test getting a connection from the pool."""
        config = ConnectionConfig(url="sqlite:///:memory:")
        pool = ConnectionPool(config)
        conn = pool.get_connection()
        assert isinstance(conn, sqlalchemy.engine.Connection)
        conn.close()

    def test_pool_execute(self):
        """Test executing a statement through the pool."""
        config = ConnectionConfig(url="sqlite:///:memory:")
        pool = ConnectionPool(config)
        result = pool.execute("SELECT 1 AS val")
        row = result.fetchone()
        assert row[0] == 1

    def test_pool_context_manager(self):
        """Test ConnectionPool as a context manager."""
        config = ConnectionConfig(url="sqlite:///:memory:")
        with ConnectionPool(config) as pool:
            result = pool.execute("SELECT 42 AS val")
            row = result.fetchone()
            assert row[0] == 42

    def test_pool_dispose(self):
        """Test pool disposal cleans up the engine."""
        config = ConnectionConfig(url="sqlite:///:memory:")
        pool = ConnectionPool(config)
        _ = pool.engine  # Force engine creation
        pool.dispose()
        assert pool._engine is None


# ---------------------------------------------------------------------------
# Retry logic tests
# ---------------------------------------------------------------------------


class TestRetryLogic:
    """Tests for the retry mechanism."""

    def test_retry_operation_succeeds_first_try(self):
        """Test that a successful function is not retried."""
        call_count = 0

        def succeed():
            nonlocal call_count
            call_count += 1
            return "ok"

        result = _retry_operation(succeed, retry_count=3, retry_delay=0)
        assert result == "ok"
        assert call_count == 1

    def test_retry_operation_succeeds_after_retries(self):
        """Test that a function succeeding after failures is retried."""
        call_count = 0

        def fail_then_succeed():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("temporary failure")
            return "ok"

        result = _retry_operation(
            fail_then_succeed, retry_count=5, retry_delay=0,
        )
        assert result == "ok"
        assert call_count == 3

    def test_retry_operation_exhausts_all_retries(self):
        """Test that all retries are exhausted before raising."""
        call_count = 0

        def always_fail():
            nonlocal call_count
            call_count += 1
            raise ConnectionError("persistent failure")

        with pytest.raises(ConnectionError, match="persistent failure"):
            _retry_operation(always_fail, retry_count=3, retry_delay=0)
        assert call_count == 3


# ---------------------------------------------------------------------------
# URL sanitization tests
# ---------------------------------------------------------------------------


class TestSanitizeUrl:
    """Tests for URL sanitization."""

    def test_sanitize_url_with_password(self):
        """Test that passwords are removed from URLs."""
        url = "postgresql://user:secret@localhost/db"
        sanitized = _sanitize_url(url)
        assert "secret" not in sanitized
        assert ":***@" in sanitized

    def test_sanitize_url_without_password(self):
        """Test that URLs without passwords are unchanged."""
        url = "sqlite:///test.db"
        sanitized = _sanitize_url(url)
        assert sanitized == url

    def test_sanitize_url_with_special_chars_in_password(self):
        """Test sanitization with special characters in password."""
        url = "postgresql://user:p%40ss@localhost/db"
        sanitized = _sanitize_url(url)
        assert "p%40ss" not in sanitized


# ---------------------------------------------------------------------------
# DatabaseReader / DatabaseWriter integration tests
# ---------------------------------------------------------------------------


class TestDatabaseReaderIntegration:
    """Integration tests for DatabaseReader with connection pooling."""

    def setup_method(self):
        """Set up test database."""
        dispose_all()
        self.temp_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.temp_file.close()
        self.conn_str = f"sqlite:///{self.temp_file.name}"

        # Create test data
        df = pd.DataFrame(
            {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]},
        )
        df.to_sql("test_table", self.conn_str, index=False)

    def teardown_method(self):
        """Clean up test database."""
        dispose_all()
        if os.path.exists(self.temp_file.name):
            os.unlink(self.temp_file.name)

    def test_reader_with_connection_string(self):
        """Test reading with a connection string."""
        reader = DatabaseReader()
        df = reader.read(self.conn_str, sql="SELECT * FROM test_table")
        assert len(df) == 3
        assert set(df.columns) == {"id", "name", "age"}

    def test_reader_with_engine(self):
        """Test reading with a SQLAlchemy engine."""
        engine = sqlalchemy.create_engine(self.conn_str)
        reader = DatabaseReader()
        df = reader.read(engine, sql="SELECT * FROM test_table")
        assert len(df) == 3

    def test_reader_with_connection_pool(self):
        """Test reading with a ConnectionPool."""
        config = ConnectionConfig(url=self.conn_str)
        pool = ConnectionPool(config)
        reader = DatabaseReader()
        df = reader.read(pool, sql="SELECT * FROM test_table")
        assert len(df) == 3

    def test_reader_with_table_name(self):
        """Test reading a table by name."""
        reader = DatabaseReader()
        df = reader.read(self.conn_str, table="test_table")
        assert len(df) == 3

    def test_reader_chunked(self):
        """Test chunked reading."""
        reader = DatabaseReader()
        chunks = list(
            reader.read_chunks(
                self.conn_str,
                sql="SELECT * FROM test_table ORDER BY id",
                chunk_size=2,
            )
        )
        total_rows = sum(len(c) for c in chunks)
        assert total_rows == 3

    def test_reader_invalid_source(self):
        """Test that invalid source raises ValueError."""
        reader = DatabaseReader()
        with pytest.raises(ValueError, match="Invalid source type"):
            reader.read(12345)

    def test_reader_engine_no_sql(self):
        """Test that engine without sql raises ValueError."""
        engine = sqlalchemy.create_engine(self.conn_str)
        reader = DatabaseReader()
        with pytest.raises(ValueError, match="Must provide"):
            reader.read(engine)

    def test_incremental_query(self):
        """Test incremental query builder."""
        query = DatabaseReader.incremental_query(
            table="events",
            watermark_column="updated_at",
            last_value="2024-01-01",
        )
        assert "SELECT * FROM events" in query
        assert "WHERE updated_at > '2024-01-01'" in query
        assert "ORDER BY updated_at" in query

    def test_incremental_query_with_columns(self):
        """Test incremental query with specific columns."""
        query = DatabaseReader.incremental_query(
            table="events",
            watermark_column="id",
            last_value=100,
            additional_columns=["id", "name", "created_at"],
            order_by="id",
        )
        assert "SELECT id, name, created_at FROM events" in query
        assert "WHERE id > 100" in query

    def test_read_chunks_requires_sql(self):
        """Test that read_chunks requires sql parameter."""
        reader = DatabaseReader()
        with pytest.raises(ValueError, match="Must provide 'sql' parameter"):
            list(reader.read_chunks(self.conn_str))


class TestDatabaseWriterIntegration:
    """Integration tests for DatabaseWriter with connection pooling."""

    def setup_method(self):
        """Set up test database."""
        dispose_all()
        self.temp_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.temp_file.close()
        self.conn_str = f"sqlite:///{self.temp_file.name}"

    def teardown_method(self):
        """Clean up test database."""
        dispose_all()
        if os.path.exists(self.temp_file.name):
            os.unlink(self.temp_file.name)

    def test_writer_with_connection_string(self):
        """Test writing with a connection string."""
        df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
        writer = DatabaseWriter()
        writer.write(df, self.conn_str, table_name="people")

        result = pd.read_sql("SELECT * FROM people", self.conn_str)
        assert len(result) == 2

    def test_writer_with_engine(self):
        """Test writing with a SQLAlchemy engine."""
        engine = sqlalchemy.create_engine(self.conn_str)
        df = pd.DataFrame({"id": [1], "name": ["Alice"]})
        writer = DatabaseWriter()
        writer.write(df, engine, table_name="people")

        result = pd.read_sql("SELECT * FROM people", engine)
        assert len(result) == 1

    def test_writer_with_connection_pool(self):
        """Test writing with a ConnectionPool."""
        config = ConnectionConfig(url=self.conn_str)
        pool = ConnectionPool(config)
        df = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
        writer = DatabaseWriter()
        writer.write(df, pool, table_name="items")

        result = pd.read_sql("SELECT * FROM items", self.conn_str)
        assert len(result) == 3

    def test_writer_invalid_destination(self):
        """Test that invalid destination raises ValueError."""
        writer = DatabaseWriter()
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="Invalid destination type"):
            writer.write(df, 12345, table_name="test")

    def test_writer_append(self):
        """Test appending data to an existing table."""
        df1 = pd.DataFrame({"id": [1], "name": ["Alice"]})
        df2 = pd.DataFrame({"id": [2], "name": ["Bob"]})

        writer = DatabaseWriter()
        writer.write(df1, self.conn_str, table_name="people", if_exists="replace")
        writer.write(df2, self.conn_str, table_name="people", if_exists="append")

        result = pd.read_sql("SELECT * FROM people", self.conn_str)
        assert len(result) == 2

    def test_writer_replace(self):
        """Test replacing data in an existing table."""
        df1 = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
        df2 = pd.DataFrame({"id": [3], "name": ["Charlie"]})

        writer = DatabaseWriter()
        writer.write(df1, self.conn_str, table_name="people", if_exists="replace")
        writer.write(df2, self.conn_str, table_name="people", if_exists="replace")

        result = pd.read_sql("SELECT * FROM people", self.conn_str)
        assert len(result) == 1
        assert result.iloc[0]["name"] == "Charlie"

    def test_merge_sqlite(self):
        """Test SQLite UPSERT via merge()."""
        pd.DataFrame(
            {"id": [1, 2], "name": ["Alice", "Bob"], "age": [25, 30]},
        )
        engine = sqlalchemy.create_engine(self.conn_str)
        # Create table with PRIMARY KEY for ON CONFLICT to work
        with engine.begin() as conn:
            conn.execute(sqlalchemy.text(
                "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)"
            ))
            conn.execute(sqlalchemy.text(
                "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25), (2, 'Bob', 30)"
            ))

        # Upsert: update Alice's age, insert Charlie
        df_update = pd.DataFrame(
            {"id": [1, 3], "name": ["Alice_Updated", "Charlie"], "age": [26, 35]},
        )
        writer = DatabaseWriter()
        rows = writer.merge(
            df_update,
            engine,
            table_name="users",
            key_columns=["id"],
            update_columns=["name", "age"],
        )
        assert rows > 0

        result = pd.read_sql("SELECT * FROM users ORDER BY id", engine)
        assert len(result) == 3
        assert result[result["id"] == 1]["name"].iloc[0] == "Alice_Updated"
        assert result[result["id"] == 1]["age"].iloc[0] == 26

    def test_write_chunks(self):
        """Test chunked writing."""
        df1 = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
        df2 = pd.DataFrame({"id": [3, 4], "name": ["C", "D"]})

        writer = DatabaseWriter()
        writer.write_chunks(
            iter([df1, df2]),
            self.conn_str,
            table_name="chunked",
        )

        result = pd.read_sql("SELECT * FROM chunked", self.conn_str)
        assert len(result) == 4


# ---------------------------------------------------------------------------
# Config integration tests
# ---------------------------------------------------------------------------


class TestDatabaseConfig:
    """Tests for the DatabaseConfig in config module."""

    def test_default_database_config(self):
        """Test default DatabaseConfig values."""
        from simpleetl.core.config import DatabaseConfig

        db_config = DatabaseConfig()
        assert db_config.pool_size == 5
        assert db_config.max_overflow == 10
        assert db_config.pool_timeout == 30
        assert db_config.pool_recycle == 3600
        assert db_config.ssl_mode is None
        assert db_config.connect_timeout == 10
        assert db_config.retry_count == 3
        assert db_config.retry_delay == 1.0

    def test_etl_job_config_has_database(self):
        """Test that ETLJobConfig includes database config."""
        from simpleetl.core.config import ETLJobConfig

        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="database",
        )
        assert config.database is not None
        assert config.database.pool_size == 5

    def test_etl_job_config_custom_database(self):
        """Test ETLJobConfig with custom database config."""
        from simpleetl.core.config import DatabaseConfig, ETLJobConfig

        db_config = DatabaseConfig(pool_size=20, ssl_mode="require")
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="database",
            database=db_config,
        )
        assert config.database.pool_size == 20
        assert config.database.ssl_mode == "require"
