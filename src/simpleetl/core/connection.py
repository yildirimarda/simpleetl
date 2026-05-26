"""
Production-grade connection management for database operations.

Provides SQLAlchemy-based connection pooling, SSL/TLS configuration,
connection timeouts, and retry logic.
"""

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# Registry of created engines keyed by connection URL
_engine_registry: Dict[str, Engine] = {}


@dataclass
class ConnectionConfig:
    """Configuration for database connection pooling and behavior.

    Attributes:
        url: Database connection URL (SQLAlchemy format).
        pool_size: Number of persistent connections in the pool.
        max_overflow: Maximum number of connections beyond pool_size.
        pool_timeout: Seconds to wait for a connection from the pool.
        pool_recycle: Seconds after which connections are recycled.
        connect_args: Additional arguments passed to the DBAPI connect() method.
        ssl_ca: Path to SSL CA certificate file.
        ssl_cert: Path to SSL client certificate file.
        ssl_key: Path to SSL client private key file.
        ssl_mode: SSL mode (e.g., 'require', 'verify-full', 'disable').
        connect_timeout: Timeout in seconds for establishing a connection.
        read_timeout: Timeout in seconds for read operations.
        write_timeout: Timeout in seconds for write operations.
        retry_count: Number of connection retry attempts.
        retry_delay: Delay in seconds between retry attempts.
    """

    url: str
    pool_size: int = 5
    max_overflow: int = 10
    pool_timeout: int = 30
    pool_recycle: int = 3600
    connect_args: Dict[str, Any] = field(default_factory=dict)
    ssl_ca: Optional[str] = None
    ssl_cert: Optional[str] = None
    ssl_key: Optional[str] = None
    ssl_mode: Optional[str] = None
    connect_timeout: int = 10
    read_timeout: int = 30
    write_timeout: int = 30
    retry_count: int = 3
    retry_delay: float = 1.0

    def to_engine_kwargs(self) -> Dict[str, Any]:
        """Build keyword arguments for sqlalchemy.create_engine.

        SQLite uses SingletonThreadPool which does not support pool_size,
        max_overflow, or pool_timeout. Those arguments are skipped for SQLite.
        """
        kwargs: Dict[str, Any] = {}

        # SQLite uses SingletonThreadPool -- it does not support standard
        # pool tuning arguments. Only pass pool args for non-SQLite dialects.
        if not self.url.startswith("sqlite"):
            kwargs["pool_size"] = self.pool_size
            kwargs["max_overflow"] = self.max_overflow
            kwargs["pool_timeout"] = self.pool_timeout

        kwargs["pool_recycle"] = self.pool_recycle

        # Merge connect_args with timeout settings
        connect_args = dict(self.connect_args)

        # Build SSL arguments
        ssl_args = self._build_ssl_args()
        if ssl_args:
            connect_args["ssl"] = ssl_args

        # Apply connect_timeout if the driver supports it.
        # SQLite (pysqlite) does not accept connect_timeout.
        if (
            self.connect_timeout
            and "connect_timeout" not in connect_args
            and not self.url.startswith("sqlite")
        ):
            connect_args["connect_timeout"] = self.connect_timeout

        if connect_args:
            kwargs["connect_args"] = connect_args

        return kwargs

    def _build_ssl_args(self) -> Dict[str, str]:
        """Build SSL configuration dictionary for the database driver."""
        ssl_args: Dict[str, str] = {}

        if self.ssl_mode:
            ssl_args["sslmode"] = self.ssl_mode
        if self.ssl_ca:
            ssl_args["sslca"] = self.ssl_ca
        if self.ssl_cert:
            ssl_args["sslcert"] = self.ssl_cert
        if self.ssl_key:
            ssl_args["sslkey"] = self.ssl_key

        return ssl_args


class ConnectionPool:
    """SQLAlchemy-based connection pool manager.

    Wraps engine creation, connection retrieval, and cleanup with
    support for SSL, timeouts, and retry logic.

    Example::

        config = ConnectionConfig(
            url="postgresql://user:pass@localhost/db",
            pool_size=5,
            ssl_mode="require",
        )
        pool = ConnectionPool(config)
        with pool.get_connection() as conn:
            result = conn.execute(text("SELECT 1"))
    """

    def __init__(self, config: ConnectionConfig):
        self.config = config
        self._engine: Optional[Engine] = None

    @property
    def engine(self) -> Engine:
        """Get or create the SQLAlchemy engine."""
        if self._engine is None:
            self._engine = get_engine(self.config)
        return self._engine

    def get_connection(self):
        """Get a connection from the pool.

        Returns:
            A SQLAlchemy Connection context manager.
        """
        return _retry_operation(
            self.engine.connect,
            retry_count=self.config.retry_count,
            retry_delay=self.config.retry_delay,
        )

    def execute(self, statement: Any, parameters: Optional[Dict] = None):
        """Execute a SQL statement with retry logic.

        Args:
            statement: A SQLAlchemy text() construct or SQL string.
            parameters: Optional bind parameters.

        Returns:
            The result of the execution.
        """
        if isinstance(statement, str):
            statement = text(statement)

        def _execute():
            with self.get_connection() as conn:
                return conn.execute(statement, parameters or {})

        return _retry_operation(
            _execute,
            retry_count=self.config.retry_count,
            retry_delay=self.config.retry_delay,
        )

    def dispose(self):
        """Dispose of the engine and its connection pool."""
        if self._engine is not None:
            dispose_engine(self.config.url)
            self._engine = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.dispose()
        return False


def get_engine(config: ConnectionConfig) -> Engine:
    """Create or return a cached SQLAlchemy engine with connection pooling.

    Engines are cached by connection URL to avoid creating duplicate pools
    for the same database.

    Args:
        config: Connection configuration.

    Returns:
        A SQLAlchemy Engine instance.
    """
    global _engine_registry

    if config.url in _engine_registry:
        logger.debug("Returning cached engine for %s", _sanitize_url(config.url))
        return _engine_registry[config.url]

    logger.info("Creating new engine for %s", _sanitize_url(config.url))
    kwargs = config.to_engine_kwargs()
    engine = create_engine(config.url, **kwargs)

    _engine_registry[config.url] = engine
    return engine


def get_connection(engine: Engine):
    """Get a connection from the engine's pool.

    Args:
        engine: A SQLAlchemy Engine instance.

    Returns:
        A SQLAlchemy Connection context manager.
    """
    return engine.connect()


def dispose_engine(url: str) -> None:
    """Dispose of the engine and its connection pool for the given URL.

    Args:
        url: The connection URL whose engine should be disposed.
    """
    global _engine_registry

    if url in _engine_registry:
        logger.info("Disposing engine for %s", _sanitize_url(url))
        engine = _engine_registry.pop(url)
        engine.dispose()


def dispose_all() -> None:
    """Dispose of all cached engines."""
    global _engine_registry

    for url, engine in _engine_registry.items():
        logger.info("Disposing engine for %s", _sanitize_url(url))
        engine.dispose()
    _engine_registry.clear()


def _retry_operation(func, retry_count: int = 3, retry_delay: float = 1.0):
    """Execute a function with retry logic.

    Args:
        func: The callable to execute.
        retry_count: Maximum number of attempts.
        retry_delay: Seconds to wait between retries.

    Returns:
        The return value of the function.

    Raises:
        The last exception if all retries are exhausted.
    """
    last_exception: Exception | None = None

    for attempt in range(1, retry_count + 1):
        try:
            return func()
        except Exception as exc:
            last_exception = exc
            if attempt < retry_count:
                logger.warning(
                    "Attempt %d/%d failed: %s. Retrying in %.1fs...",
                    attempt,
                    retry_count,
                    exc,
                    retry_delay,
                )
                time.sleep(retry_delay)
            else:
                logger.error(
                    "All %d attempts failed. Last error: %s",
                    retry_count,
                    exc,
                )

    raise last_exception  # type: ignore[misc]


def _sanitize_url(url: str) -> str:
    """Remove credentials from a connection URL for safe logging.

    Args:
        url: A database connection URL.

    Returns:
        The URL with the password replaced by '***'.
    """
    try:
        parsed = urlparse(url)
        if parsed.password:
            return url.replace(f":{parsed.password}@", ":***@")
    except Exception:
        pass
    return url
