"""
Polars engine abstraction for SimpleETL.

This module provides the engine abstraction layer that allows the
public SimpleETL API to switch between ``"pandas"`` (default) and
``"polars"`` (optional acceleration) for CSV/Parquet IO and hot-path
transformations.  The public API stays pandas-typed; ``engine`` selects
which backend executes the operation.

Features:

* an interop bridge between pandas and polars (:func:`to_polars`,
  :func:`from_polars`),
* an escape hatch for hot transformation paths
  (:func:`polars_transform`, :func:`polars_sql_transform`),
* helpers used by the CSV/Parquet IO fast paths
  (:func:`is_polars_available`, :func:`validate_engine`).

Requires the ``polars`` optional dependency::

    pip install simpleetl[polars]
"""

from typing import TYPE_CHECKING, Any, Callable, Tuple, Union

import pandas as pd

if TYPE_CHECKING:  # pragma: no cover - only for type checkers
    import polars as pl

#: Engine names accepted by the framework's engine abstraction.
VALID_ENGINES: Tuple[str, ...] = ("pandas", "polars")


def _require_polars() -> Any:
    """Import and return the ``polars`` module, lazily.

    Returns:
        The imported ``polars`` module.

    Raises:
        ImportError: If polars is not installed
            (``pip install simpleetl[polars]``).
    """
    try:
        import polars
    except ImportError as exc:
        raise ImportError(
            "polars is required for this feature. "
            "Install it with: pip install simpleetl[polars]"
        ) from exc
    return polars


def is_polars_available() -> bool:
    """Check whether the optional ``polars`` package is installed.

    Returns:
        True if polars can be imported, False otherwise.
    """
    try:
        import polars  # noqa: F401
    except ImportError:
        return False
    return True


def validate_engine(engine: str) -> str:
    """Validate an engine name for the framework's engine abstraction.

    Args:
        engine: Engine name to validate (``"pandas"`` or ``"polars"``).

    Returns:
        The validated engine name (unchanged).

    Raises:
        ValueError: If *engine* is not one of ``"pandas"`` or ``"polars"``.
    """
    if engine not in VALID_ENGINES:
        valid = ", ".join(repr(name) for name in VALID_ENGINES)
        raise ValueError(f"Unknown engine {engine!r}; expected one of {valid}.")
    return engine


def to_polars(df: pd.DataFrame) -> "pl.DataFrame":
    """Convert a pandas DataFrame to a polars DataFrame.

    Uses :func:`polars.from_pandas`, which is Arrow-backed and cheap for
    most dtypes.  The pandas index is not carried over.

    Args:
        df: Input pandas DataFrame.

    Returns:
        Equivalent polars DataFrame.

    Raises:
        ImportError: If ``polars`` is not installed
            (``pip install simpleetl[polars]``).
    """
    polars = _require_polars()
    return polars.from_pandas(df)


def from_polars(pldf: "Union[pl.DataFrame, pl.LazyFrame]") -> pd.DataFrame:
    """Convert a polars DataFrame or LazyFrame to a pandas DataFrame.

    LazyFrames are collected before conversion.

    Args:
        pldf: polars DataFrame or LazyFrame to convert.

    Returns:
        Equivalent pandas DataFrame.

    Raises:
        ImportError: If ``polars`` is not installed
            (``pip install simpleetl[polars]``).
    """
    polars = _require_polars()
    frame: Any = pldf
    if isinstance(frame, polars.LazyFrame):
        frame = frame.collect()
    return frame.to_pandas()


def polars_transform(
    df: pd.DataFrame,
    fn: "Callable[[pl.DataFrame], Union[pl.DataFrame, pl.LazyFrame]]",
) -> pd.DataFrame:
    """Apply a polars transformation function to a pandas DataFrame.

    Escape hatch for hot transformation paths: the DataFrame is converted
    to polars, *fn* is applied, and the result is converted back to
    pandas.  *fn* may return either a polars DataFrame or a LazyFrame
    (which is collected automatically).

    .. code-block:: python

        result = polars_transform(
            df,
            lambda pldf: pldf.filter(pl.col("revenue") > 0),
        )

    Args:
        df: Input pandas DataFrame.
        fn: Callable receiving a polars DataFrame and returning a polars
            DataFrame or LazyFrame.

    Returns:
        Transformed data as a new pandas DataFrame.

    Raises:
        ImportError: If ``polars`` is not installed
            (``pip install simpleetl[polars]``).
        TypeError: If *fn* returns anything other than a polars DataFrame
            or LazyFrame.
    """
    polars = _require_polars()
    result: Any = fn(to_polars(df))
    if isinstance(result, polars.LazyFrame):
        result = result.collect()
    if not isinstance(result, polars.DataFrame):
        raise TypeError(
            "polars_transform expected fn to return a polars DataFrame or "
            f"LazyFrame, got {type(result).__name__}."
        )
    return result.to_pandas()


def polars_sql_transform(
    df: pd.DataFrame,
    query: str,
    *,
    table_name: str = "df",
) -> pd.DataFrame:
    """Execute a SQL query against a DataFrame using the polars SQL engine.

    The DataFrame is registered in a :class:`polars.SQLContext` under
    *table_name* (default ``"df"``), so queries can reference it directly:

    .. code-block:: python

        result = polars_sql_transform(
            df,
            "SELECT region, SUM(revenue) AS total FROM df GROUP BY region",
        )

    Args:
        df: Input DataFrame to query.
        query: SQL query string.  Reference the DataFrame with *table_name*.
        table_name: Name used to register *df* (default: ``"df"``).

    Returns:
        Query result as a new pandas DataFrame.

    Raises:
        ImportError: If ``polars`` is not installed
            (``pip install simpleetl[polars]``).
    """
    polars = _require_polars()
    ctx = polars.SQLContext()
    ctx.register(table_name, to_polars(df))
    result = ctx.execute(query, eager=True)
    return result.to_pandas()
