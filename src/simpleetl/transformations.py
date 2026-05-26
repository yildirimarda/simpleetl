"""
Transformation functions for ETL jobs.

This module provides a comprehensive set of production-grade DataFrame
transformation functions for use in ETL pipelines. All functions accept
and return pandas DataFrames and use vectorized operations for efficiency.
"""

import pandas as pd
from typing import Union, List, Dict, Any, Callable, Optional, Tuple


def filter_data(
    df: pd.DataFrame,
    filter_func: Optional[Callable[[pd.Series], bool]] = None,
    column: Optional[str] = None,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
) -> pd.DataFrame:
    """
    Filter a DataFrame based on conditions.

    Args:
        df: Input DataFrame
        filter_func: Function that takes a Series and returns a boolean
        column: Column name to filter on (used with min_value/max_value)
        min_value: Minimum value (inclusive) for the column. If None, no lower bound.
        max_value: Maximum value (inclusive) for the column. If None, no upper bound.

    Returns:
        Filtered DataFrame
    """
    if column is not None:
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in DataFrame")

        filtered_df = df.copy()
        if min_value is not None:
            filtered_df = filtered_df[filtered_df[column] >= min_value]
        if max_value is not None:
            filtered_df = filtered_df[filtered_df[column] <= max_value]
        return filtered_df
    elif filter_func is not None:
        # Use the function to filter
        mask = df.apply(filter_func, axis=1)
        return df[mask].copy()
    else:
        raise ValueError("Either filter_func or column must be provided")


def map_values(
    df: pd.DataFrame,
    column: str,
    mapping: Union[Dict[Any, Any], Callable[[Any], Any]],
) -> pd.DataFrame:
    """
    Map values in a column using a dictionary or a function.

    Args:
        df: Input DataFrame
        column: Column name to map
        mapping: Either a dictionary mapping old values to new values,
                 or a function that takes a value and returns a new value.

    Returns:
        DataFrame with mapped column
    """
    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found in DataFrame")

    result_df = df.copy()
    if isinstance(mapping, dict):
        result_df[column] = result_df[column].map(mapping)
    elif callable(mapping):
        result_df[column] = result_df[column].apply(mapping)
    else:
        raise TypeError("Mapping must be a dictionary or a callable function")

    return result_df


def aggregate_data(
    df: pd.DataFrame,
    groupby: Union[str, List[str]],
    agg: Dict[str, Union[str, List[str], Callable]],
) -> pd.DataFrame:
    """
    Aggregate data using groupby and aggregation specifications.

    Args:
        df: Input DataFrame
        groupby: Column name or list of column names to group by
        agg: Dictionary specifying aggregations.
             Keys are column names, values are aggregation specifications
             (e.g., 'sum', 'mean', [min, max], or a custom function).

    Returns:
        Aggregated DataFrame
    """
    # Check that groupby columns exist
    if isinstance(groupby, str):
        groupby_cols = [groupby]
    else:
        groupby_cols = groupby

    for col in groupby_cols:
        if col not in df.columns:
            raise ValueError(f"Groupby column '{col}' not found in DataFrame")

    # Check that agg columns exist
    for col in agg.keys():
        if col not in df.columns:
            raise ValueError(f"Aggregation column '{col}' not found in DataFrame")

    # Perform groupby and aggregation
    result_df = df.groupby(groupby_cols).agg(agg).reset_index()

    # Flatten column MultiIndex if present (from multiple agg functions)
    if isinstance(result_df.columns, pd.MultiIndex):
        # Create new column names by joining levels with underscore, skipping empty levels
        new_columns = []
        for col in result_df.columns:
            if isinstance(col, tuple):
                # Filter out empty strings and join with underscore
                parts = [str(part) for part in col if part != '']
                new_col = '_'.join(parts) if parts else ''
                new_columns.append(new_col)
            else:
                new_columns.append(str(col))
        result_df.columns = new_columns

    return result_df


def join_data(
    left: pd.DataFrame,
    right: pd.DataFrame,
    on: Optional[Union[str, List[str]]] = None,
    left_on: Optional[Union[str, List[str]]] = None,
    right_on: Optional[Union[str, List[str]]] = None,
    how: str = "inner",
    suffix: str = "_right",
) -> pd.DataFrame:
    """
    Join two DataFrames together.

    Args:
        left: Left DataFrame.
        right: Right DataFrame.
        on: Column name(s) to join on. Must exist in both DataFrames.
            If None, left_on and right_on must be provided.
        left_on: Column name(s) in the left DataFrame to join on.
        right_on: Column name(s) in the right DataFrame to join on.
        how: Type of join - 'inner', 'left', 'right', 'outer', or 'cross'.
        suffix: Suffix to append to overlapping column names from the right DataFrame.

    Returns:
        Joined DataFrame.

    Raises:
        ValueError: If join columns are missing or an invalid join type is specified.
    """
    valid_joins = ("inner", "left", "right", "outer", "cross")
    if how not in valid_joins:
        raise ValueError(f"Invalid join type '{how}'. Must be one of {valid_joins}")

    if on is None and (left_on is None or right_on is None):
        raise ValueError("Either 'on' or both 'left_on' and 'right_on' must be provided")

    # Validate columns exist
    if on is not None:
        on_cols = [on] if isinstance(on, str) else list(on)
        for col in on_cols:
            if col not in left.columns:
                raise ValueError(f"Join column '{col}' not found in left DataFrame")
            if col not in right.columns:
                raise ValueError(f"Join column '{col}' not found in right DataFrame")
    else:
        assert left_on is not None and right_on is not None
        left_cols = [left_on] if isinstance(left_on, str) else list(left_on)
        right_cols = [right_on] if isinstance(right_on, str) else list(right_on)
        for col in left_cols:
            if col not in left.columns:
                raise ValueError(f"Join column '{col}' not found in left DataFrame")
        for col in right_cols:
            if col not in right.columns:
                raise ValueError(f"Join column '{col}' not found in right DataFrame")

    kwargs: Dict[str, Any] = {"how": how}
    if on is not None:
        kwargs["on"] = on
    else:
        kwargs["left_on"] = left_on
        kwargs["right_on"] = right_on

    if how != "cross":
        kwargs["suffixes"] = ("", suffix)

    return pd.merge(left, right, **kwargs)


def union_data(
    df: pd.DataFrame,
    other: pd.DataFrame,
    ignore_index: bool = True,
) -> pd.DataFrame:
    """
    Concatenate two DataFrames vertically with schema alignment.

    Handles mismatched columns by filling missing columns with NaN,
    similar to a SQL UNION ALL operation.

    Args:
        df: First DataFrame.
        other: Second DataFrame to append.
        ignore_index: If True, reset the index in the result. Defaults to True.

    Returns:
        Concatenated DataFrame containing all rows from both inputs.

    Examples:
        >>> df1 = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        >>> df2 = pd.DataFrame({"a": [5], "c": [6]})
        >>> union_data(df1, df2)
           a    b    c
        0  1  3.0  NaN
        1  2  4.0  NaN
        2  5  NaN  6.0
    """
    return pd.concat([df, other], ignore_index=ignore_index)


def deduplicate_data(
    df: pd.DataFrame,
    subset: Optional[List[str]] = None,
    keep: Union[str, bool] = "first",
) -> pd.DataFrame:
    """
    Remove duplicate rows from a DataFrame.

    Args:
        df: Input DataFrame.
        subset: Column names to consider for identifying duplicates.
                If None, all columns are used.
        keep: Which duplicates to keep:
              'first' - Keep the first occurrence.
              'last' - Keep the last occurrence.
              False - Drop all duplicates.

    Returns:
        DataFrame with duplicates removed.

    Raises:
        ValueError: If subset columns are not found in the DataFrame.
    """
    if subset is not None:
        missing = [col for col in subset if col not in df.columns]
        if missing:
            raise ValueError(f"Columns not found in DataFrame: {missing}")

    return df.drop_duplicates(subset=subset, keep=keep).reset_index(drop=True)  # type: ignore[call-overload]


def with_column(
    df: pd.DataFrame,
    name: str,
    value: Union[Any, pd.Series, Callable[[pd.DataFrame], Union[Any, pd.Series]]],
) -> pd.DataFrame:
    """
    Add or replace a computed column in a DataFrame.

    Args:
        df: Input DataFrame.
        name: Name of the new or existing column.
        value: The value to assign. Can be:
               - A scalar (broadcast to all rows)
               - A pd.Series (must match DataFrame length)
               - A callable that takes the DataFrame and returns a scalar or Series

    Returns:
        DataFrame with the new column added or replaced.

    Raises:
        ValueError: If a Series value length does not match the DataFrame length.
    """
    result_df = df.copy()
    if callable(value) and not isinstance(value, pd.Series):
        computed = value(result_df)
        result_df[name] = computed
    elif isinstance(value, pd.Series):
        if len(value) != len(df):
            raise ValueError(
                f"Series length ({len(value)}) does not match "
                f"DataFrame length ({len(df)})"
            )
        result_df[name] = value.values
    else:
        result_df[name] = value
    return result_df


def rename_columns(
    df: pd.DataFrame,
    mapping: Dict[str, str],
) -> pd.DataFrame:
    """
    Rename columns using a mapping dictionary.

    Args:
        df: Input DataFrame.
        mapping: Dictionary mapping old column names to new column names.
                 Columns not in the mapping are left unchanged.

    Returns:
        DataFrame with renamed columns.

    Raises:
        ValueError: If any source column in the mapping is not found.
    """
    missing = [col for col in mapping.keys() if col not in df.columns]
    if missing:
        raise ValueError(f"Columns not found in DataFrame: {missing}")
    return df.rename(columns=mapping)


def select_columns(
    df: pd.DataFrame,
    columns: List[str],
) -> pd.DataFrame:
    """
    Select specific columns from a DataFrame, reordering them as specified.

    Args:
        df: Input DataFrame.
        columns: List of column names to select, in desired order.

    Returns:
        DataFrame containing only the specified columns.

    Raises:
        ValueError: If any specified column is not found.
    """
    missing = [col for col in columns if col not in df.columns]
    if missing:
        raise ValueError(f"Columns not found in DataFrame: {missing}")
    return df[columns].copy()


def drop_columns(
    df: pd.DataFrame,
    columns: Union[str, List[str]],
) -> pd.DataFrame:
    """
    Drop columns from a DataFrame by name.

    Args:
        df: Input DataFrame.
        columns: Column name or list of column names to drop.

    Returns:
        DataFrame with specified columns removed.

    Raises:
        ValueError: If any specified column is not found.
    """
    if isinstance(columns, str):
        columns = [columns]
    missing = [col for col in columns if col not in df.columns]
    if missing:
        raise ValueError(f"Columns not found in DataFrame: {missing}")
    return df.drop(columns=columns)


def fill_na(
    df: pd.DataFrame,
    value: Union[Any, Dict[str, Any]],
    subset: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Fill NaN values in a DataFrame.

    Args:
        df: Input DataFrame:
        value: Value to use for filling. Can be:
               - A scalar: fills all NaN values (or those in subset columns)
               - A dict: maps column names to fill values
        subset: Column names to limit the fill operation to.
                If None and value is a scalar, all columns are filled.

    Returns:
        DataFrame with NaN values filled.

    Raises:
        ValueError: If subset columns or dict keys are not found.
    """
    result_df = df.copy()

    if isinstance(value, dict):
        missing = [col for col in value.keys() if col not in df.columns]
        if missing:
            raise ValueError(f"Columns not found in DataFrame: {missing}")
        result_df = result_df.fillna(value)
    elif subset is not None:
        missing = [col for col in subset if col not in df.columns]
        if missing:
            raise ValueError(f"Columns not found in DataFrame: {missing}")
        result_df[subset] = result_df[subset].fillna(value)
    else:
        result_df = result_df.fillna(value)

    return result_df


def drop_na(
    df: pd.DataFrame,
    subset: Optional[List[str]] = None,
    how: str = "any",
) -> pd.DataFrame:
    """
    Drop rows containing NaN values.

    Args:
        df: Input DataFrame.
        subset: Column names to consider when looking for NaN.
                If None, all columns are considered.
        how: 'any' drops rows with any NaN in the considered columns.
             'all' drops rows where all considered columns are NaN.

    Returns:
        DataFrame with NaN rows dropped.

    Raises:
        ValueError: If subset columns are not found or how is invalid.
    """
    if how not in ("any", "all"):
        raise ValueError(f"Invalid value '{how}' for 'how'. Must be 'any' or 'all'")

    if subset is not None:
        missing = [col for col in subset if col not in df.columns]
        if missing:
            raise ValueError(f"Columns not found in DataFrame: {missing}")

    return df.dropna(subset=subset, how=how).reset_index(drop=True)  # type: ignore[call-overload]


def sort_data(
    df: pd.DataFrame,
    by: Union[str, List[str]],
    ascending: Union[bool, List[bool]] = True,
) -> pd.DataFrame:
    """
    Sort a DataFrame by one or more columns.

    Args:
        df: Input DataFrame.
        by: Column name or list of column names to sort by.
        ascending: Sort order. If True, ascending; if False, descending.
                   Can be a single bool or a list matching the length of 'by'.

    Returns:
        Sorted DataFrame.

    Raises:
        ValueError: If any sort column is not found.
    """
    sort_cols = [by] if isinstance(by, str) else list(by)
    missing = [col for col in sort_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Columns not found in DataFrame: {missing}")

    return df.sort_values(by=by, ascending=ascending).reset_index(drop=True)


def limit_rows(
    df: pd.DataFrame,
    n: int,
) -> pd.DataFrame:
    """
    Limit the number of rows in a DataFrame.

    Args:
        df: Input DataFrame.
        n: Maximum number of rows to return.

    Returns:
        DataFrame with at most n rows.
    """
    if n < 0:
        raise ValueError(f"n must be non-negative, got {n}")
    return df.head(n).reset_index(drop=True)


def sample_data(
    df: pd.DataFrame,
    n: Optional[int] = None,
    frac: Optional[float] = None,
    seed: Optional[int] = None,
) -> pd.DataFrame:
    """
    Return a random sample of rows from a DataFrame.

    Args:
        df: Input DataFrame.
        n: Number of rows to sample. Cannot be used with frac.
        frac: Fraction of rows to sample (0.0 to 1.0). Cannot be used with n.
        seed: Random seed for reproducibility.

    Returns:
        Sampled DataFrame.

    Raises:
        ValueError: If both n and frac are provided, or neither is provided.
    """
    if n is not None and frac is not None:
        raise ValueError("Only one of 'n' or 'frac' should be provided, not both")
    if n is None and frac is None:
        raise ValueError("Either 'n' or 'frac' must be provided")

    return df.sample(n=n, frac=frac, random_state=seed).reset_index(drop=True)


def distinct_data(
    df: pd.DataFrame,
    subset: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Return distinct rows across all columns or a subset of columns.

    This is equivalent to SELECT DISTINCT in SQL.

    Args:
        df: Input DataFrame.
        subset: Column names to determine distinctness.
                If None, all columns are used.

    Returns:
        DataFrame with only distinct rows.

    Raises:
        ValueError: If subset columns are not found.
    """
    if subset is not None:
        missing = [col for col in subset if col not in df.columns]
        if missing:
            raise ValueError(f"Columns not found in DataFrame: {missing}")

    return df.drop_duplicates(subset=subset).reset_index(drop=True)


def cast_columns(
    df: pd.DataFrame,
    type_map: Dict[str, Union[str, type]],
    errors: str = "raise",
) -> pd.DataFrame:
    """
    Cast columns to specified data types.

    Args:
        df: Input DataFrame.
        type_map: Dictionary mapping column names to target dtypes.
                  Values can be numpy dtype strings (e.g., 'int64', 'float32')
                  or Python types (e.g., int, float, str).
        errors: How to handle conversion errors:
                'raise' - raise an exception on error (default)
                'coerce' - set invalid values to NaN

    Returns:
        DataFrame with columns cast to the specified types.

    Raises:
        ValueError: If columns are not found or errors value is invalid.
    """
    if errors not in ("raise", "coerce"):
        raise ValueError(f"Invalid value '{errors}' for 'errors'. Must be 'raise' or 'coerce'")

    missing = [col for col in type_map.keys() if col not in df.columns]
    if missing:
        raise ValueError(f"Columns not found in DataFrame: {missing}")

    result_df = df.copy()
    for col, dtype in type_map.items():
        if errors == "coerce":
            cast_dtype: Any = dtype
            result_df[col] = pd.to_numeric(
                result_df[col], errors="coerce"
            ).astype(cast_dtype) if dtype in (
                int, float, "int64", "float64", "int32", "float32"
            ) else result_df[col].astype(cast_dtype)
        else:
            cast_dtype = dtype
            result_df[col] = result_df[col].astype(cast_dtype)

    return result_df


def when_otherwise(
    df: pd.DataFrame,
    conditions: List[Tuple[pd.Series, Any]],
    otherwise_value: Any,
    output_col: str,
) -> pd.DataFrame:
    """
    Create a column with conditional values, similar to SQL CASE WHEN.

    Evaluates conditions in order and assigns the corresponding value
    for the first matching condition. If no conditions match, assigns
    the otherwise_value.

    Args:
        df: Input DataFrame.
        conditions: List of (condition, value) tuples where condition is a
                    boolean Series and value is the value to assign when
                    the condition is True. Evaluated in order.
        otherwise_value: Value to assign when no conditions match.
        output_col: Name of the output column.

    Returns:
        DataFrame with the new conditional column.

    Raises:
        ValueError: If any condition Series length does not match the DataFrame.
    """
    result_df = df.copy()
    result_df[output_col] = otherwise_value

    # Apply conditions in reverse order so the first condition takes precedence
    # (np.select applies first-match-wins, but we build from the end for simple assignment)
    for condition, value in reversed(conditions):
        if len(condition) != len(df):
            raise ValueError(
                f"Condition Series length ({len(condition)}) does not match "
                f"DataFrame length ({len(df)})"
            )
        result_df.loc[condition, output_col] = value

    return result_df


def add_computed_column(
    df: pd.DataFrame,
    expression: str,
    output_col: str,
) -> pd.DataFrame:
    """
    Evaluate an expression string to create a new column using pd.eval.

    The expression can reference any column in the DataFrame by name.

    Args:
        df: Input DataFrame.
        expression: A string expression using column names as variables.
                    Example: "col_a + col_b * 2"
        output_col: Name of the new column to create.

    Returns:
        DataFrame with the computed column added.

    Raises:
        ValueError: If the expression cannot be evaluated.
    """
    result_df = df.copy()
    try:
        result_df[output_col] = pd.eval(expression, local_dict=df)  # type: ignore[arg-type]
    except Exception as e:
        raise ValueError(f"Failed to evaluate expression '{expression}': {e}") from e
    return result_df


def group_by_aggregate_data(
    df: pd.DataFrame,
    group_by: Union[str, List[str]],
    agg: Dict[str, Union[str, List[str], Callable, Dict[str, Union[str, Callable]]]],
) -> pd.DataFrame:
    """
    Enhanced groupby aggregation with multiple aggregation functions per column.

    Supports specifying different aggregation functions for different columns,
    including named aggregations.

    Args:
        df: Input DataFrame.
        group_by: Column name or list of column names to group by.
        agg: Dictionary specifying aggregations. Keys are column names.
             Values can be:
             - A single function: 'sum', 'mean', np.sum, etc.
             - A list of functions: ['sum', 'mean', 'count']
             - A dict for named output columns: {'total': 'sum', 'avg': 'mean'}

    Returns:
        Aggregated DataFrame with flattened column names.

    Raises:
        ValueError: If groupby or agg columns are not found.
    """
    group_cols = [group_by] if isinstance(group_by, str) else list(group_by)

    for col in group_cols:
        if col not in df.columns:
            raise ValueError(f"Groupby column '{col}' not found in DataFrame")

    for col in agg.keys():
        if col not in df.columns:
            raise ValueError(f"Aggregation column '{col}' not found in DataFrame")

    result_df = df.groupby(group_cols).agg(agg).reset_index()  # type: ignore[arg-type]

    # Flatten MultiIndex columns
    if isinstance(result_df.columns, pd.MultiIndex):
        new_columns = []
        for col in result_df.columns:
            if isinstance(col, tuple):
                parts = [str(part) for part in col if part != ""]
                new_columns.append("_".join(parts))
            else:
                new_columns.append(str(col))
        result_df.columns = new_columns

    return result_df


def pivot_data(
    df: pd.DataFrame,
    index: Union[str, List[str]],
    columns: str,
    values: Union[str, List[str]],
    agg_func: str = "first",
) -> pd.DataFrame:
    """
    Pivot a DataFrame from long to wide format.

    Args:
        df: Input DataFrame.
        index: Column(s) to use as the index (row identifiers).
        columns: Column whose unique values become new column headers.
        values: Column(s) to use as the values in the pivoted table.
        agg_func: Aggregation function to use when there are duplicate
                  entries for an index/column combination.
                  Default is 'first'.

    Returns:
        Pivoted DataFrame.

    Raises:
        ValueError: If any specified column is not found.
    """
    index_cols = [index] if isinstance(index, str) else list(index)
    values_cols = [values] if isinstance(values, str) else list(values)

    all_cols = index_cols + [columns] + values_cols
    missing = [col for col in all_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Columns not found in DataFrame: {missing}")

    result = df.pivot_table(
        index=index_cols,
        columns=columns,
        values=values_cols,
        aggfunc=agg_func,  # type: ignore[arg-type]
    ).reset_index()

    # Flatten MultiIndex columns if values was a list
    if isinstance(result.columns, pd.MultiIndex):
        new_columns = []
        for col in result.columns:
            if isinstance(col, tuple):
                parts = [str(part) for part in col if part != ""]
                new_columns.append("_".join(parts))
            else:
                new_columns.append(str(col))
        result.columns = new_columns

    return result


def unpivot_data(
    df: pd.DataFrame,
    id_vars: Union[str, List[str]],
    value_vars: Optional[Union[str, List[str]]] = None,
    var_name: str = "variable",
    value_name: str = "value",
) -> pd.DataFrame:
    """
    Unpivot a DataFrame from wide to long format (melt).

    Args:
        df: Input DataFrame.
        id_vars: Column(s) to use as identifier variables (kept as columns).
        value_vars: Column(s) to unpivot. If None, uses all columns
                    not in id_vars.
        var_name: Name for the 'variable' column. Default is 'variable'.
        value_name: Name for the 'value' column. Default is 'value'.

    Returns:
        Unpivoted (melted) DataFrame.

    Raises:
        ValueError: If any specified column is not found.
    """
    id_cols = [id_vars] if isinstance(id_vars, str) else list(id_vars)
    missing = [col for col in id_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Columns not found in DataFrame: {missing}")

    if value_vars is not None:
        val_cols = [value_vars] if isinstance(value_vars, str) else list(value_vars)
        missing = [col for col in val_cols if col not in df.columns]
        if missing:
            raise ValueError(f"Columns not found in DataFrame: {missing}")
    else:
        val_cols = None

    return df.melt(
        id_vars=id_cols,
        value_vars=val_cols,
        var_name=var_name,
        value_name=value_name,
    )


def transform_chain(
    df: pd.DataFrame,
    steps: List[Tuple[Callable[..., pd.DataFrame], Dict[str, Any]]],
) -> pd.DataFrame:
    """
    Apply a sequence of transformation functions to a DataFrame.

    Each step is a tuple of (function, kwargs_dict). The output of each
    step is passed as the first positional argument to the next step.

    Args:
        df: Input DataFrame.
        steps: List of (function, kwargs) tuples to apply sequentially.
               Each function must accept a DataFrame as its first argument
               and return a DataFrame.

    Returns:
        Transformed DataFrame after all steps have been applied.

    Raises:
        ValueError: If any step function does not return a DataFrame.

    Examples:
        >>> steps = [
        ...     (filter_data, {"column": "age", "min_value": 18}),
        ...     (select_columns, {"columns": ["name", "age"]}),
        ...     (sort_data, {"by": "name"}),
        ... ]
        >>> result = transform_chain(df, steps)
    """
    result = df
    for func, kwargs in steps:
        result = func(result, **kwargs)
        if not isinstance(result, pd.DataFrame):
            raise ValueError(
                f"Step function '{func.__name__}' did not return a DataFrame"
            )
    return result


def window_functions(
    df: pd.DataFrame,
    partition_by: Union[str, List[str]],
    order_by: Union[str, List[str]],
    functions: Dict[str, Dict[str, Any]],
) -> pd.DataFrame:
    """
    Apply window functions to a DataFrame, similar to SQL window functions.

    Partitions the data by the specified columns, orders within each partition,
    and computes window functions on each partition.

    Args:
        df: Input DataFrame.
        partition_by: Column name or list of column names to partition by.
        order_by: Column name or list of column names to order by within partitions.
        functions: Dictionary mapping output column names to function specs.
                   Each spec is a dict with:
                   - 'function': one of 'rank', 'dense_rank', 'lag', 'lead',
                     'row_number', 'percent_rank', 'cumsum', 'cume_dist'
                   - 'column': the column to operate on (required for lag/lead/cumsum)
                   - 'offset': offset for lag/lead (default 1)
                   - 'fill_value': fill value for lag/lead (default None)

    Returns:
        DataFrame with the new window function columns added.

    Raises:
        ValueError: If partition/order columns are missing or an invalid function is specified.

    Examples:
        >>> df = pd.DataFrame({
        ...     'dept': ['A', 'A', 'B', 'B'],
        ...     'salary': [50000, 60000, 45000, 55000]
        ... })
        >>> result = window_functions(df, 'dept', 'salary', {
        ...     'sal_rank': {'function': 'rank'},
        ...     'prev_sal': {'function': 'lag', 'column': 'salary', 'offset': 1}
        ... })
    """
    VALID_FUNCTIONS = {
        'rank', 'dense_rank', 'lag', 'lead', 'row_number',
        'percent_rank', 'cumsum', 'cume_dist',
    }

    # Validate partition and order columns
    partition_cols = [partition_by] if isinstance(partition_by, str) else list(partition_by)
    order_cols = [order_by] if isinstance(order_by, str) else list(order_by)

    for col in partition_cols:
        if col not in df.columns:
            raise ValueError(f"Partition column '{col}' not found in DataFrame")
    for col in order_cols:
        if col not in df.columns:
            raise ValueError(f"Order column '{col}' not found in DataFrame")

    # Validate function specs
    for out_col, spec in functions.items():
        func_name = spec.get('function')
        if func_name not in VALID_FUNCTIONS:
            raise ValueError(
                f"Invalid window function '{func_name}'. Must be one of {sorted(VALID_FUNCTIONS)}"
            )
        if func_name in ('lag', 'lead', 'cumsum') and 'column' not in spec:
            raise ValueError(
                f"Window function '{func_name}' requires a 'column' key in its spec"
            )

    result_df = df.copy()

    # Sort the DataFrame by partition + order columns to ensure correct window computation
    sorted_df = result_df.sort_values(partition_cols + order_cols).reset_index(drop=True)

    # Group by partition columns
    grouped = sorted_df.groupby(partition_cols, sort=False)

    for out_col, spec in functions.items():
        func_name = spec['function']

        if func_name == 'rank':
            sorted_df[out_col] = grouped[order_cols[0]].rank(
                method='min', ascending=True, pct=False
            )
        elif func_name == 'dense_rank':
            sorted_df[out_col] = grouped[order_cols[0]].rank(
                method='dense', ascending=True
            )
        elif func_name == 'row_number':
            sorted_df[out_col] = grouped.cumcount() + 1
        elif func_name == 'percent_rank':
            sorted_df[out_col] = grouped[order_cols[0]].rank(
                method='min', ascending=True, pct=True
            )
        elif func_name == 'lag':
            offset = spec.get('offset', 1)
            fill_value = spec.get('fill_value', None)
            sorted_df[out_col] = grouped[spec['column']].shift(
                offset, fill_value=fill_value
            )
        elif func_name == 'lead':
            offset = spec.get('offset', 1)
            fill_value = spec.get('fill_value', None)
            sorted_df[out_col] = grouped[spec['column']].shift(
                -offset, fill_value=fill_value
            )
        elif func_name == 'cumsum':
            sorted_df[out_col] = grouped[spec['column']].cumsum()
        elif func_name == 'cume_dist':
            # cume_dist = rank / count
            counts = grouped[order_cols[0]].transform('count')
            ranks = grouped[order_cols[0]].rank(method='min')
            sorted_df[out_col] = ranks / counts

    # Restore original order using the index
    result_df = sorted_df.sort_index().reset_index(drop=True)

    return result_df


def string_operations(
    df: pd.DataFrame,
    column: str,
    operation: str,
    **kwargs: Any,
) -> pd.DataFrame:
    """
    Apply string operations to a column in a DataFrame.

    All operations use vectorized pandas string methods for efficiency.

    Args:
        df: Input DataFrame.
        column: Column name to operate on.
        operation: One of 'trim', 'upper', 'lower', 'replace', 'split',
                   'contains', 'regex_extract', 'length', 'substring',
                   'pad_left', 'pad_right'.
        **kwargs: Additional arguments depending on the operation:
                  - replace: old (str), new (str)
                  - split: sep (str), maxsplit (int, default -1), expand (bool, default False)
                  - contains: pattern (str), case (bool, default True)
                  - regex_extract: pattern (str), group (int, default 0)
                  - substring: start (int), stop (int, step (int))
                  - pad_left/pad_right: width (int), fillchar (str, default ' ')

    Returns:
        DataFrame with the string operation applied. For 'split' with expand=True,
        new columns are added. For 'contains', a boolean column is added.
        For all other operations, the original column is replaced.

    Raises:
        ValueError: If the column is not found or the operation is invalid.

    Examples:
        >>> df = pd.DataFrame({'name': ['  Alice  ', 'BOB', 'Charlie']})
        >>> string_operations(df, 'name', 'trim')
           name
        0  Alice
        1    BOB
        2  Charlie
    """
    VALID_OPERATIONS = {
        'trim', 'upper', 'lower', 'replace', 'split',
        'contains', 'regex_extract', 'length', 'substring',
        'pad_left', 'pad_right',
    }

    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found in DataFrame")
    if operation not in VALID_OPERATIONS:
        raise ValueError(
            f"Invalid string operation '{operation}'. Must be one of {sorted(VALID_OPERATIONS)}"
        )

    result_df = df.copy()
    col_series = result_df[column].astype(str)
    str_acc = col_series.str

    if operation == 'trim':
        result_df[column] = str_acc.strip()
    elif operation == 'upper':
        result_df[column] = str_acc.upper()
    elif operation == 'lower':
        result_df[column] = str_acc.lower()
    elif operation == 'replace':
        old = kwargs.get('old')
        new = kwargs.get('new', '')
        if old is None:
            raise ValueError("'replace' operation requires 'old' keyword argument")
        result_df[column] = str_acc.replace(old, new)
    elif operation == 'split':
        sep = kwargs.get('sep')
        if sep is None:
            raise ValueError("'split' operation requires 'sep' keyword argument")
        maxsplit = kwargs.get('maxsplit', -1)
        expand = kwargs.get('expand', False)
        split_result = str_acc.split(sep, n=maxsplit, expand=expand)
        if expand:
            for i, sub_col in enumerate(split_result.columns):
                result_df[f"{column}_split_{i}"] = split_result[sub_col]
        else:
            result_df[column] = split_result
    elif operation == 'contains':
        pattern = kwargs.get('pattern')
        if pattern is None:
            raise ValueError("'contains' operation requires 'pattern' keyword argument")
        case = kwargs.get('case', True)
        result_df[f"{column}_contains"] = str_acc.contains(pattern, case=case)
    elif operation == 'regex_extract':
        pattern = kwargs.get('pattern')
        if pattern is None:
            raise ValueError("'regex_extract' operation requires 'pattern' keyword argument")
        extracted = str_acc.extract(pattern)
        result_df[column] = extracted
    elif operation == 'length':
        result_df[f"{column}_length"] = str_acc.len()
    elif operation == 'substring':
        start = kwargs.get('start', 0)
        stop = kwargs.get('stop')
        step = kwargs.get('step')
        result_df[column] = str_acc.slice(start, stop, step)
    elif operation == 'pad_left':
        width = kwargs.get('width')
        if width is None:
            raise ValueError("'pad_left' operation requires 'width' keyword argument")
        fillchar = kwargs.get('fillchar', ' ')
        result_df[column] = str_acc.rjust(width, fillchar)
    elif operation == 'pad_right':
        width = kwargs.get('width')
        if width is None:
            raise ValueError("'pad_right' operation requires 'width' keyword argument")
        fillchar = kwargs.get('fillchar', ' ')
        result_df[column] = str_acc.ljust(width, fillchar)

    return result_df


def date_operations(
    df: pd.DataFrame,
    column: str,
    operation: str,
    **kwargs: Any,
) -> pd.DataFrame:
    """
    Apply date/datetime operations to a column in a DataFrame.

    The specified column is converted to datetime if it is not already.

    Args:
        df: Input DataFrame.
        column: Column name to operate on. Will be converted to datetime if needed.
        operation: One of 'trunc', 'extract', 'diff', 'format', 'timezone',
                   'add', 'is_weekend', 'is_business_day'.
        **kwargs: Additional arguments depending on the operation:
                  - trunc: freq (str) - one of 'year', 'month', 'day', 'hour',
                    'minute', 'second'
                  - extract: part (str) - one of 'year', 'month', 'day', 'hour',
                    'minute', 'second', 'dayofweek', 'dayofyear', 'quarter', 'week'
                  - diff: other (str), unit (str, one of 'D', 'h', 'm', 's')
                  - format: fmt (str) - strftime format string
                  - timezone: tz (str) - timezone string
                  - add: value (int), unit (str, one of 'days', 'hours',
                    'minutes', 'seconds')

    Returns:
        DataFrame with the date operation applied. For 'extract', 'diff', 'format',
        'is_weekend', 'is_business_day', and 'add', a new column is created.
        For 'trunc' and 'timezone', the original column is modified.

    Raises:
        ValueError: If the column is not found or the operation is invalid.

    Examples:
        >>> df = pd.DataFrame({'dt': ['2024-03-15', '2024-07-20', '2024-11-01']})
        >>> date_operations(df, 'dt', 'extract', part='year')
                  dt  dt_year
        0  2024-03-15     2024
        1  2024-07-20     2024
        2  2024-11-01     2024
    """
    VALID_OPERATIONS = {
        'trunc', 'extract', 'diff', 'format', 'timezone',
        'add', 'is_weekend', 'is_business_day',
    }

    FREQ_MAP = {
        'year': 'YS',
        'month': 'MS',
        'day': 'D',
        'hour': 'h',
        'minute': 'min',
        'second': 's',
    }

    # Frequencies that need to_period().dt.to_timestamp() (non-fixed)
    PERIOD_FREQ = {
        'year': 'Y',
        'month': 'M',
    }

    EXTRACT_MAP = {
        'year': lambda s: s.dt.year,
        'month': lambda s: s.dt.month,
        'day': lambda s: s.dt.day,
        'hour': lambda s: s.dt.hour,
        'minute': lambda s: s.dt.minute,
        'second': lambda s: s.dt.second,
        'dayofweek': lambda s: s.dt.dayofweek,
        'dayofyear': lambda s: s.dt.dayofyear,
        'quarter': lambda s: s.dt.quarter,
        'week': lambda s: s.dt.isocalendar().week.astype(int),
    }

    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found in DataFrame")
    if operation not in VALID_OPERATIONS:
        raise ValueError(
            f"Invalid date operation '{operation}'. Must be one of {sorted(VALID_OPERATIONS)}"
        )

    result_df = df.copy()
    # Convert to datetime if not already
    if not pd.api.types.is_datetime64_any_dtype(result_df[column]):
        result_df[column] = pd.to_datetime(result_df[column])

    dt_series = result_df[column]

    if operation == 'trunc':
        freq = kwargs.get('freq')
        if freq is None:
            raise ValueError("'trunc' operation requires 'freq' keyword argument")
        if freq not in FREQ_MAP:
            raise ValueError(
                f"Invalid freq '{freq}'. Must be one of {sorted(FREQ_MAP.keys())}"
            )
        if freq in PERIOD_FREQ:
            result_df[column] = dt_series.dt.to_period(
                PERIOD_FREQ[freq]
            ).dt.to_timestamp()
        else:
            result_df[column] = dt_series.dt.floor(FREQ_MAP[freq])

    elif operation == 'extract':
        part = kwargs.get('part')
        if part is None:
            raise ValueError("'extract' operation requires 'part' keyword argument")
        if part not in EXTRACT_MAP:
            raise ValueError(
                f"Invalid part '{part}'. Must be one of {sorted(EXTRACT_MAP.keys())}"
            )
        result_df[f"{column}_{part}"] = EXTRACT_MAP[part](dt_series)

    elif operation == 'diff':
        other = kwargs.get('other')
        if other is None:
            raise ValueError("'diff' operation requires 'other' keyword argument")
        if other not in result_df.columns:
            raise ValueError(f"Column '{other}' not found in DataFrame")
        other_series = pd.to_datetime(result_df[other])
        unit = kwargs.get('unit', 'D')
        if unit not in ('D', 'h', 'm', 's'):
            raise ValueError(f"Invalid unit '{unit}'. Must be one of 'D', 'h', 'm', 's'")
        diff = (dt_series - other_series)
        multipliers = {'D': 86400, 'h': 3600, 'm': 60, 's': 1}
        result_df[f"{column}_diff_{other}"] = diff.dt.total_seconds() / multipliers[unit]

    elif operation == 'format':
        fmt = kwargs.get('fmt')
        if fmt is None:
            raise ValueError("'format' operation requires 'fmt' keyword argument")
        result_df[f"{column}_formatted"] = dt_series.dt.strftime(fmt)

    elif operation == 'timezone':
        tz = kwargs.get('tz')
        if tz is None:
            raise ValueError("'timezone' operation requires 'tz' keyword argument")
        if dt_series.dt.tz is None:
            result_df[column] = dt_series.dt.tz_localize('UTC').dt.tz_convert(tz)
        else:
            result_df[column] = dt_series.dt.tz_convert(tz)

    elif operation == 'add':
        value = kwargs.get('value')
        if value is None:
            raise ValueError("'add' operation requires 'value' keyword argument")
        unit = kwargs.get('unit')
        if unit is None:
            raise ValueError("'add' operation requires 'unit' keyword argument")
        if unit not in ('days', 'hours', 'minutes', 'seconds'):
            raise ValueError(
                f"Invalid unit '{unit}'. Must be one of 'days', 'hours', 'minutes', 'seconds'"
            )
        result_df[f"{column}_added"] = dt_series + pd.Timedelta(**{unit: value})

    elif operation == 'is_weekend':
        result_df[f"{column}_is_weekend"] = dt_series.dt.dayofweek.isin([5, 6])

    elif operation == 'is_business_day':
        result_df[f"{column}_is_business_day"] = ~dt_series.dt.dayofweek.isin([5, 6])

    return result_df


class TransformationChain:
    """
    A fluent, chainable API for applying DataFrame transformations.

    Each method returns ``self`` for chaining, except ``.result()`` which
    returns the transformed DataFrame. Every method delegates to the
    corresponding standalone transformation function.

    Args:
        df: The input DataFrame to transform.

    Examples:
        >>> result = (
        ...     TransformationChain(df)
        ...     .filter(column='age', min_value=18)
        ...     .select_columns(['name', 'age'])
        ...     .sort(by='name')
        ...     .result()
        ... )
    """

    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df

    def filter(
        self,
        filter_func: Optional[Callable[[pd.Series], bool]] = None,
        column: Optional[str] = None,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
    ) -> "TransformationChain":
        """Apply filter_data. See :func:`filter_data` for details."""
        self._df = filter_data(
            self._df,
            filter_func=filter_func,
            column=column,
            min_value=min_value,
            max_value=max_value,
        )
        return self

    def map(
        self,
        column: str,
        mapping: Union[Dict[Any, Any], Callable[[Any], Any]],
    ) -> "TransformationChain":
        """Apply map_values. See :func:`map_values` for details."""
        self._df = map_values(self._df, column=column, mapping=mapping)
        return self

    def aggregate(
        self,
        groupby: Union[str, List[str]],
        agg: Dict[str, Union[str, List[str], Callable]],
    ) -> "TransformationChain":
        """Apply aggregate_data. See :func:`aggregate_data` for details."""
        self._df = aggregate_data(self._df, groupby=groupby, agg=agg)
        return self

    def join(
        self,
        right: pd.DataFrame,
        on: Optional[Union[str, List[str]]] = None,
        left_on: Optional[Union[str, List[str]]] = None,
        right_on: Optional[Union[str, List[str]]] = None,
        how: str = "inner",
        suffix: str = "_right",
    ) -> "TransformationChain":
        """Apply join_data. See :func:`join_data` for details."""
        self._df = join_data(
            self._df,
            right,
            on=on,
            left_on=left_on,
            right_on=right_on,
            how=how,
            suffix=suffix,
        )
        return self

    def union(self, other: pd.DataFrame, ignore_index: bool = True) -> "TransformationChain":
        """Apply union_data. See :func:`union_data` for details."""
        self._df = union_data(self._df, other=other, ignore_index=ignore_index)
        return self

    def deduplicate(
        self,
        subset: Optional[List[str]] = None,
        keep: Union[str, bool] = "first",
    ) -> "TransformationChain":
        """Apply deduplicate_data. See :func:`deduplicate_data` for details."""
        self._df = deduplicate_data(self._df, subset=subset, keep=keep)
        return self

    def with_column(
        self,
        name: str,
        value: Union[Any, pd.Series, Callable[[pd.DataFrame], Union[Any, pd.Series]]],
    ) -> "TransformationChain":
        """Apply with_column. See :func:`with_column` for details."""
        self._df = with_column(self._df, name=name, value=value)
        return self

    def rename_columns(self, mapping: Dict[str, str]) -> "TransformationChain":
        """Apply rename_columns. See :func:`rename_columns` for details."""
        self._df = rename_columns(self._df, mapping=mapping)
        return self

    def select_columns(self, columns: List[str]) -> "TransformationChain":
        """Apply select_columns. See :func:`select_columns` for details."""
        self._df = select_columns(self._df, columns=columns)
        return self

    def drop_columns(self, columns: Union[str, List[str]]) -> "TransformationChain":
        """Apply drop_columns. See :func:`drop_columns` for details."""
        self._df = drop_columns(self._df, columns=columns)
        return self

    def fill_na(
        self,
        value: Union[Any, Dict[str, Any]],
        subset: Optional[List[str]] = None,
    ) -> "TransformationChain":
        """Apply fill_na. See :func:`fill_na` for details."""
        self._df = fill_na(self._df, value=value, subset=subset)
        return self

    def drop_na(
        self,
        subset: Optional[List[str]] = None,
        how: str = "any",
    ) -> "TransformationChain":
        """Apply drop_na. See :func:`drop_na` for details."""
        self._df = drop_na(self._df, subset=subset, how=how)
        return self

    def sort(
        self,
        by: Union[str, List[str]],
        ascending: Union[bool, List[bool]] = True,
    ) -> "TransformationChain":
        """Apply sort_data. See :func:`sort_data` for details."""
        self._df = sort_data(self._df, by=by, ascending=ascending)
        return self

    def limit(self, n: int) -> "TransformationChain":
        """Apply limit_rows. See :func:`limit_rows` for details."""
        self._df = limit_rows(self._df, n=n)
        return self

    def sample(
        self,
        n: Optional[int] = None,
        frac: Optional[float] = None,
        seed: Optional[int] = None,
    ) -> "TransformationChain":
        """Apply sample_data. See :func:`sample_data` for details."""
        self._df = sample_data(self._df, n=n, frac=frac, seed=seed)
        return self

    def distinct(self, subset: Optional[List[str]] = None) -> "TransformationChain":
        """Apply distinct_data. See :func:`distinct_data` for details."""
        self._df = distinct_data(self._df, subset=subset)
        return self

    def cast(
        self,
        type_map: Dict[str, Union[str, type]],
        errors: str = "raise",
    ) -> "TransformationChain":
        """Apply cast_columns. See :func:`cast_columns` for details."""
        self._df = cast_columns(self._df, type_map=type_map, errors=errors)
        return self

    def when(
        self,
        conditions: List[Tuple[pd.Series, Any]],
        otherwise_value: Any,
        output_col: str,
    ) -> "TransformationChain":
        """Apply when_otherwise. See :func:`when_otherwise` for details."""
        self._df = when_otherwise(
            self._df,
            conditions=conditions,
            otherwise_value=otherwise_value,
            output_col=output_col,
        )
        return self

    def compute(self, expression: str, output_col: str) -> "TransformationChain":
        """Apply add_computed_column. See :func:`add_computed_column` for details."""
        self._df = add_computed_column(self._df, expression=expression, output_col=output_col)
        return self

    def pivot(
        self,
        index: Union[str, List[str]],
        columns: str,
        values: Union[str, List[str]],
        agg_func: str = "first",
    ) -> "TransformationChain":
        """Apply pivot_data. See :func:`pivot_data` for details."""
        self._df = pivot_data(
            self._df,
            index=index,
            columns=columns,
            values=values,
            agg_func=agg_func,
        )
        return self

    def unpivot(
        self,
        id_vars: Union[str, List[str]],
        value_vars: Optional[Union[str, List[str]]] = None,
        var_name: str = "variable",
        value_name: str = "value",
    ) -> "TransformationChain":
        """Apply unpivot_data. See :func:`unpivot_data` for details."""
        self._df = unpivot_data(
            self._df,
            id_vars=id_vars,
            value_vars=value_vars,
            var_name=var_name,
            value_name=value_name,
        )
        return self

    def window(
        self,
        partition_by: Union[str, List[str]],
        order_by: Union[str, List[str]],
        functions: Dict[str, Dict[str, Any]],
    ) -> "TransformationChain":
        """Apply window_functions. See :func:`window_functions` for details."""
        self._df = window_functions(
            self._df,
            partition_by=partition_by,
            order_by=order_by,
            functions=functions,
        )
        return self

    def str_op(
        self,
        column: str,
        operation: str,
        **kwargs: Any,
    ) -> "TransformationChain":
        """Apply string_operations. See :func:`string_operations` for details."""
        self._df = string_operations(self._df, column=column, operation=operation, **kwargs)
        return self

    def date_op(
        self,
        column: str,
        operation: str,
        **kwargs: Any,
    ) -> "TransformationChain":
        """Apply date_operations. See :func:`date_operations` for details."""
        self._df = date_operations(self._df, column=column, operation=operation, **kwargs)
        return self

    def result(self) -> pd.DataFrame:
        """
        Return the transformed DataFrame.

        Returns:
            The final DataFrame after all chained transformations.
        """
        return self._df


def chain(df: pd.DataFrame) -> TransformationChain:
    """
    Create a TransformationChain from a DataFrame.

    This is a convenience function equivalent to ``TransformationChain(df)``.

    Args:
        df: The input DataFrame to transform.

    Returns:
        A new TransformationChain instance.

    Examples:
        >>> result = chain(df).filter(column='age', min_value=18).sort(by='name').result()
    """
    return TransformationChain(df)
