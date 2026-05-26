"""
Data quality validation functions for SimpleETL.

Provides schema validation, null checks, duplicate detection,
value range checks, unique value checks, data profiling, and
a report class for collecting multiple check results.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pandas as pd


class DataQualityError(Exception):
    """Raised when a data quality check fails."""

    def __init__(self, message: str, check_name: str = "",
                 details: Optional[Dict[str, Any]] = None):
        self.check_name = check_name
        self.details = details or {}
        super().__init__(message)


def validate_schema(
    df: pd.DataFrame,
    required_columns: List[str],
    column_types: Optional[Dict[str, str]] = None,
) -> bool:
    """Validate that DataFrame has required columns and optional type checks.

    Args:
        df: DataFrame to validate.
        required_columns: List of column names that must be present.
        column_types: Optional mapping of column names to expected
            pandas dtype strings (e.g. {'age': 'int64'}).

    Returns:
        True if validation passes.

    Raises:
        DataQualityError: If required columns are missing or types
            do not match.
    """
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise DataQualityError(
            f"Missing required columns: {missing}",
            check_name="validate_schema",
            details={"missing_columns": missing},
        )

    if column_types:
        type_mismatches = {}
        for col, expected_type in column_types.items():
            if col in df.columns:
                actual = str(df[col].dtype)
                if actual != expected_type:
                    type_mismatches[col] = {
                        "expected": expected_type,
                        "actual": actual,
                    }
        if type_mismatches:
            raise DataQualityError(
                f"Column type mismatches: {type_mismatches}",
                check_name="validate_schema",
                details={"type_mismatches": type_mismatches},
            )

    return True


def check_nulls(
    df: pd.DataFrame,
    columns: Optional[List[str]] = None,
    threshold: float = 0.0,
) -> Dict[str, float]:
    """Check for null values in specified columns.

    Args:
        df: DataFrame to check.
        columns: Columns to check. If None, checks all columns.
        threshold: Maximum allowed fraction of null values (0.0 = no
            nulls allowed).

    Returns:
        Dictionary mapping column name to null fraction.

    Raises:
        DataQualityError: If null fraction exceeds threshold in any
            column.
    """
    if columns is None:
        columns = list(df.columns)

    null_fractions: Dict[str, float] = {}
    violations: Dict[str, float] = {}

    for col in columns:
        if col in df.columns:
            fraction = df[col].isna().mean()
            null_fractions[col] = fraction
            if fraction > threshold:
                violations[col] = fraction

    if violations:
        raise DataQualityError(
            f"Null threshold exceeded in columns: "
            f"{', '.join(f'{k} ({v:.2%})' for k, v in violations.items())}",
            check_name="check_nulls",
            details={
                "threshold": threshold,
                "violations": violations,
                "null_fractions": null_fractions,
            },
        )

    return null_fractions


def check_duplicates(
    df: pd.DataFrame,
    columns: Optional[List[str]] = None,
    threshold: float = 0.0,
) -> int:
    """Check for duplicate rows.

    Args:
        df: DataFrame to check.
        columns: Columns to consider for duplicate detection. If None,
            uses all columns.
        threshold: Maximum allowed fraction of duplicate rows
            (0.0 = no duplicates allowed).

    Returns:
        Number of duplicate rows found.

    Raises:
        DataQualityError: If duplicate fraction exceeds threshold.
    """
    if columns:
        dup_mask = df.duplicated(subset=columns, keep="first")
    else:
        dup_mask = df.duplicated(keep="first")

    dup_count = int(dup_mask.sum())
    total = len(df)
    fraction = dup_count / total if total > 0 else 0.0

    if fraction > threshold:
        raise DataQualityError(
            f"Duplicate threshold exceeded: {dup_count} duplicates "
            f"({fraction:.2%})",
            check_name="check_duplicates",
            details={
                "threshold": threshold,
                "duplicate_count": dup_count,
                "duplicate_fraction": fraction,
            },
        )

    return dup_count


def check_value_range(
    df: pd.DataFrame,
    column: str,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
) -> bool:
    """Check that column values are within the specified range.

    Args:
        df: DataFrame to check.
        column: Column name to validate.
        min_value: Minimum allowed value (inclusive). None for no
            lower bound.
        max_value: Maximum allowed value (inclusive). None for no
            upper bound.

    Returns:
        True if all values are within range.

    Raises:
        DataQualityError: If values are outside the specified range.
    """
    if column not in df.columns:
        raise DataQualityError(
            f"Column '{column}' not found in DataFrame",
            check_name="check_value_range",
            details={"column": column},
        )

    series = df[column].dropna()
    violations = {}

    if min_value is not None:
        below = int((series < min_value).sum())
        if below > 0:
            violations["below_min"] = {
                "count": below,
                "min_value": min_value,
                "actual_min": float(series.min()),
            }

    if max_value is not None:
        above = int((series > max_value).sum())
        if above > 0:
            violations["above_max"] = {
                "count": above,
                "max_value": max_value,
                "actual_max": float(series.max()),
            }

    if violations:
        raise DataQualityError(
            f"Value range violation in column '{column}': {violations}",
            check_name="check_value_range",
            details={"column": column, "violations": violations},
        )

    return True


def check_unique_values(
    df: pd.DataFrame,
    column: str,
    expected_count: Optional[int] = None,
) -> int:
    """Check the number of unique values in a column.

    Args:
        df: DataFrame to check.
        column: Column name to check.
        expected_count: If provided, verify that the number of unique
            values matches this count.

    Returns:
        Number of unique values in the column.

    Raises:
        DataQualityError: If expected_count is provided and does not
            match the actual unique count.
    """
    if column not in df.columns:
        raise DataQualityError(
            f"Column '{column}' not found in DataFrame",
            check_name="check_unique_values",
            details={"column": column},
        )

    unique_count = int(df[column].nunique())

    if expected_count is not None and unique_count != expected_count:
        raise DataQualityError(
            f"Unique value count mismatch for column '{column}': "
            f"expected {expected_count}, got {unique_count}",
            check_name="check_unique_values",
            details={
                "column": column,
                "expected_count": expected_count,
                "actual_count": unique_count,
            },
        )

    return unique_count


def profile_data(df: pd.DataFrame) -> Dict[str, Any]:
    """Return basic profiling information about a DataFrame.

    Args:
        df: DataFrame to profile.

    Returns:
        Dictionary with row_count, column_count, null_counts,
        dtypes, and memory_usage_bytes.
    """
    null_counts = df.isna().sum().to_dict()
    dtypes = {col: str(dtype) for col, dtype in df.dtypes.items()}
    memory_bytes = int(df.memory_usage(deep=True).sum())

    return {
        "row_count": len(df),
        "column_count": len(df.columns),
        "null_counts": null_counts,
        "dtypes": dtypes,
        "memory_usage_bytes": memory_bytes,
    }


@dataclass
class CheckResult:
    """Result of a single data quality check."""

    name: str
    passed: bool
    details: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None


class DataQualityReport:
    """Collects multiple quality check results.

    Can operate in strict mode (raise on first failure) or report
    mode (collect all results and raise at the end if any failed).
    """

    def __init__(self, raise_on_failure: bool = True):
        """Initialize the report.

        Args:
            raise_on_failure: If True, raise DataQualityError
                immediately when a check fails.
        """
        self.raise_on_failure = raise_on_failure
        self._results: List[CheckResult] = []

    def add_check(self, name: str, passed: bool,
                  details: Optional[Dict[str, Any]] = None,
                  error: Optional[str] = None) -> None:
        """Add a check result to the report.

        Args:
            name: Name of the check.
            passed: Whether the check passed.
            details: Optional details about the check result.
            error: Error message if the check failed.

        Raises:
            DataQualityError: If raise_on_failure is True and
                the check did not pass.
        """
        result = CheckResult(
            name=name,
            passed=passed,
            details=details or {},
            error=error,
        )
        self._results.append(result)

        if not passed and self.raise_on_failure:
            raise DataQualityError(
                error or f"Check '{name}' failed",
                check_name=name,
                details=result.details,
            )

    def validate_schema(
        self,
        df: pd.DataFrame,
        required_columns: List[str],
        column_types: Optional[Dict[str, str]] = None,
    ) -> None:
        """Run validate_schema and record the result."""
        try:
            validate_schema(df, required_columns, column_types)
            self.add_check("validate_schema", passed=True)
        except DataQualityError as e:
            self.add_check(
                "validate_schema",
                passed=False,
                details=e.details,
                error=str(e),
            )

    def check_nulls(
        self,
        df: pd.DataFrame,
        columns: Optional[List[str]] = None,
        threshold: float = 0.0,
    ) -> None:
        """Run check_nulls and record the result."""
        try:
            result = check_nulls(df, columns, threshold)
            self.add_check("check_nulls", passed=True,
                           details={"null_fractions": result})
        except DataQualityError as e:
            self.add_check(
                "check_nulls",
                passed=False,
                details=e.details,
                error=str(e),
            )

    def check_duplicates(
        self,
        df: pd.DataFrame,
        columns: Optional[List[str]] = None,
        threshold: float = 0.0,
    ) -> None:
        """Run check_duplicates and record the result."""
        try:
            dup_count = check_duplicates(df, columns, threshold)
            self.add_check(
                "check_duplicates",
                passed=True,
                details={"duplicate_count": dup_count},
            )
        except DataQualityError as e:
            self.add_check(
                "check_duplicates",
                passed=False,
                details=e.details,
                error=str(e),
            )

    def check_value_range(
        self,
        df: pd.DataFrame,
        column: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
    ) -> None:
        """Run check_value_range and record the result."""
        try:
            check_value_range(df, column, min_value, max_value)
            self.add_check("check_value_range", passed=True)
        except DataQualityError as e:
            self.add_check(
                "check_value_range",
                passed=False,
                details=e.details,
                error=str(e),
            )

    def check_unique_values(
        self,
        df: pd.DataFrame,
        column: str,
        expected_count: Optional[int] = None,
    ) -> None:
        """Run check_unique_values and record the result."""
        try:
            count = check_unique_values(df, column, expected_count)
            self.add_check(
                "check_unique_values",
                passed=True,
                details={"unique_count": count},
            )
        except DataQualityError as e:
            self.add_check(
                "check_unique_values",
                passed=False,
                details=e.details,
                error=str(e),
            )

    @property
    def results(self) -> List[CheckResult]:
        """Return all recorded check results."""
        return list(self._results)

    @property
    def passed(self) -> bool:
        """Return True if all checks passed."""
        return all(r.passed for r in self._results)

    @property
    def failed_checks(self) -> List[CheckResult]:
        """Return all failed check results."""
        return [r for r in self._results if not r.passed]

    def raise_on_failures(self) -> None:
        """Raise DataQualityError if any checks failed.

        Raises:
            DataQualityError: If any checks did not pass.
        """
        if self.failed_checks:
            failed_names = [c.name for c in self.failed_checks]
            raise DataQualityError(
                f"Data quality checks failed: {failed_names}",
                check_name="DataQualityReport",
                details={
                    "failed_checks": [
                        {"name": c.name, "error": c.error}
                        for c in self.failed_checks
                    ],
                },
            )

    def summary(self) -> Dict[str, Any]:
        """Return a summary of all check results.

        Returns:
            Dictionary with total, passed, failed counts and
            list of results.
        """
        return {
            "total": len(self._results),
            "passed": sum(1 for r in self._results if r.passed),
            "failed": sum(1 for r in self._results if not r.passed),
            "results": [
                {
                    "name": r.name,
                    "passed": r.passed,
                    "error": r.error,
                }
                for r in self._results
            ],
        }
