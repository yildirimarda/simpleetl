"""
Tests for the data quality validation module.
"""

import pytest
import pandas as pd
from simpleetl.core.quality import (
    DataQualityError,
    DataQualityReport,
    CheckResult,
    validate_schema,
    check_nulls,
    check_duplicates,
    check_value_range,
    check_unique_values,
    profile_data,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_df():
    """A basic DataFrame used across many tests."""
    return pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "age": [25, 30, 35, 40, 45],
        "score": [88.5, 92.0, 76.3, 95.1, 81.7],
    })


@pytest.fixture
def df_with_nulls():
    """A DataFrame containing null values."""
    return pd.DataFrame({
        "id": [1, 2, None, 4, 5],
        "name": ["Alice", None, "Charlie", None, "Eve"],
        "value": [10.0, 20.0, 30.0, 40.0, 50.0],
    })


@pytest.fixture
def df_with_duplicates():
    """A DataFrame containing duplicate rows."""
    return pd.DataFrame({
        "id": [1, 2, 2, 3, 3, 3],
        "name": ["a", "b", "b", "c", "c", "c"],
    })


# ---------------------------------------------------------------------------
# validate_schema
# ---------------------------------------------------------------------------


class TestValidateSchema:
    """Tests for validate_schema."""

    def test_all_required_columns_present(self, sample_df):
        result = validate_schema(sample_df, ["id", "name"])
        assert result is True

    def test_missing_required_column(self, sample_df):
        with pytest.raises(DataQualityError, match="Missing required columns"):
            validate_schema(sample_df, ["id", "nonexistent"])

    def test_multiple_missing_columns(self, sample_df):
        with pytest.raises(DataQualityError) as exc_info:
            validate_schema(sample_df, ["x", "y", "z"])
        assert exc_info.value.details["missing_columns"] == ["x", "y", "z"]

    def test_empty_required_columns(self, sample_df):
        result = validate_schema(sample_df, [])
        assert result is True

    def test_column_types_match(self, sample_df):
        result = validate_schema(
            sample_df, ["id"], column_types={"id": "int64"}
        )
        assert result is True

    def test_column_types_mismatch(self, sample_df):
        with pytest.raises(DataQualityError, match="type mismatches"):
            validate_schema(
                sample_df, ["id"], column_types={"id": "float64"}
            )

    def test_column_types_mismatch_details(self, sample_df):
        with pytest.raises(DataQualityError) as exc_info:
            validate_schema(
                sample_df, ["id"], column_types={"id": "float64"}
            )
        mismatches = exc_info.value.details["type_mismatches"]
        assert "id" in mismatches
        assert mismatches["id"]["expected"] == "float64"
        assert mismatches["id"]["actual"] == "int64"

    def test_column_types_skip_missing_col(self, sample_df):
        """Type check for a column not in df should not raise."""
        result = validate_schema(
            sample_df, ["id"], column_types={"nonexistent": "int64"}
        )
        assert result is True

    def test_error_check_name(self, sample_df):
        with pytest.raises(DataQualityError) as exc_info:
            validate_schema(sample_df, ["missing"])
        assert exc_info.value.check_name == "validate_schema"


# ---------------------------------------------------------------------------
# check_nulls
# ---------------------------------------------------------------------------


class TestCheckNulls:
    """Tests for check_nulls."""

    def test_no_nulls(self, sample_df):
        result = check_nulls(sample_df)
        assert all(v == 0.0 for v in result.values())

    def test_no_nulls_specific_columns(self, sample_df):
        result = check_nulls(sample_df, columns=["id", "name"])
        assert result == {"id": 0.0, "name": 0.0}

    def test_nulls_within_threshold(self, df_with_nulls):
        result = check_nulls(df_with_nulls, threshold=0.5)
        assert result["id"] == pytest.approx(0.2)
        assert result["name"] == pytest.approx(0.4)

    def test_nulls_exceed_threshold(self, df_with_nulls):
        with pytest.raises(DataQualityError, match="Null threshold exceeded"):
            check_nulls(df_with_nulls, threshold=0.1)

    def test_nulls_specific_column_exceeds(self, df_with_nulls):
        with pytest.raises(DataQualityError):
            check_nulls(
                df_with_nulls, columns=["name"], threshold=0.1
            )

    def test_nulls_specific_column_within(self, df_with_nulls):
        result = check_nulls(
            df_with_nulls, columns=["value"], threshold=0.0
        )
        assert result["value"] == 0.0

    def test_zero_threshold_no_nulls_allowed(self, sample_df):
        result = check_nulls(sample_df, threshold=0.0)
        assert all(v == 0.0 for v in result.values())

    def test_zero_threshold_with_nulls(self, df_with_nulls):
        with pytest.raises(DataQualityError):
            check_nulls(df_with_nulls, threshold=0.0)

    def test_error_details(self, df_with_nulls):
        with pytest.raises(DataQualityError) as exc_info:
            check_nulls(df_with_nulls, threshold=0.1)
        assert "violations" in exc_info.value.details
        assert "null_fractions" in exc_info.value.details

    def test_nonexistent_column_ignored(self, sample_df):
        """Columns not in df should not cause errors."""
        result = check_nulls(sample_df, columns=["nonexistent"])
        assert result == {}


# ---------------------------------------------------------------------------
# check_duplicates
# ---------------------------------------------------------------------------


class TestCheckDuplicates:
    """Tests for check_duplicates."""

    def test_no_duplicates(self, sample_df):
        result = check_duplicates(sample_df)
        assert result == 0

    def test_duplicates_found(self, df_with_duplicates):
        result = check_duplicates(df_with_duplicates, threshold=1.0)
        assert result == 3

    def test_duplicates_exceed_threshold(self, df_with_duplicates):
        with pytest.raises(DataQualityError, match="Duplicate threshold exceeded"):
            check_duplicates(df_with_duplicates, threshold=0.1)

    def test_duplicates_within_threshold(self, df_with_duplicates):
        result = check_duplicates(df_with_duplicates, threshold=0.6)
        assert result == 3

    def test_duplicates_specific_columns(self):
        df = pd.DataFrame({
            "a": [1, 1, 2, 2],
            "b": ["x", "y", "z", "w"],
        })
        result = check_duplicates(df, columns=["a"], threshold=1.0)
        assert result == 2

    def test_no_duplicates_specific_columns(self, sample_df):
        result = check_duplicates(sample_df, columns=["id"])
        assert result == 0

    def test_empty_dataframe(self):
        df = pd.DataFrame({"a": pd.Series([], dtype="int64")})
        result = check_duplicates(df)
        assert result == 0

    def test_error_details(self, df_with_duplicates):
        with pytest.raises(DataQualityError) as exc_info:
            check_duplicates(df_with_duplicates, threshold=0.1)
        assert "duplicate_count" in exc_info.value.details
        assert "duplicate_fraction" in exc_info.value.details


# ---------------------------------------------------------------------------
# check_value_range
# ---------------------------------------------------------------------------


class TestCheckValueRange:
    """Tests for check_value_range."""

    def test_values_within_range(self, sample_df):
        result = check_value_range(
            sample_df, "age", min_value=20, max_value=50
        )
        assert result is True

    def test_values_below_min(self, sample_df):
        with pytest.raises(DataQualityError, match="below_min"):
            check_value_range(sample_df, "age", min_value=30)

    def test_values_above_max(self, sample_df):
        with pytest.raises(DataQualityError, match="above_max"):
            check_value_range(sample_df, "age", max_value=30)

    def test_min_only(self, sample_df):
        result = check_value_range(sample_df, "age", min_value=0)
        assert result is True

    def test_max_only(self, sample_df):
        result = check_value_range(sample_df, "age", max_value=100)
        assert result is True

    def test_no_bounds(self, sample_df):
        result = check_value_range(sample_df, "age")
        assert result is True

    def test_nonexistent_column(self, sample_df):
        with pytest.raises(DataQualityError, match="not found"):
            check_value_range(sample_df, "nonexistent", min_value=0)

    def test_with_nulls_ignored(self):
        df = pd.DataFrame({"val": [1.0, 2.0, None, 4.0]})
        result = check_value_range(df, "val", min_value=0, max_value=10)
        assert result is True

    def test_error_details(self, sample_df):
        with pytest.raises(DataQualityError) as exc_info:
            check_value_range(sample_df, "age", min_value=30)
        assert "violations" in exc_info.value.details
        assert "below_min" in exc_info.value.details["violations"]


# ---------------------------------------------------------------------------
# check_unique_values
# ---------------------------------------------------------------------------


class TestCheckUniqueValues:
    """Tests for check_unique_values."""

    def test_unique_count(self, sample_df):
        result = check_unique_values(sample_df, "name")
        assert result == 5

    def test_expected_count_matches(self, sample_df):
        result = check_unique_values(sample_df, "name", expected_count=5)
        assert result == 5

    def test_expected_count_mismatch(self, sample_df):
        with pytest.raises(
            DataQualityError, match="Unique value count mismatch"
        ):
            check_unique_values(sample_df, "name", expected_count=3)

    def test_nonexistent_column(self, sample_df):
        with pytest.raises(DataQualityError, match="not found"):
            check_unique_values(sample_df, "nonexistent")

    def test_duplicates_reduce_unique_count(self):
        df = pd.DataFrame({"col": ["a", "a", "b", "b"]})
        result = check_unique_values(df, "col")
        assert result == 2

    def test_error_details(self, sample_df):
        with pytest.raises(DataQualityError) as exc_info:
            check_unique_values(sample_df, "name", expected_count=3)
        assert exc_info.value.details["expected_count"] == 3
        assert exc_info.value.details["actual_count"] == 5


# ---------------------------------------------------------------------------
# profile_data
# ---------------------------------------------------------------------------


class TestProfileData:
    """Tests for profile_data."""

    def test_basic_profile(self, sample_df):
        result = profile_data(sample_df)
        assert result["row_count"] == 5
        assert result["column_count"] == 4

    def test_null_counts(self, df_with_nulls):
        result = profile_data(df_with_nulls)
        assert result["null_counts"]["id"] == 1
        assert result["null_counts"]["name"] == 2
        assert result["null_counts"]["value"] == 0

    def test_dtypes(self, sample_df):
        result = profile_data(sample_df)
        assert result["dtypes"]["id"] == "int64"
        assert result["dtypes"]["name"] in ("object", "str")

    def test_memory_usage_positive(self, sample_df):
        result = profile_data(sample_df)
        assert result["memory_usage_bytes"] > 0

    def test_empty_dataframe(self):
        df = pd.DataFrame()
        result = profile_data(df)
        assert result["row_count"] == 0
        assert result["column_count"] == 0
        assert result["null_counts"] == {}
        assert result["dtypes"] == {}


# ---------------------------------------------------------------------------
# DataQualityReport
# ---------------------------------------------------------------------------


class TestDataQualityReport:
    """Tests for DataQualityReport."""

    def test_all_pass_strict_mode(self, sample_df):
        report = DataQualityReport(raise_on_failure=True)
        report.validate_schema(sample_df, ["id", "name"])
        report.check_nulls(sample_df)
        report.check_duplicates(sample_df)
        assert report.passed is True
        assert len(report.results) == 3

    def test_failure_strict_mode_raises(self, sample_df):
        report = DataQualityReport(raise_on_failure=True)
        with pytest.raises(DataQualityError):
            report.validate_schema(sample_df, ["nonexistent"])

    def test_failure_non_strict_mode(self, sample_df):
        report = DataQualityReport(raise_on_failure=False)
        report.validate_schema(sample_df, ["nonexistent"])
        assert report.passed is False
        assert len(report.failed_checks) == 1

    def test_multiple_failures_non_strict(self, sample_df):
        report = DataQualityReport(raise_on_failure=False)
        report.validate_schema(sample_df, ["missing_col"])
        report.check_nulls(sample_df, threshold=0.0)
        report.check_duplicates(sample_df, threshold=0.0)
        assert report.passed is False
        assert report.summary()["failed"] == 1

    def test_raise_on_failures(self, sample_df):
        report = DataQualityReport(raise_on_failure=False)
        report.validate_schema(sample_df, ["missing"])
        with pytest.raises(DataQualityError, match="Data quality checks failed"):
            report.raise_on_failures()

    def test_raise_on_failures_when_all_pass(self, sample_df):
        report = DataQualityReport(raise_on_failure=False)
        report.validate_schema(sample_df, ["id"])
        report.raise_on_failures()  # should not raise

    def test_summary(self, sample_df):
        report = DataQualityReport(raise_on_failure=False)
        report.validate_schema(sample_df, ["id"])
        report.validate_schema(sample_df, ["missing"])
        summary = report.summary()
        assert summary["total"] == 2
        assert summary["passed"] == 1
        assert summary["failed"] == 1

    def test_results_property_returns_copy(self, sample_df):
        report = DataQualityReport(raise_on_failure=False)
        report.validate_schema(sample_df, ["id"])
        results = report.results
        results.clear()
        assert len(report.results) == 1

    def test_check_nulls_via_report(self, df_with_nulls):
        report = DataQualityReport(raise_on_failure=False)
        report.check_nulls(df_with_nulls, threshold=0.0)
        assert report.passed is False

    def test_check_duplicates_via_report(self, df_with_duplicates):
        report = DataQualityReport(raise_on_failure=False)
        report.check_duplicates(df_with_duplicates, threshold=0.0)
        assert report.passed is False

    def test_check_value_range_via_report(self, sample_df):
        report = DataQualityReport(raise_on_failure=False)
        report.check_value_range(sample_df, "age", min_value=100)
        assert report.passed is False

    def test_check_unique_values_via_report(self, sample_df):
        report = DataQualityReport(raise_on_failure=False)
        report.check_unique_values(sample_df, "name", expected_count=99)
        assert report.passed is False

    def test_add_check_directly(self):
        report = DataQualityReport(raise_on_failure=False)
        report.add_check("custom", passed=True, details={"info": "ok"})
        assert report.passed is True

    def test_add_check_failure_strict(self):
        report = DataQualityReport(raise_on_failure=True)
        with pytest.raises(DataQualityError):
            report.add_check("custom", passed=False, error="failed")

    def test_check_result_dataclass(self):
        result = CheckResult(name="test", passed=True)
        assert result.name == "test"
        assert result.passed is True
        assert result.details == {}
        assert result.error is None


# ---------------------------------------------------------------------------
# DataQualityError
# ---------------------------------------------------------------------------


class TestDataQualityError:
    """Tests for DataQualityError."""

    def test_basic_error(self):
        err = DataQualityError("something went wrong")
        assert str(err) == "something went wrong"

    def test_error_with_check_name(self):
        err = DataQualityError("fail", check_name="my_check")
        assert err.check_name == "my_check"

    def test_error_with_details(self):
        err = DataQualityError("fail", details={"key": "value"})
        assert err.details == {"key": "value"}

    def test_error_is_exception(self):
        with pytest.raises(DataQualityError):
            raise DataQualityError("test")
