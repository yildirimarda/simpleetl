"""
Tests for the declarative data quality rules engine.

Covers RuleResult, RuleReport, QualityRuleEngine (eager validation and
every supported rule type), severity behavior, and QualityRuleHook.
"""

import json
import logging
from unittest.mock import MagicMock

import pandas as pd
import pytest

from simpleetl.core.hooks import (
    POST_TRANSFORM,
    HookContext,
    HookRegistry,
    execute_hooks,
    register_hook,
)
from simpleetl.core.quality_rules import (
    SUPPORTED_RULE_TYPES,
    QualityRuleEngine,
    QualityRuleError,
    QualityRuleHook,
    RuleReport,
    RuleResult,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_df():
    """A basic DataFrame used across many tests."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            "age": [25, 30, 35, 40, 45],
            "status": ["active", "active", "inactive", "active", "inactive"],
            "email": [
                "alice@x.com",
                "bob@x.com",
                "charlie@x.com",
                "diana@x.com",
                "eve@x.com",
            ],
        }
    )


@pytest.fixture
def mock_job():
    """Return a mocked ETLJob with a config.name attribute."""
    job = MagicMock()
    job.config.name = "test_job"
    return job


@pytest.fixture(autouse=True)
def reset_registry():
    """Reset the singleton HookRegistry before and after each test."""
    reg = HookRegistry()
    reg.reset()
    yield
    reg.reset()


def evaluate_one(rule, df):
    """Evaluate a single rule and return its RuleResult."""
    report = QualityRuleEngine([rule]).evaluate(df)
    assert len(report.results) == 1
    return report.results[0]


# ---------------------------------------------------------------------------
# Eager rule validation
# ---------------------------------------------------------------------------


class TestRuleValidation:
    """Tests for eager validation in QualityRuleEngine.__init__."""

    def test_empty_rules_list(self):
        engine = QualityRuleEngine([])
        assert engine.rules == []

    def test_rules_property_returns_copies(self):
        rules = [{"type": "not_null", "column": "id"}]
        engine = QualityRuleEngine(rules)
        engine.rules[0]["column"] = "mutated"
        assert engine.rules[0]["column"] == "id"

    def test_unknown_rule_type(self):
        with pytest.raises(ValueError, match="unknown rule type 'bogus'"):
            QualityRuleEngine([{"type": "bogus", "column": "id"}])

    def test_missing_type_key(self):
        with pytest.raises(ValueError, match="unknown rule type None"):
            QualityRuleEngine([{"column": "id"}])

    def test_non_dict_rule(self):
        with pytest.raises(ValueError, match="Rule 0 must be a dict"):
            QualityRuleEngine(["not_null"])

    def test_missing_column_key(self):
        with pytest.raises(ValueError, match="missing required key"):
            QualityRuleEngine([{"type": "not_null"}])

    def test_in_range_requires_min_or_max(self):
        with pytest.raises(ValueError, match="at least one of 'min' or 'max'"):
            QualityRuleEngine([{"type": "in_range", "column": "age"}])

    def test_in_set_missing_values(self):
        with pytest.raises(ValueError, match="missing required key"):
            QualityRuleEngine([{"type": "in_set", "column": "status"}])

    def test_in_set_values_not_a_list(self):
        with pytest.raises(ValueError, match="'values' must be a list"):
            QualityRuleEngine(
                [{"type": "in_set", "column": "status", "values": "active"}]
            )

    def test_matches_regex_invalid_pattern(self):
        with pytest.raises(ValueError, match="invalid pattern"):
            QualityRuleEngine(
                [{"type": "matches_regex", "column": "email", "pattern": "["}]
            )

    def test_row_count_min_missing_value(self):
        with pytest.raises(ValueError, match="missing required key"):
            QualityRuleEngine([{"type": "row_count_min"}])

    def test_expression_missing_expr(self):
        with pytest.raises(ValueError, match="missing required key"):
            QualityRuleEngine([{"type": "expression"}])

    def test_invalid_severity(self):
        with pytest.raises(ValueError, match="invalid severity 'fatal'"):
            QualityRuleEngine(
                [{"type": "not_null", "column": "id", "severity": "fatal"}]
            )

    def test_error_message_includes_rule_index(self):
        rules = [
            {"type": "not_null", "column": "id"},
            {"type": "bogus"},
        ]
        with pytest.raises(ValueError, match="Rule 1"):
            QualityRuleEngine(rules)

    def test_supported_rule_types_listed_in_error(self):
        with pytest.raises(ValueError, match="Supported types"):
            QualityRuleEngine([{"type": "nope"}])
        assert "not_null" in SUPPORTED_RULE_TYPES
        assert "expression" in SUPPORTED_RULE_TYPES


# ---------------------------------------------------------------------------
# Individual rule types
# ---------------------------------------------------------------------------


class TestNotNull:
    def test_pass(self, sample_df):
        result = evaluate_one({"type": "not_null", "column": "id"}, sample_df)
        assert result.passed is True
        assert result.failed_count == 0
        assert result.severity == "error"

    def test_fail(self):
        df = pd.DataFrame({"id": [1, None, 3, None]})
        result = evaluate_one({"type": "not_null", "column": "id"}, df)
        assert result.passed is False
        assert result.failed_count == 2
        assert "2 null value(s)" in result.message


class TestUnique:
    def test_pass(self, sample_df):
        result = evaluate_one({"type": "unique", "column": "id"}, sample_df)
        assert result.passed is True
        assert result.failed_count == 0

    def test_fail(self):
        df = pd.DataFrame({"id": [1, 2, 2, 3, 3, 3]})
        result = evaluate_one({"type": "unique", "column": "id"}, df)
        assert result.passed is False
        assert result.failed_count == 3
        assert "duplicate" in result.message


class TestInRange:
    def test_pass_with_both_bounds(self, sample_df):
        rule = {"type": "in_range", "column": "age", "min": 0, "max": 120}
        result = evaluate_one(rule, sample_df)
        assert result.passed is True

    def test_fail_below_min(self, sample_df):
        rule = {"type": "in_range", "column": "age", "min": 30}
        result = evaluate_one(rule, sample_df)
        assert result.passed is False
        assert result.failed_count == 1
        assert "outside range" in result.message
        assert "below_min" in result.metadata["violations"]

    def test_fail_above_max(self, sample_df):
        rule = {"type": "in_range", "column": "age", "max": 40}
        result = evaluate_one(rule, sample_df)
        assert result.passed is False
        assert result.failed_count == 1
        assert "above_max" in result.metadata["violations"]

    def test_min_only_pass(self, sample_df):
        rule = {"type": "in_range", "column": "age", "min": 0}
        assert evaluate_one(rule, sample_df).passed is True

    def test_max_only_pass(self, sample_df):
        rule = {"type": "in_range", "column": "age", "max": 100}
        assert evaluate_one(rule, sample_df).passed is True


class TestInSet:
    def test_pass(self, sample_df):
        rule = {
            "type": "in_set",
            "column": "status",
            "values": ["active", "inactive"],
        }
        assert evaluate_one(rule, sample_df).passed is True

    def test_fail(self, sample_df):
        rule = {"type": "in_set", "column": "status", "values": ["active"]}
        result = evaluate_one(rule, sample_df)
        assert result.passed is False
        assert result.failed_count == 2
        assert result.metadata["unexpected_values"] == ["inactive"]

    def test_nulls_are_ignored(self):
        df = pd.DataFrame({"status": ["a", None, "a"]})
        rule = {"type": "in_set", "column": "status", "values": ["a"]}
        assert evaluate_one(rule, df).passed is True


class TestMatchesRegex:
    def test_pass(self, sample_df):
        rule = {
            "type": "matches_regex",
            "column": "email",
            "pattern": r"[a-z]+@x\.com",
        }
        assert evaluate_one(rule, sample_df).passed is True

    def test_fail(self):
        df = pd.DataFrame({"email": ["good@x.com", "bad", "worse"]})
        rule = {
            "type": "matches_regex",
            "column": "email",
            "pattern": r".+@.+\..+",
        }
        result = evaluate_one(rule, df)
        assert result.passed is False
        assert result.failed_count == 2
        assert "not matching pattern" in result.message


class TestMinMaxLength:
    def test_min_length_pass(self, sample_df):
        rule = {"type": "min_length", "column": "name", "value": 3}
        assert evaluate_one(rule, sample_df).passed is True

    def test_min_length_fail(self):
        df = pd.DataFrame({"name": ["ab", "abcd", "a"]})
        rule = {"type": "min_length", "column": "name", "value": 3}
        result = evaluate_one(rule, df)
        assert result.passed is False
        assert result.failed_count == 2
        assert "shorter than minimum length 3" in result.message

    def test_max_length_pass(self, sample_df):
        rule = {"type": "max_length", "column": "name", "value": 10}
        assert evaluate_one(rule, sample_df).passed is True

    def test_max_length_fail(self, sample_df):
        rule = {"type": "max_length", "column": "name", "value": 4}
        result = evaluate_one(rule, sample_df)
        assert result.passed is False
        assert result.failed_count == 3  # Alice, Charlie, Diana
        assert "longer than maximum length 4" in result.message


class TestRowCount:
    def test_row_count_min_pass(self, sample_df):
        rule = {"type": "row_count_min", "value": 5}
        result = evaluate_one(rule, sample_df)
        assert result.passed is True
        assert result.metadata["row_count"] == 5

    def test_row_count_min_fail(self, sample_df):
        rule = {"type": "row_count_min", "value": 10}
        result = evaluate_one(rule, sample_df)
        assert result.passed is False
        assert result.failed_count == 5  # deficit of 5 rows
        assert "below minimum 10" in result.message

    def test_row_count_max_pass(self, sample_df):
        rule = {"type": "row_count_max", "value": 5}
        assert evaluate_one(rule, sample_df).passed is True

    def test_row_count_max_fail(self, sample_df):
        rule = {"type": "row_count_max", "value": 3}
        result = evaluate_one(rule, sample_df)
        assert result.passed is False
        assert result.failed_count == 2  # excess of 2 rows
        assert "above maximum 3" in result.message


class TestExpression:
    def test_pass(self, sample_df):
        rule = {"type": "expression", "expr": "age > 0"}
        assert evaluate_one(rule, sample_df).passed is True

    def test_fail(self, sample_df):
        rule = {"type": "expression", "expr": "age > 30"}
        result = evaluate_one(rule, sample_df)
        assert result.passed is False
        assert result.failed_count == 2  # ages 25 and 30
        assert "do not satisfy expression" in result.message

    def test_invalid_expression_is_failed_result(self, sample_df):
        rule = {"type": "expression", "expr": "missing_col > 0"}
        result = evaluate_one(rule, sample_df)
        assert result.passed is False
        assert "Failed to evaluate expression" in result.message

    def test_non_boolean_expression_is_failed_result(self, sample_df):
        rule = {"type": "expression", "expr": "age * 2"}
        result = evaluate_one(rule, sample_df)
        assert result.passed is False
        assert "did not produce a boolean result" in result.message


# ---------------------------------------------------------------------------
# Missing column and empty DataFrame behavior
# ---------------------------------------------------------------------------


class TestMissingColumn:
    @pytest.mark.parametrize(
        "rule",
        [
            {"type": "not_null", "column": "ghost"},
            {"type": "unique", "column": "ghost"},
            {"type": "in_range", "column": "ghost", "min": 0},
            {"type": "in_set", "column": "ghost", "values": [1]},
            {"type": "matches_regex", "column": "ghost", "pattern": ".*"},
            {"type": "min_length", "column": "ghost", "value": 1},
            {"type": "max_length", "column": "ghost", "value": 1},
        ],
    )
    def test_missing_column_fails_without_exception(self, sample_df, rule):
        result = evaluate_one(rule, sample_df)
        assert result.passed is False
        assert "not found in DataFrame" in result.message
        assert result.metadata["column_missing"] is True

    def test_missing_column_respects_severity(self, sample_df):
        rule = {"type": "not_null", "column": "ghost", "severity": "warning"}
        report = QualityRuleEngine([rule]).evaluate(sample_df)
        assert report.passed is True
        assert len(report.warnings) == 1


class TestEmptyDataFrame:
    def test_value_rules_pass_on_empty(self):
        df = pd.DataFrame({"id": pd.Series([], dtype="int64")})
        rules = [
            {"type": "not_null", "column": "id"},
            {"type": "unique", "column": "id"},
            {"type": "in_range", "column": "id", "min": 0, "max": 10},
            {"type": "expression", "expr": "id > 0"},
        ]
        report = QualityRuleEngine(rules).evaluate(df)
        assert report.passed is True
        assert all(r.passed for r in report.results)

    def test_row_count_min_fails_on_empty(self):
        df = pd.DataFrame({"id": pd.Series([], dtype="int64")})
        rule = {"type": "row_count_min", "value": 1}
        result = evaluate_one(rule, df)
        assert result.passed is False
        assert result.failed_count == 1


# ---------------------------------------------------------------------------
# Severity behavior
# ---------------------------------------------------------------------------


class TestSeverity:
    def test_warning_failure_does_not_fail_report(self):
        df = pd.DataFrame({"id": [1, None]})
        rule = {"type": "not_null", "column": "id", "severity": "warning"}
        report = QualityRuleEngine([rule]).evaluate(df)
        assert report.passed is True
        assert len(report.warnings) == 1
        assert report.failures == []

    def test_error_failure_fails_report(self):
        df = pd.DataFrame({"id": [1, None]})
        rule = {"type": "not_null", "column": "id", "severity": "error"}
        report = QualityRuleEngine([rule]).evaluate(df)
        assert report.passed is False
        assert len(report.failures) == 1
        assert report.warnings == []

    def test_mixed_severities(self):
        df = pd.DataFrame({"id": [1, None], "age": [200, 5]})
        rules = [
            {"type": "not_null", "column": "id", "severity": "warning"},
            {"type": "in_range", "column": "age", "max": 120},
            {"type": "unique", "column": "id"},
        ]
        report = QualityRuleEngine(rules).evaluate(df)
        assert report.passed is False
        assert len(report.failures) == 1
        assert len(report.warnings) == 1
        assert sum(1 for r in report.results if r.passed) == 1


# ---------------------------------------------------------------------------
# RuleResult and RuleReport serialization
# ---------------------------------------------------------------------------


class TestRuleResult:
    def test_to_dict(self):
        result = RuleResult(
            rule_type="not_null",
            column="id",
            passed=False,
            severity="error",
            message="2 null value(s) in column 'id'",
            failed_count=2,
            metadata={"name": "id_check"},
        )
        as_dict = result.to_dict()
        assert as_dict == {
            "rule_type": "not_null",
            "column": "id",
            "passed": False,
            "severity": "error",
            "message": "2 null value(s) in column 'id'",
            "failed_count": 2,
            "metadata": {"name": "id_check"},
        }

    def test_defaults(self):
        result = RuleResult(rule_type="unique", column="id", passed=True)
        assert result.severity == "error"
        assert result.message == ""
        assert result.failed_count == 0
        assert result.metadata == {}


class TestRuleReport:
    def test_empty_report_passes(self):
        report = RuleReport()
        assert report.passed is True
        assert report.results == []
        assert report.failures == []
        assert report.warnings == []

    def test_init_with_results(self):
        results = [RuleResult("not_null", "id", True)]
        report = RuleReport(results)
        assert len(report.results) == 1

    def test_to_dict(self, sample_df):
        rules = [
            {"type": "not_null", "column": "id"},
            {"type": "row_count_min", "value": 100, "severity": "warning"},
        ]
        report = QualityRuleEngine(rules).evaluate(sample_df)
        as_dict = report.to_dict()
        assert as_dict["passed"] is True
        assert as_dict["total_rules"] == 2
        assert as_dict["passed_rules"] == 1
        assert as_dict["error_failures"] == 0
        assert as_dict["warning_failures"] == 1
        assert len(as_dict["results"]) == 2

    def test_to_dict_is_json_serializable(self, sample_df):
        rules = [
            {"type": "in_set", "column": "status", "values": ["active"]},
            {"type": "in_range", "column": "age", "min": 30},
        ]
        report = QualityRuleEngine(rules).evaluate(sample_df)
        serialized = json.dumps(report.to_dict())
        assert "unexpected_values" in serialized

    def test_summary_counts(self, sample_df):
        rules = [
            {"type": "not_null", "column": "id"},
            {"type": "row_count_min", "value": 100},
            {"type": "unique", "column": "status", "severity": "warning"},
        ]
        report = QualityRuleEngine(rules).evaluate(sample_df)
        summary = report.summary()
        assert "3 rule(s)" in summary
        assert "1 passed" in summary
        assert "1 error(s)" in summary
        assert "1 warning(s)" in summary

    def test_summary_lists_failures_only(self, sample_df):
        rules = [
            {"type": "not_null", "column": "id"},
            {"type": "row_count_min", "value": 100},
        ]
        report = QualityRuleEngine(rules).evaluate(sample_df)
        summary = report.summary()
        assert "[ERROR] row_count_min" in summary
        assert "[ERROR] not_null" not in summary

    def test_summary_uses_rule_name_and_column(self, sample_df):
        rules = [
            {
                "type": "in_range",
                "column": "age",
                "max": 30,
                "name": "age_sanity",
                "severity": "warning",
            },
        ]
        report = QualityRuleEngine(rules).evaluate(sample_df)
        summary = report.summary()
        assert "[WARNING] age_sanity on 'age'" in summary

    def test_summary_all_passed_is_single_line(self, sample_df):
        rules = [{"type": "not_null", "column": "id"}]
        report = QualityRuleEngine(rules).evaluate(sample_df)
        assert "\n" not in report.summary()


# ---------------------------------------------------------------------------
# QualityRuleHook
# ---------------------------------------------------------------------------


class TestQualityRuleHook:
    def test_raises_on_error_severity_failure(self, sample_df, mock_job):
        hook = QualityRuleHook(rules=[{"type": "row_count_min", "value": 100}])
        context = HookContext(job=mock_job, phase=POST_TRANSFORM, data=sample_df)
        with pytest.raises(QualityRuleError, match="test_job"):
            hook.execute(context)

    def test_error_lists_failed_rules(self, sample_df):
        hook = QualityRuleHook(
            rules=[
                {"type": "in_range", "column": "age", "max": 30},
                {"type": "not_null", "column": "ghost", "name": "ghost_nn"},
            ]
        )
        context = HookContext(phase=POST_TRANSFORM, data=sample_df)
        with pytest.raises(QualityRuleError) as exc_info:
            hook.execute(context)
        message = str(exc_info.value)
        assert "in_range (column 'age')" in message
        assert "ghost_nn" in message
        assert exc_info.value.report is not None
        assert exc_info.value.report.passed is False

    def test_report_stored_even_when_raising(self, sample_df):
        hook = QualityRuleHook(rules=[{"type": "row_count_min", "value": 100}])
        context = HookContext(phase=POST_TRANSFORM, data=sample_df)
        with pytest.raises(QualityRuleError):
            hook.execute(context)
        report = context.metadata["quality_rule_report"]
        assert isinstance(report, RuleReport)
        assert report.passed is False

    def test_does_not_raise_on_warning_failure(self, sample_df, caplog):
        hook = QualityRuleHook(
            rules=[
                {
                    "type": "row_count_min",
                    "value": 100,
                    "severity": "warning",
                }
            ]
        )
        context = HookContext(phase=POST_TRANSFORM, data=sample_df)
        with caplog.at_level(logging.WARNING):
            hook.execute(context)
        report = context.metadata["quality_rule_report"]
        assert report.passed is True
        assert any("row_count_min" in record.message for record in caplog.records)

    def test_passing_rules_store_report(self, sample_df):
        hook = QualityRuleHook(rules=[{"type": "not_null", "column": "id"}])
        context = HookContext(phase=POST_TRANSFORM, data=sample_df)
        hook.execute(context)
        report = context.metadata["quality_rule_report"]
        assert report.passed is True

    def test_ignores_non_dataframe_data(self):
        hook = QualityRuleHook(rules=[{"type": "row_count_min", "value": 1}])
        context = HookContext(phase=POST_TRANSFORM, data={"not": "a df"})
        hook.execute(context)
        assert "quality_rule_report" not in context.metadata

    def test_ignores_none_data(self):
        hook = QualityRuleHook(rules=[{"type": "row_count_min", "value": 1}])
        context = HookContext(phase=POST_TRANSFORM, data=None)
        hook.execute(context)
        assert "quality_rule_report" not in context.metadata

    def test_constructed_with_engine(self, sample_df):
        engine = QualityRuleEngine([{"type": "not_null", "column": "id"}])
        hook = QualityRuleHook(engine=engine)
        context = HookContext(phase=POST_TRANSFORM, data=sample_df)
        hook.execute(context)
        assert context.metadata["quality_rule_report"].passed is True

    def test_invalid_rules_raise_at_construction(self):
        with pytest.raises(ValueError, match="unknown rule type"):
            QualityRuleHook(rules=[{"type": "bogus"}])

    def test_registered_at_post_transform(self, sample_df):
        hook = QualityRuleHook(
            rules=[
                {
                    "type": "row_count_min",
                    "value": 100,
                    "severity": "warning",
                }
            ]
        )
        register_hook(POST_TRANSFORM, hook)
        context = HookContext(phase=POST_TRANSFORM, data=sample_df)
        execute_hooks(POST_TRANSFORM, context)
        assert context.metadata["quality_rule_report"].passed is True
