"""
Declarative data quality rules engine for SimpleETL.

Evaluates a list of rule dictionaries (typically supplied via
``ETLJobConfig.validation_rules``) against a pandas DataFrame and
produces a structured :class:`RuleReport`. Also provides
:class:`QualityRuleHook` for running the rules automatically as part
of the hook lifecycle (intended for the ``post_transform`` point).

Example rule definitions::

    {"type": "not_null", "column": "id", "severity": "error"}
    {"type": "in_range", "column": "age", "min": 0, "max": 120}
    {"type": "expression", "expr": "price > 0", "severity": "warning"}
"""

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from .hooks import Hook, HookContext
from .quality import DataQualityError, check_duplicates, check_value_range

logger = logging.getLogger(__name__)

SEVERITY_ERROR = "error"
SEVERITY_WARNING = "warning"
VALID_SEVERITIES = (SEVERITY_ERROR, SEVERITY_WARNING)

# Required keys per rule type ("severity" and "name" are always optional).
_RULE_REQUIRED_KEYS: Dict[str, List[str]] = {
    "not_null": ["column"],
    "unique": ["column"],
    "in_range": ["column"],  # plus "min" and/or "max", checked separately
    "in_set": ["column", "values"],
    "matches_regex": ["column", "pattern"],
    "min_length": ["column", "value"],
    "max_length": ["column", "value"],
    "row_count_min": ["value"],
    "row_count_max": ["value"],
    "expression": ["expr"],
}

SUPPORTED_RULE_TYPES = sorted(_RULE_REQUIRED_KEYS)


class QualityRuleError(Exception):
    """Raised when error-severity quality rules fail."""

    def __init__(self, message: str, report: Optional["RuleReport"] = None):
        self.report = report
        super().__init__(message)


@dataclass
class RuleResult:
    """Result of evaluating a single declarative quality rule.

    Attributes:
        rule_type: The rule type (e.g. ``not_null``).
        column: The column the rule applies to, if any.
        passed: Whether the rule passed.
        severity: ``error`` or ``warning``.
        message: Human-readable description of the outcome.
        failed_count: Number of failing values/rows (0 when passed).
        metadata: Additional details about the evaluation.
    """

    rule_type: str
    column: Optional[str]
    passed: bool
    severity: str = SEVERITY_ERROR
    message: str = ""
    failed_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Return the result as a plain dictionary."""
        return {
            "rule_type": self.rule_type,
            "column": self.column,
            "passed": self.passed,
            "severity": self.severity,
            "message": self.message,
            "failed_count": self.failed_count,
            "metadata": self.metadata,
        }


class RuleReport:
    """Collects results from evaluating a set of quality rules."""

    def __init__(self, results: Optional[List[RuleResult]] = None):
        """Initialize the report.

        Args:
            results: Optional initial list of rule results.
        """
        self._results: List[RuleResult] = list(results or [])

    def add(self, result: RuleResult) -> None:
        """Add a rule result to the report.

        Args:
            result: The result to record.
        """
        self._results.append(result)

    @property
    def results(self) -> List[RuleResult]:
        """Return all recorded rule results."""
        return list(self._results)

    @property
    def failures(self) -> List[RuleResult]:
        """Return failed results with ``error`` severity."""
        return [
            r for r in self._results if not r.passed and r.severity == SEVERITY_ERROR
        ]

    @property
    def warnings(self) -> List[RuleResult]:
        """Return failed results with ``warning`` severity."""
        return [
            r for r in self._results if not r.passed and r.severity == SEVERITY_WARNING
        ]

    @property
    def passed(self) -> bool:
        """Return True if no error-severity rules failed."""
        return not self.failures

    def to_dict(self) -> Dict[str, Any]:
        """Return the report as a plain dictionary."""
        return {
            "passed": self.passed,
            "total_rules": len(self._results),
            "passed_rules": sum(1 for r in self._results if r.passed),
            "error_failures": len(self.failures),
            "warning_failures": len(self.warnings),
            "results": [r.to_dict() for r in self._results],
        }

    def summary(self) -> str:
        """Return a human-readable summary of the report."""
        passed_count = sum(1 for r in self._results if r.passed)
        lines = [
            f"Quality rule report: {len(self._results)} rule(s), "
            f"{passed_count} passed, {len(self.failures)} error(s), "
            f"{len(self.warnings)} warning(s)"
        ]
        for result in self._results:
            if result.passed:
                continue
            label = result.metadata.get("name") or result.rule_type
            target = f" on '{result.column}'" if result.column else ""
            lines.append(
                f"  [{result.severity.upper()}] {label}{target}: {result.message}"
            )
        return "\n".join(lines)


class QualityRuleEngine:
    """Evaluates declarative data quality rules against DataFrames.

    Rules are plain dictionaries with a ``type`` key plus type-specific
    keys (see :data:`SUPPORTED_RULE_TYPES`). Every rule also accepts an
    optional ``severity`` (``error`` by default, or ``warning``) and an
    optional ``name`` used for display purposes.
    """

    def __init__(self, rules: List[Dict[str, Any]]):
        """Validate rule definitions eagerly and store them.

        Args:
            rules: List of rule dictionaries.

        Raises:
            ValueError: If a rule has an unknown type, is missing
                required keys, or has invalid values.
        """
        self._rules: List[Dict[str, Any]] = [
            self._validate_rule(index, rule) for index, rule in enumerate(rules)
        ]

    @property
    def rules(self) -> List[Dict[str, Any]]:
        """Return copies of the validated rule definitions."""
        return [dict(rule) for rule in self._rules]

    @staticmethod
    def _validate_rule(index: int, rule: Any) -> Dict[str, Any]:
        """Validate a single rule definition.

        Args:
            index: Position of the rule in the rules list.
            rule: The rule definition to validate.

        Returns:
            A copy of the validated rule dictionary.

        Raises:
            ValueError: If the rule definition is invalid.
        """
        if not isinstance(rule, dict):
            raise ValueError(f"Rule {index} must be a dict, got {type(rule).__name__}")

        rule_type = rule.get("type")
        if rule_type not in _RULE_REQUIRED_KEYS:
            raise ValueError(
                f"Rule {index}: unknown rule type {rule_type!r}. "
                f"Supported types: {SUPPORTED_RULE_TYPES}"
            )

        missing = [key for key in _RULE_REQUIRED_KEYS[rule_type] if key not in rule]
        if missing:
            raise ValueError(
                f"Rule {index} ('{rule_type}'): missing required key(s): {missing}"
            )

        if rule_type == "in_range" and "min" not in rule and "max" not in rule:
            raise ValueError(
                f"Rule {index} ('in_range'): requires at least one of 'min' or 'max'"
            )

        if rule_type == "in_set" and not isinstance(rule["values"], (list, tuple, set)):
            raise ValueError(
                f"Rule {index} ('in_set'): 'values' must be a list, got "
                f"{type(rule['values']).__name__}"
            )

        if rule_type == "matches_regex":
            try:
                re.compile(rule["pattern"])
            except re.error as exc:
                raise ValueError(
                    f"Rule {index} ('matches_regex'): invalid pattern "
                    f"{rule['pattern']!r}: {exc}"
                ) from exc

        severity = rule.get("severity", SEVERITY_ERROR)
        if severity not in VALID_SEVERITIES:
            raise ValueError(
                f"Rule {index}: invalid severity {severity!r}. "
                f"Valid severities: {list(VALID_SEVERITIES)}"
            )

        return dict(rule)

    def evaluate(self, df: pd.DataFrame) -> RuleReport:
        """Evaluate all rules against a DataFrame.

        A rule referencing a column that is missing from the DataFrame
        produces a failed result rather than raising an exception.

        Args:
            df: The DataFrame to validate.

        Returns:
            A RuleReport with one result per rule.
        """
        report = RuleReport()
        for rule in self._rules:
            report.add(self._evaluate_rule(rule, df))
        return report

    def _evaluate_rule(self, rule: Dict[str, Any], df: pd.DataFrame) -> RuleResult:
        """Evaluate a single rule against a DataFrame."""
        rule_type: str = rule["type"]
        severity: str = rule.get("severity", SEVERITY_ERROR)
        column: Optional[str] = rule.get("column")
        metadata: Dict[str, Any] = {}
        if rule.get("name"):
            metadata["name"] = rule["name"]

        if column is not None and column not in df.columns:
            metadata["column_missing"] = True
            return RuleResult(
                rule_type=rule_type,
                column=column,
                passed=False,
                severity=severity,
                message=f"Column '{column}' not found in DataFrame",
                failed_count=0,
                metadata=metadata,
            )

        passed, message, failed_count, extra = self._check(rule_type, rule, df)
        metadata.update(extra)
        if passed and not message:
            target = f" on column '{column}'" if column else ""
            message = f"Rule '{rule_type}'{target} passed"

        return RuleResult(
            rule_type=rule_type,
            column=column,
            passed=passed,
            severity=severity,
            message=message,
            failed_count=failed_count,
            metadata=metadata,
        )

    def _check(
        self, rule_type: str, rule: Dict[str, Any], df: pd.DataFrame
    ) -> Tuple[bool, str, int, Dict[str, Any]]:
        """Run the check for a rule type.

        Returns:
            Tuple of (passed, failure message, failed count, extra
            metadata). The message is empty when the rule passed.
        """
        column = rule.get("column")

        if rule_type == "not_null":
            failed = int(df[column].isna().sum())
            if failed:
                return (
                    False,
                    f"{failed} null value(s) in column '{column}'",
                    failed,
                    {},
                )
            return True, "", 0, {}

        if rule_type == "unique":
            try:
                check_duplicates(df, [str(column)], threshold=0.0)
            except DataQualityError as exc:
                failed = int(exc.details.get("duplicate_count", 0))
                return (
                    False,
                    f"{failed} duplicate value(s) in column '{column}'",
                    failed,
                    {},
                )
            return True, "", 0, {}

        if rule_type == "in_range":
            min_value = rule.get("min")
            max_value = rule.get("max")
            try:
                check_value_range(df, str(column), min_value, max_value)
            except DataQualityError as exc:
                violations = exc.details.get("violations", {})
                failed = sum(int(v.get("count", 0)) for v in violations.values())
                return (
                    False,
                    f"{failed} value(s) outside range "
                    f"[{min_value}, {max_value}] in column '{column}'",
                    failed,
                    {"violations": violations},
                )
            return True, "", 0, {}

        if rule_type == "in_set":
            allowed = list(rule["values"])
            series = df[column].dropna()
            mask = ~series.isin(allowed)
            failed = int(mask.sum())
            if failed:
                unexpected = series[mask].unique().tolist()[:10]
                return (
                    False,
                    f"{failed} value(s) not in allowed set in column '{column}'",
                    failed,
                    {"unexpected_values": unexpected},
                )
            return True, "", 0, {}

        if rule_type == "matches_regex":
            pattern = rule["pattern"]
            series = df[column].dropna().astype(str)
            matched = series.str.fullmatch(pattern)
            failed = int((~matched).sum())
            if failed:
                return (
                    False,
                    f"{failed} value(s) not matching pattern "
                    f"'{pattern}' in column '{column}'",
                    failed,
                    {},
                )
            return True, "", 0, {}

        if rule_type in ("min_length", "max_length"):
            value = int(rule["value"])
            lengths = df[column].dropna().astype(str).str.len()
            if rule_type == "min_length":
                failed = int((lengths < value).sum())
                comparison = "shorter than minimum length"
            else:
                failed = int((lengths > value).sum())
                comparison = "longer than maximum length"
            if failed:
                return (
                    False,
                    f"{failed} value(s) {comparison} {value} in column '{column}'",
                    failed,
                    {},
                )
            return True, "", 0, {}

        if rule_type in ("row_count_min", "row_count_max"):
            value = int(rule["value"])
            row_count = len(df)
            extra = {"row_count": row_count}
            if rule_type == "row_count_min" and row_count < value:
                return (
                    False,
                    f"Row count {row_count} below minimum {value}",
                    value - row_count,
                    extra,
                )
            if rule_type == "row_count_max" and row_count > value:
                return (
                    False,
                    f"Row count {row_count} above maximum {value}",
                    row_count - value,
                    extra,
                )
            return True, "", 0, extra

        # rule_type == "expression" (validated eagerly in __init__)
        expr = rule["expr"]
        try:
            result = df.eval(expr)
        except Exception as exc:  # noqa: BLE001 - report, don't crash
            return (
                False,
                f"Failed to evaluate expression '{expr}': {exc}",
                0,
                {},
            )
        if not isinstance(result, pd.Series) or result.dtype != bool:
            return (
                False,
                f"Expression '{expr}' did not produce a boolean result",
                0,
                {},
            )
        failed = int((~result).sum())
        if failed:
            return (
                False,
                f"{failed} row(s) do not satisfy expression '{expr}'",
                failed,
                {},
            )
        return True, "", 0, {}


class QualityRuleHook(Hook):
    """Hook that evaluates declarative quality rules on hook data.

    Intended to be registered at the ``post_transform`` hook point.
    When the context data is a DataFrame, the rules are evaluated and
    the resulting :class:`RuleReport` is stored in
    ``context.metadata["quality_rule_report"]``. Warning-severity
    failures are logged; error-severity failures raise
    :class:`QualityRuleError`.
    """

    name = "quality_rules"
    priority = 0

    def __init__(
        self,
        rules: Optional[List[Dict[str, Any]]] = None,
        engine: Optional[QualityRuleEngine] = None,
    ):
        """Initialize the hook.

        Args:
            rules: Rule dictionaries used to build an engine. Ignored
                when *engine* is provided.
            engine: A pre-built QualityRuleEngine to use directly.

        Raises:
            ValueError: If any rule definition is invalid.
        """
        self._engine = engine or QualityRuleEngine(rules or [])
        self._logger = logging.getLogger(f"{__name__}.QualityRuleHook")

    def execute(self, context: HookContext) -> None:
        """Evaluate the rules against the context data.

        Args:
            context: The hook context. Ignored unless ``context.data``
                is a pandas DataFrame.

        Raises:
            QualityRuleError: If any error-severity rules fail.
        """
        if not isinstance(context.data, pd.DataFrame):
            return

        report = self._engine.evaluate(context.data)
        context.metadata["quality_rule_report"] = report
        job_name = context.job.config.name if context.job else "unknown"

        for result in report.warnings:
            self._logger.warning(
                "[QualityRuleHook] Job '%s': rule '%s' failed: %s",
                job_name,
                result.metadata.get("name") or result.rule_type,
                result.message,
            )

        if not report.passed:
            failed_rules = ", ".join(
                str(r.metadata.get("name") or r.rule_type)
                + (f" (column '{r.column}')" if r.column else "")
                for r in report.failures
            )
            raise QualityRuleError(
                f"Quality rules failed for job '{job_name}': {failed_rules}",
                report=report,
            )
