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

import json
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

    def to_json(self, indent: int = 2) -> str:
        """Return the report as a JSON string."""
        return json.dumps(self.to_dict(), indent=indent, default=str)

    def to_html(self, max_row_samples: int = 5) -> str:
        """Return a minimal HTML page summarizing the report."""
        rows = ""
        for r in self._results:
            status = "PASS" if r.passed else "FAIL"
            color = "green" if r.passed else "red"
            rows += (
                f"<tr>"
                f"<td>{r.rule_type}</td>"
                f"<td>{r.column or '-'}</td>"
                f'<td style="color:{color};font-weight:bold;">{status}</td>'
                f"<td>{r.severity}</td>"
                f"<td>{r.message}</td>"
                f"<td>{r.failed_count}</td>"
                f"</tr>\n"
            )
        return (
            "<!DOCTYPE html>\n<html>\n<head><meta charset='utf-8'>"
            "<title>Quality Rule Report</title></head>\n<body>\n"
            "<h1>Quality Rule Report</h1>\n"
            f"<p><b>Passed:</b> {self.passed} | "
            f"<b>Total:</b> {len(self._results)} | "
            f"<b>Passed rules:</b> {sum(1 for r in self._results if r.passed)} | "
            f"<b>Errors:</b> {len(self.failures)} | "
            f"<b>Warnings:</b> {len(self.warnings)}</p>\n"
            '<table border="1" cellpadding="6" cellspacing="0">\n'
            "<tr><th>Rule Type</th><th>Column</th><th>Status</th>"
            "<th>Severity</th><th>Message</th><th>Failed Count</th></tr>\n"
            f"{rows}"
            "</table>\n</body>\n</html>"
        )

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


class QualityReportArtifact:
    """Generate an HTML/JSON artifact from a quality rule evaluation.

    Includes pass/fail per rule, a summary, and optional row samples
    for rules that failed. Designed to be written to a file path that
    can be uploaded as a CI artifact (e.g. ``quality_report.html``).
    """

    def __init__(
        self,
        report: RuleReport,
        df: Optional[pd.DataFrame] = None,
        max_row_samples: int = 5,
        output_path: str = "quality_report.html",
        report_format: str = "html",
    ):
        self.report = report
        self.df = df
        self.max_row_samples = max_row_samples
        self.output_path = output_path
        self.report_format = report_format

    def write(self, path: Optional[str] = None) -> str:
        """Write the artifact to a file and return the file path."""
        path = path or self.output_path
        fmt = self.report_format
        if fmt == "json":
            content = self.to_json()
        elif fmt == "html":
            content = self.to_html()
        else:
            raise ValueError(f"Unsupported report format: {fmt!r}")
        import pathlib

        pathlib.Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        return path

    def to_json(self) -> str:
        """Return the artifact as a JSON string with row samples."""
        samples = self._row_samples()
        payload = {
            "passed": self.report.passed,
            "total_rules": len(self.report.results),
            "passed_rules": sum(1 for r in self.report.results if r.passed),
            "error_failures": len(self.report.failures),
            "warning_failures": len(self.report.warnings),
            "results": [r.to_dict() for r in self.report.results],
            "row_samples": samples,
        }
        return json.dumps(payload, indent=2, default=str)

    def to_html(self) -> str:
        """Return the artifact as an HTML page with row samples."""
        report_table_rows = ""
        for r in self.report.results:
            status = "PASS" if r.passed else "FAIL"
            color = "green" if r.passed else "red"
            report_table_rows += (
                f"<tr>"
                f"<td>{r.rule_type}</td>"
                f"<td>{r.column or '-'}</td>"
                f'<td style="color:{color};font-weight:bold;">{status}</td>'
                f"<td>{r.severity}</td>"
                f"<td>{r.message}</td>"
                f"<td>{r.failed_count}</td>"
                f"</tr>\n"
            )

        samples = self._row_samples()
        sample_sections = []
        for rule_key, sample_df in samples.items():
            if sample_df is None or sample_df.empty:
                continue
            sample_rows = ""
            for _, row in sample_df.iterrows():
                cells = [f"<td>{str(v)}</td>" for v in row.values]
                sample_rows += f"<tr>{''.join(cells)}</tr>\n"
            headers = [f"<th>{c}</th>" for c in sample_df.columns]
            sample_sections.append(
                f"<h2>Failed rows for: {rule_key}</h2>\n"
                f'<table border="1" cellpadding="4" cellspacing="0">\n'
                f"<tr>{''.join(headers)}</tr>\n"
                f"{sample_rows}"
                f"</table>\n"
            )

        sample_html = (
            "\n".join(sample_sections)
            if sample_sections
            else ("<p>No failing row samples available.</p>\n")
        )

        return (
            "<!DOCTYPE html>\n<html>\n<head>"
            "<meta charset='utf-8'><title>Data Quality Report Artifact</title>"
            "<style>"
            "body{font-family:Arial,sans-serif;margin:2rem;background:#f8f9fa;}"
            "h1{color:#2c3e50;}"
            "table{border-collapse:collapse;width:100%;background:#fff;}"
            "th{background:#2c3e50;color:#fff;text-align:left;padding:8px;}"
            "td{padding:8px;border-bottom:1px solid #ddd;}"
            "</style></head>\n<body>\n"
            "<h1>Data Quality Report Artifact</h1>\n"
            f"<p><b>Overall:</b> {self.report.passed}</p>\n"
            f"<p><b>Rules evaluated:</b> {len(self.report.results)} | "
            f"<b>Passed:</b> {sum(1 for r in self.report.results if r.passed)} | "
            f"<b>Errors:</b> {len(self.report.failures)} | "
            f"<b>Warnings:</b> {len(self.report.warnings)}</p>\n"
            "<h2>Rule Results</h2>\n"
            '<table border="1" cellpadding="6" cellspacing="0">\n'
            "<tr><th>Rule Type</th><th>Column</th><th>Status</th>"
            "<th>Severity</th><th>Message</th><th>Failed Count</th></tr>\n"
            f"{report_table_rows}"
            "</table>\n"
            "<h2>Row Samples (Failed Rules)</h2>\n"
            f"{sample_html}\n"
            "</body>\n</html>"
        )

    def _row_samples(self) -> Dict[str, pd.DataFrame]:
        """Extract sample failing rows per failed rule from the data."""
        samples: Dict[str, pd.DataFrame] = {}
        if self.df is None or self.df.empty:
            return samples
        for r in self.report.results:
            if r.passed:
                continue
            label = r.metadata.get("name") or r.rule_type
            key = f"{label} (column '{r.column}')" if r.column else label
            try:
                if r.rule_type == "not_null":
                    mask = self.df[r.column].isna()
                elif r.rule_type == "unique":
                    dup_mask = self.df[r.column].duplicated(keep=False)
                    mask = dup_mask
                elif r.rule_type == "in_range":
                    series = self.df[r.column].dropna()
                    min_v = (
                        r.metadata.get("violations", {})
                        .get("below_min", {})
                        .get("min_value")
                    )
                    max_v = (
                        r.metadata.get("violations", {})
                        .get("above_max", {})
                        .get("max_value")
                    )
                    if min_v is not None and max_v is not None:
                        mask = (self.df[r.column] < min_v) | (self.df[r.column] > max_v)
                    elif min_v is not None:
                        mask = self.df[r.column] < min_v
                    elif max_v is not None:
                        mask = self.df[r.column] > max_v
                    else:
                        mask = pd.Series(False, index=self.df.index)
                elif r.rule_type == "in_set":
                    unexpected = r.metadata.get("unexpected_values", [])
                    mask = pd.Series(False, index=self.df.index)
                    if r.column in self.df.columns and unexpected:
                        mask = self.df[r.column].isin(unexpected)
                elif r.rule_type == "matches_regex":
                    pattern = r.metadata.get("pattern", "")
                    mask = pd.Series(False, index=self.df.index)
                    if r.column in self.df.columns and pattern:
                        try:
                            series = self.df[r.column].dropna().astype(str)
                            matched = series.str.fullmatch(pattern)
                            matched_series = pd.Series(False, index=self.df.index)
                            matched_series.loc[series.index] = matched.values
                            mask = ~matched_series
                        except Exception:
                            pass
                elif r.rule_type == "expression":
                    expr = r.metadata.get("expr") or r.rule_type
                    try:
                        result = self.df.eval(expr)
                        if isinstance(result, pd.Series) and result.dtype == bool:
                            mask = ~result
                        else:
                            mask = pd.Series(False, index=self.df.index)
                    except Exception:
                        mask = pd.Series(False, index=self.df.index)
                elif r.rule_type in ("min_length", "max_length"):
                    value = r.metadata.get("value", 0)
                    lengths = self.df[r.column].dropna().astype(str).str.len()
                    if r.rule_type == "min_length":
                        mask = lengths < value
                    else:
                        mask = lengths > value
                else:
                    mask = pd.Series(False, index=self.df.index)

                # Convert boolean mask to same index as df
                if isinstance(mask, pd.Series) and mask.index.equals(self.df.index):
                    sampled = self.df[mask].head(self.max_row_samples)
                else:
                    sampled = self.df.iloc[: self.max_row_samples]
                if not sampled.empty:
                    samples[key] = sampled
            except Exception as exc:
                # Best-effort: don't crash artifact generation for bad samples
                samples[key] = pd.DataFrame({"error": [str(exc)]})
        return samples


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
                    {"pattern": pattern},
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
                    {"value": value},
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
                {"expr": expr},
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
        emit_report: bool = False,
        report_path: str = "quality_report.html",
        report_format: str = "html",
        max_row_samples: int = 5,
    ):
        """Initialize the hook.

        Args:
            rules: Rule dictionaries used to build an engine. Ignored
                when *engine* is provided.
            engine: A pre-built QualityRuleEngine to use directly.
            emit_report: If True, write a quality report artifact after
                evaluating rules.
            report_path: File path for the emitted artifact.
            report_format: ``"html"`` or ``"json"``.
            max_row_samples: Max failing rows to include per rule.

        Raises:
            ValueError: If any rule definition is invalid.
        """
        self._engine = engine or QualityRuleEngine(rules or [])
        self._logger = logging.getLogger(f"{__name__}.QualityRuleHook")
        self.emit_report = emit_report
        self.report_path = report_path
        self.report_format = report_format
        self.max_row_samples = max_row_samples

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

        # Optionally emit an artifact for CI upload
        if self.emit_report:
            try:
                artifact = QualityReportArtifact(
                    report=report,
                    df=context.data,
                    max_row_samples=self.max_row_samples,
                    output_path=self.report_path,
                    report_format=self.report_format,
                )
                path = artifact.write()
                self._logger.info(
                    "[QualityRuleHook] Job '%s': quality report artifact emitted: %s",
                    job_name,
                    path,
                )
            except Exception as exc:
                self._logger.warning(
                    "[QualityRuleHook] Job '%s': failed to emit quality report: %s",
                    job_name,
                    exc,
                    exc_info=True,
                )

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
