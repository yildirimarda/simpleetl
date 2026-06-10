"""
Schema drift detection for SimpleETL.

Compares the schema of incoming data against the latest baseline
registered in a schema registry and reacts according to the configured
drift policy: ``fail`` (raise), ``warn`` (log and continue), or
``evolve`` (register the new schema version).
"""

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd

from simpleetl.core.config import SchemaDriftConfig
from simpleetl.core.hooks import Hook, HookContext
from simpleetl.core.schema import Schema, SchemaDiff
from simpleetl.core.schema_registry import FileSchemaRegistry, SchemaRegistry

logger = logging.getLogger(__name__)

#: Valid values for ``SchemaDriftConfig.on_drift``.
VALID_DRIFT_ACTIONS = ("fail", "warn", "evolve")


class SchemaDriftError(Exception):
    """Raised when schema drift is detected and the policy is ``fail``.

    Attributes:
        diff: The SchemaDiff describing the detected drift, if available.
    """

    def __init__(self, message: str, diff: Optional[SchemaDiff] = None):
        self.diff = diff
        super().__init__(message)


@dataclass
class DriftReport:
    """Result of a schema drift check.

    Attributes:
        schema_name: Name of the schema that was checked.
        drifted: Whether drift was detected against the baseline.
        baseline_version: Version of the baseline schema used for the
            comparison, or ``None`` when no baseline was registered.
        diff: The SchemaDiff between baseline and current schema, or
            ``None`` when there was no baseline or no changes.
        action_taken: What the detector did (``"none"``,
            ``"baseline_registered"``, ``"warned"``, or ``"evolved"``).
    """

    schema_name: str
    drifted: bool
    baseline_version: Optional[int] = None
    diff: Optional[SchemaDiff] = None
    action_taken: str = "none"

    def to_dict(self) -> Dict[str, Any]:
        """Serialize the report to a dictionary."""
        return {
            "schema_name": self.schema_name,
            "drifted": self.drifted,
            "baseline_version": self.baseline_version,
            "diff": self.diff.to_dict() if self.diff is not None else None,
            "action_taken": self.action_taken,
        }

    def summary(self) -> str:
        """Return a human-readable summary of the drift check."""
        if not self.drifted:
            return (
                f"Schema '{self.schema_name}': no drift detected "
                f"(action: {self.action_taken})"
            )

        lines: List[str] = [
            f"Schema '{self.schema_name}' drifted from baseline "
            f"version {self.baseline_version} "
            f"(action: {self.action_taken})"
        ]
        if self.diff is not None:
            if self.diff.added_columns:
                lines.append(
                    f"  added columns: {self.diff.added_columns}"
                )
            if self.diff.removed_columns:
                lines.append(
                    f"  removed columns: {self.diff.removed_columns}"
                )
            for name, change in self.diff.type_changes.items():
                lines.append(
                    f"  type change '{name}': "
                    f"{change['old']} -> {change['new']}"
                )
            for name, nchange in self.diff.nullability_changes.items():
                lines.append(
                    f"  nullability change '{name}': "
                    f"{nchange['old']} -> {nchange['new']}"
                )
        return "\n".join(lines)


class SchemaDriftDetector:
    """Detects schema drift between a DataFrame and a registered baseline.

    The detector infers the schema of the incoming DataFrame, compares it
    to the latest version registered under the schema name, and acts
    according to the configured ``on_drift`` policy.

    Example::

        config = SchemaDriftConfig(enabled=True, on_drift="warn")
        detector = SchemaDriftDetector(config)
        report = detector.check(df, "users")
        if report.drifted:
            print(report.summary())
    """

    def __init__(
        self,
        config: SchemaDriftConfig,
        registry: Optional[SchemaRegistry] = None,
    ) -> None:
        """Initialize the detector.

        Args:
            config: Schema drift configuration.
            registry: Optional schema registry. When omitted, a
                :class:`FileSchemaRegistry` is created at
                ``config.registry_path``.

        Raises:
            ValueError: If ``config.on_drift`` is not one of
                ``fail``, ``warn``, or ``evolve``.
        """
        if config.on_drift not in VALID_DRIFT_ACTIONS:
            raise ValueError(
                f"Invalid on_drift value '{config.on_drift}'. "
                f"Valid values: {list(VALID_DRIFT_ACTIONS)}"
            )
        self._config = config
        self._registry: SchemaRegistry = registry or FileSchemaRegistry(
            config.registry_path
        )

    @property
    def registry(self) -> SchemaRegistry:
        """Return the schema registry used by this detector."""
        return self._registry

    def check(self, df: pd.DataFrame, schema_name: str) -> DriftReport:
        """Check a DataFrame for schema drift against the baseline.

        Args:
            df: DataFrame whose schema is checked.
            schema_name: Name under which the baseline is registered.

        Returns:
            A DriftReport describing the outcome.

        Raises:
            SchemaDriftError: If drift is detected and the configured
                policy is ``fail``.
        """
        current = Schema.from_dataframe(df)

        try:
            versions = self._registry.list_versions(schema_name)
        except KeyError:
            versions = []

        if not versions:
            return self._handle_no_baseline(current, schema_name)

        baseline_version = max(versions)
        baseline = self._registry.get_schema(schema_name, baseline_version)
        diff = baseline.diff(current)

        if not diff.has_changes:
            logger.debug(
                "No schema drift for '%s' (baseline v%d)",
                schema_name,
                baseline_version,
            )
            return DriftReport(
                schema_name=schema_name,
                drifted=False,
                baseline_version=baseline_version,
                diff=None,
                action_taken="none",
            )

        report = DriftReport(
            schema_name=schema_name,
            drifted=True,
            baseline_version=baseline_version,
            diff=diff,
            action_taken="none",
        )

        if self._config.on_drift == "fail":
            report.action_taken = "failed"
            raise SchemaDriftError(report.summary(), diff=diff)

        if self._config.on_drift == "evolve":
            evolved = baseline.evolve(
                current,
                allow_type_changes=True,
                allow_nullability_changes=True,
            )
            new_version = baseline_version + 1
            self._registry.register_schema(schema_name, new_version, evolved)
            report.action_taken = "evolved"
            logger.info(
                "Schema '%s' evolved to version %d after drift: %s",
                schema_name,
                new_version,
                diff.to_dict(),
            )
            return report

        # on_drift == "warn"
        report.action_taken = "warned"
        logger.warning("Schema drift detected: %s", report.summary())
        return report

    def _handle_no_baseline(
        self, current: Schema, schema_name: str
    ) -> DriftReport:
        """Handle the first run when no baseline schema is registered."""
        if self._config.auto_register:
            self._registry.register_schema(schema_name, 1, current)
            logger.info(
                "Registered baseline schema '%s' version 1", schema_name
            )
            return DriftReport(
                schema_name=schema_name,
                drifted=False,
                baseline_version=None,
                diff=None,
                action_taken="baseline_registered",
            )

        logger.debug(
            "No baseline for schema '%s' and auto_register is disabled",
            schema_name,
        )
        return DriftReport(
            schema_name=schema_name,
            drifted=False,
            baseline_version=None,
            diff=None,
            action_taken="none",
        )


class SchemaDriftHook(Hook):
    """Hook that runs schema drift detection on extracted data.

    Intended for registration at the ``POST_EXTRACT`` hook point. When the
    hook context carries a DataFrame, the detector checks it against the
    registered baseline and stores the resulting :class:`DriftReport` in
    ``context.metadata["schema_drift_report"]``.

    The schema name is resolved in order of precedence: the explicit
    *schema_name* constructor argument, ``config.schema_name``, the job
    name from ``context.job``, and finally ``"default"``.
    """

    name = "schema_drift"
    priority = 0

    def __init__(
        self,
        config: SchemaDriftConfig,
        schema_name: Optional[str] = None,
        registry: Optional[SchemaRegistry] = None,
    ) -> None:
        """Initialize the hook.

        Args:
            config: Schema drift configuration.
            schema_name: Optional override for the schema name.
            registry: Optional schema registry passed to the detector.

        Raises:
            ValueError: If ``config.on_drift`` is invalid.
        """
        self._config = config
        self._schema_name = schema_name
        self._detector = SchemaDriftDetector(config, registry=registry)

    def execute(self, context: HookContext) -> None:
        """Run drift detection when the context data is a DataFrame.

        Args:
            context: The hook context for this invocation.
        """
        if not isinstance(context.data, pd.DataFrame):
            return

        schema_name = (
            self._schema_name
            or self._config.schema_name
            or self._resolve_job_name(context)
        )
        report = self._detector.check(context.data, schema_name)
        context.metadata["schema_drift_report"] = report

    @staticmethod
    def _resolve_job_name(context: HookContext) -> str:
        """Resolve the job name from the context, falling back to default."""
        if context.job is not None:
            config = getattr(context.job, "config", None)
            job_name = getattr(config, "name", None)
            if job_name:
                return str(job_name)
        return "default"
