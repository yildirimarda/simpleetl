"""
Hook / interceptor mechanism for SimpleETL.

Provides hook points throughout the ETL lifecycle, a hook registry,
built-in hook implementations, and a context object passed to every hook.
"""

import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .job import ETLJob

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Hook point constants
# ---------------------------------------------------------------------------

PRE_EXTRACT = "pre_extract"
POST_EXTRACT = "post_extract"
PRE_TRANSFORM = "pre_transform"
POST_TRANSFORM = "post_transform"
PRE_LOAD = "pre_load"
POST_LOAD = "post_load"
ON_ERROR = "on_error"
ON_COMPLETE = "on_complete"

ALL_HOOK_POINTS = [
    PRE_EXTRACT,
    POST_EXTRACT,
    PRE_TRANSFORM,
    POST_TRANSFORM,
    PRE_LOAD,
    POST_LOAD,
    ON_ERROR,
    ON_COMPLETE,
]


# ---------------------------------------------------------------------------
# HookContext
# ---------------------------------------------------------------------------

@dataclass
class HookContext:
    """Context object passed to every hook invocation.

    Attributes:
        job: The ETLJob instance that triggered the hook.
        phase: The current hook point name (e.g. ``pre_extract``).
        data: The data being processed (may be None depending on phase).
        error: The exception, if any (only set for ``on_error`` hooks).
        metadata: Arbitrary metadata dict for cross-hook communication.
        start_time: Timestamp when the current phase started.
        extra: Additional keyword arguments for extensibility.
    """

    job: Optional["ETLJob"] = None
    phase: str = ""
    data: Any = None
    error: Optional[Exception] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    start_time: float = 0.0
    extra: Dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Hook base class
# ---------------------------------------------------------------------------

class Hook(ABC):
    """Base class for all hooks.

    Subclasses override ``execute`` to define hook behaviour.
    The ``priority`` attribute controls execution order (higher runs first).
    """

    name: str = ""
    priority: int = 0

    @abstractmethod
    def execute(self, context: HookContext) -> None:
        """Execute the hook logic.

        Args:
            context: The hook context for this invocation.
        """
        pass


# ---------------------------------------------------------------------------
# HookRegistry
# ---------------------------------------------------------------------------

class HookRegistry:
    """Global hook registry.

    Stores hooks per hook point and executes them in priority order.
    """

    _instance: Optional["HookRegistry"] = None
    _hooks: Dict[str, List[Hook]] = {}

    def __new__(cls) -> "HookRegistry":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._hooks = {point: [] for point in ALL_HOOK_POINTS}
        return cls._instance

    def register(self, hook_point: str, hook: Hook, priority: int = 0) -> None:
        """Register a hook for a specific hook point.

        Args:
            hook_point: One of the ``*_HOOK`` constants.
            hook: The hook instance.
            priority: Execution priority (higher runs first). Overrides the
                hook's own ``priority`` attribute.

        Raises:
            ValueError: If *hook_point* is not a recognised hook point.
        """
        if hook_point not in ALL_HOOK_POINTS:
            raise ValueError(
                f"Unknown hook point '{hook_point}'. "
                f"Valid points: {ALL_HOOK_POINTS}"
            )
        hook.priority = priority
        self._hooks[hook_point].append(hook)
        # Keep list sorted by priority descending (highest first)
        self._hooks[hook_point].sort(key=lambda h: h.priority, reverse=True)
        logger.debug(
            "Registered hook '%s' for '%s' with priority %d",
            hook.name,
            hook_point,
            priority,
        )

    def execute(self, hook_point: str, context: HookContext) -> None:
        """Execute all hooks registered for *hook_point*.

        Hooks are executed in priority order (highest first). If a hook
        raises an exception, it is logged but does not prevent subsequent
        hooks from running.

        Args:
            hook_point: The hook point to trigger.
            context: The context to pass to each hook.
        """
        hooks = self._hooks.get(hook_point, [])
        if not hooks:
            return

        logger.debug(
            "Executing %d hook(s) for '%s'", len(hooks), hook_point
        )
        for hook in hooks:
            try:
                hook.execute(context)
            except Exception as exc:
                logger.warning(
                    "Hook '%s' raised an exception during '%s': %s",
                    hook.name,
                    hook_point,
                    exc,
                )

    def clear(self, hook_point: Optional[str] = None) -> None:
        """Clear registered hooks.

        Args:
            hook_point: If provided, clear only hooks for that point.
                Otherwise, clear all hooks.
        """
        if hook_point is None:
            for point in ALL_HOOK_POINTS:
                self._hooks[point] = []
        else:
            self._hooks[hook_point] = []

    def get_hooks(self, hook_point: str) -> List[Hook]:
        """Return the list of hooks registered for *hook_point*."""
        return list(self._hooks.get(hook_point, []))

    def reset(self) -> None:
        """Clear all hooks (primarily for testing)."""
        for point in ALL_HOOK_POINTS:
            self._hooks[point] = []


# ---------------------------------------------------------------------------
# Module-level convenience functions
# ---------------------------------------------------------------------------

_registry = HookRegistry()


def register_hook(hook_point: str, hook: Hook, priority: int = 0) -> None:
    """Register a hook in the global registry.

    Args:
        hook_point: One of the hook point constants.
        hook: The hook instance.
        priority: Execution priority (higher runs first).
    """
    _registry.register(hook_point, hook, priority)


def execute_hooks(hook_point: str, context: HookContext) -> None:
    """Execute all hooks for a hook point in the global registry.

    Args:
        hook_point: The hook point to trigger.
        context: The context to pass to hooks.
    """
    _registry.execute(hook_point, context)


def get_hook_registry() -> HookRegistry:
    """Return the global HookRegistry instance."""
    return _registry


# ---------------------------------------------------------------------------
# Built-in hooks
# ---------------------------------------------------------------------------

class LoggingHook(Hook):
    """Logs hook point invocations."""

    name = "logging"
    priority = 0

    def __init__(self, log_level: int = logging.INFO) -> None:
        self._log_level = log_level
        self._logger = logging.getLogger(f"{__name__}.LoggingHook")

    def execute(self, context: HookContext) -> None:
        job_name = context.job.config.name if context.job else "unknown"
        msg = f"[LoggingHook] Job '{job_name}' reached phase '{context.phase}'"
        if context.error:
            msg += f" (error: {context.error})"
        self._logger.log(self._log_level, msg)


class MetricsHook(Hook):
    """Collects timing and record-count metrics for each ETL phase."""

    name = "metrics"
    priority = 0

    def __init__(self) -> None:
        self._phase_starts: Dict[str, float] = {}
        self._logger = logging.getLogger(f"{__name__}.MetricsHook")

    def execute(self, context: HookContext) -> None:
        phase = context.phase
        job_name = context.job.config.name if context.job else "unknown"

        if phase in (PRE_EXTRACT, PRE_TRANSFORM, PRE_LOAD):
            self._phase_starts[phase] = time.time()
            context.metadata[f"{phase}_start"] = self._phase_starts[phase]

        elif phase in (POST_EXTRACT, POST_TRANSFORM, POST_LOAD):
            start_key = phase.replace("post_", "pre_") + "_start"
            start = context.metadata.get(start_key, 0.0)
            if start:
                duration = time.time() - start
                self._logger.info(
                    "[MetricsHook] Job '%s' phase '%s' took %.4fs",
                    job_name,
                    phase.replace("post_", ""),
                    duration,
                )
                context.metadata[f"{phase}_duration"] = duration

        if phase == POST_EXTRACT and context.data is not None:
            try:
                import pandas as pd

                if isinstance(context.data, pd.DataFrame):
                    context.metadata["extracted_rows"] = len(context.data)
                    self._logger.info(
                        "[MetricsHook] Job '%s' extracted %d rows",
                        job_name,
                        len(context.data),
                    )
            except ImportError:
                pass

        if phase == POST_LOAD and context.data is not None:
            try:
                import pandas as pd

                if isinstance(context.data, pd.DataFrame):
                    context.metadata["loaded_rows"] = len(context.data)
            except ImportError:
                pass


class QualityCheckHook(Hook):
    """Runs data quality checks after extraction and after transformation."""

    name = "quality_check"
    priority = 0

    def __init__(
        self,
        required_columns: Optional[List[str]] = None,
        null_threshold: float = 1.0,
        duplicate_threshold: float = 1.0,
    ) -> None:
        self._required_columns = required_columns or []
        self._null_threshold = null_threshold
        self._duplicate_threshold = duplicate_threshold
        self._logger = logging.getLogger(f"{__name__}.QualityCheckHook")

    def execute(self, context: HookContext) -> None:
        if context.phase not in (POST_EXTRACT, POST_TRANSFORM):
            return
        if context.data is None:
            return

        try:
            import pandas as pd
        except ImportError:
            return

        if not isinstance(context.data, pd.DataFrame):
            return

        df = context.data
        job_name = context.job.config.name if context.job else "unknown"

        # Schema check
        if self._required_columns:
            missing = [
                c for c in self._required_columns if c not in df.columns
            ]
            if missing:
                self._logger.warning(
                    "[QualityCheckHook] Job '%s' missing columns: %s",
                    job_name,
                    missing,
                )
                context.metadata["quality_missing_columns"] = missing
            else:
                context.metadata["quality_schema_ok"] = True

        # Null check
        null_fractions = {
            col: float(df[col].isna().mean())
            for col in df.columns
        }
        context.metadata["quality_null_fractions"] = null_fractions

        # Duplicate check
        dup_count = int(df.duplicated(keep="first").sum())
        context.metadata["quality_duplicate_count"] = dup_count

        self._logger.info(
            "[QualityCheckHook] Job '%s' phase '%s': %d rows, %d duplicates",
            job_name,
            context.phase,
            len(df),
            dup_count,
        )
