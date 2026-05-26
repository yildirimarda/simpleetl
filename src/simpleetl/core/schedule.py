"""
Cron-based scheduling for ETL jobs and DAGs.

Provides cron expression parsing and evaluation to determine whether
a scheduled job should run at a given time.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional, Set


class CronParseError(Exception):
    """Raised when a cron expression cannot be parsed."""
    pass


@dataclass
class CronExpression:
    """Parse and evaluate a 5-field cron expression.

    Supports:
    - Wildcard (``*``)
    - Single values (``5``)
    - Ranges (``1-5``)
    - Steps (``*/2``, ``1-10/2``)
    - Lists (``1,3,5``)

    Fields: minute, hour, day_of_month, month, day_of_week.

    Example::

        cron = CronExpression("0 2 * * *")  # Daily at 02:00
        cron.matches(datetime(2024, 1, 15, 2, 0))  # True
    """

    expression: str
    minutes: Set[int] = field(default_factory=set)
    hours: Set[int] = field(default_factory=set)
    days_of_month: Set[int] = field(default_factory=set)
    months: Set[int] = field(default_factory=set)
    days_of_week: Set[int] = field(default_factory=set)

    # Field bounds: (min, max)
    _FIELD_BOUNDS = [
        (0, 59),   # minute
        (0, 23),   # hour
        (1, 31),   # day of month
        (1, 12),   # month
        (0, 6),    # day of week (0 = Sunday)
    ]

    def __post_init__(self) -> None:
        parts = self.expression.strip().split()
        if len(parts) != 5:
            raise CronParseError(
                f"Cron expression must have 5 fields, got {len(parts)}: "
                f"'{self.expression}'"
            )
        field_sets = [
            self.minutes,
            self.hours,
            self.days_of_month,
            self.months,
            self.days_of_week,
        ]
        for part, val_set, (lo, hi) in zip(parts, field_sets, self._FIELD_BOUNDS):
            self._parse_field(part, val_set, lo, hi)

    @staticmethod
    def _parse_field(
        part: str, val_set: Set[int], min_val: int, max_val: int
    ) -> None:
        """Parse a single cron field and populate val_set."""
        # Handle list (comma-separated)
        for segment in part.split(","):
            segment = segment.strip()
            if segment == "*":
                val_set.update(range(min_val, max_val + 1))
                continue

            # Handle step
            step = 1
            if "/" in segment:
                base, step_str = segment.split("/", 1)
                try:
                    step = int(step_str)
                except ValueError:
                    raise CronParseError(
                        f"Invalid step value '{step_str}' in cron field "
                        f"'{part}'"
                    )
                if step <= 0:
                    raise CronParseError(
                        f"Step value must be positive, got {step}"
                    )
                segment = base if base != "*" else "*"

            if segment == "*":
                val_set.update(range(min_val, max_val + 1, step))
            elif "-" in segment:
                # Range
                range_parts = segment.split("-", 1)
                try:
                    start = int(range_parts[0])
                    end = int(range_parts[1])
                except ValueError:
                    raise CronParseError(
                        f"Invalid range '{segment}' in cron field '{part}'"
                    )
                if start < min_val or end > max_val or start > end:
                    raise CronParseError(
                        f"Range {start}-{end} out of bounds "
                        f"[{min_val}-{max_val}] in cron field '{part}'"
                    )
                val_set.update(range(start, end + 1, step))
            else:
                # Single value
                try:
                    val = int(segment)
                except ValueError:
                    raise CronParseError(
                        f"Invalid value '{segment}' in cron field '{part}'"
                    )
                if val < min_val or val > max_val:
                    raise CronParseError(
                        f"Value {val} out of bounds [{min_val}-{max_val}] "
                        f"in cron field '{part}'"
                    )
                val_set.add(val)

    def matches(self, dt: datetime) -> bool:
        """Check if the given datetime matches this cron expression.

        Args:
            dt: The datetime to check (timezone-aware preferred).

        Returns:
            True if all fields match.
        """
        # Cron uses 0=Sunday; Python weekday() uses 0=Monday.
        # isoweekday() returns 1=Mon .. 7=Sun, so %7 maps Sunday to 0.
        cron_dow = dt.isoweekday() % 7
        return (
            dt.minute in self.minutes
            and dt.hour in self.hours
            and dt.day in self.days_of_month
            and dt.month in self.months
            and cron_dow in self.days_of_week
        )

    def next_run(self, after: Optional[datetime] = None) -> datetime:
        """Calculate the next run time after the given datetime.

        Searches minute-by-minute up to 4 years ahead.

        Args:
            after: Reference datetime (defaults to now).

        Returns:
            The next datetime that matches this cron expression.
        """
        if after is None:
            after = datetime.now(timezone.utc)
        # Start from the next minute
        candidate = after.replace(second=0, microsecond=0) + timedelta(minutes=1)

        # Search up to 4 years (to handle Feb 29)
        max_iterations = 366 * 24 * 60 * 4
        for _ in range(max_iterations):
            if self.matches(candidate):
                return candidate
            candidate += timedelta(minutes=1)
        raise CronParseError(
            "Could not find next run time within 4 years"
        )


@dataclass
class Schedule:
    """A cron-based schedule for a job or DAG.

    Attributes:
        name: Human-readable name for this schedule.
        cron: A ``CronExpression`` instance.
        timezone: Timezone string (currently only UTC is fully supported).
        enabled: Whether this schedule is active.
    """

    name: str
    cron: CronExpression
    timezone: str = "UTC"
    enabled: bool = True

    @classmethod
    def from_string(cls, name: str, cron_str: str, **kwargs) -> "Schedule":
        """Create a Schedule from a cron string.

        Args:
            name: Schedule name.
            cron_str: 5-field cron expression.
            **kwargs: Additional arguments passed to the Schedule constructor.

        Returns:
            A new ``Schedule`` instance.
        """
        return cls(name=name, cron=CronExpression(cron_str), **kwargs)

    def should_run(
        self,
        now: Optional[datetime] = None,
        last_run: Optional[datetime] = None,
    ) -> bool:
        """Determine if the job should run now.

        Args:
            now: Current time (defaults to ``datetime.now(timezone.utc)``).
            last_run: Timestamp of the last successful run. If provided,
                the method ensures we do not re-trigger within the same
                minute.

        Returns:
            True if the job should run.
        """
        if not self.enabled:
            return False

        if now is None:
            now = datetime.now(timezone.utc)

        if not self.cron.matches(now):
            return False

        # Prevent re-running in the same minute
        if last_run is not None:
            if (
                last_run.year == now.year
                and last_run.month == now.month
                and last_run.day == now.day
                and last_run.hour == now.hour
                and last_run.minute == now.minute
            ):
                return False

        return True

    def next_run_time(
        self, after: Optional[datetime] = None
    ) -> datetime:
        """Return the next scheduled run time.

        Args:
            after: Reference datetime (defaults to now).

        Returns:
            Next matching datetime.
        """
        return self.cron.next_run(after)
