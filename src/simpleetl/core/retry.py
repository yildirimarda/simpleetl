"""
Retry with exponential backoff + jitter and circuit breaker.
"""

import logging
import random
import time
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreakerOpenError(Exception):
    """Raised when the circuit breaker is open."""

    pass


class CircuitBreaker:
    """Simple circuit breaker for external service calls."""

    def __init__(self, threshold: int = 5, timeout: float = 30.0) -> None:
        self.threshold = threshold
        self.timeout = timeout
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.state = CircuitState.CLOSED
        self.lock = None  # Not using threading locks for simplicity

    def _can_attempt(self) -> bool:
        if self.state == CircuitState.CLOSED:
            return True
        if self.state == CircuitState.OPEN:
            if (
                self.last_failure_time is not None
                and (time.time() - self.last_failure_time) >= self.timeout
            ):
                self.state = CircuitState.HALF_OPEN
                logger.info("Circuit breaker transitioned to HALF_OPEN")
                return True
            return False
        if self.state == CircuitState.HALF_OPEN:
            return True
        return False

    def call(self, func: Callable[[], Any], *args, **kwargs) -> Any:
        if not self._can_attempt():
            raise CircuitBreakerOpenError("Circuit breaker is OPEN")
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception:
            self._on_failure()
            raise

    def _on_success(self) -> None:
        self.failure_count = 0
        if self.state == CircuitState.HALF_OPEN:
            logger.info("Circuit breaker transitioned to CLOSED")
        self.state = CircuitState.CLOSED
        self.last_failure_time = None

    def _on_failure(self) -> None:
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.state == CircuitState.HALF_OPEN:
            self.state = CircuitState.OPEN
            logger.warning(
                "Circuit breaker OPEN (half-open attempt failed, count=%d)",
                self.failure_count,
            )
        elif self.failure_count >= self.threshold:
            self.state = CircuitState.OPEN
            logger.warning(
                "Circuit breaker OPEN (threshold %d reached)", self.threshold
            )
        else:
            logger.warning(
                "Circuit breaker failure %d/%d",
                self.failure_count,
                self.threshold,
            )


class RetryWithBackoff:
    """Retry mechanism with exponential backoff and full jitter."""

    def __init__(
        self,
        max_retries: int = 3,
        backoff_base: float = 1.0,
        cap: float = 60.0,
    ) -> None:
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.cap = cap

    def calculate_backoff(self, attempt: int) -> float:
        """Calculate sleep duration for a given retry attempt (0-indexed)."""
        exp_delay = self.backoff_base * (2**attempt)
        capped = min(exp_delay, self.cap)
        return random.uniform(0, capped)

    def execute(
        self,
        func: Callable[[], Any],
        *args,
        **kwargs,
    ) -> Any:
        last_exception: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            try:
                return func(*args, **kwargs)
            except Exception as exc:
                last_exception = exc
                if attempt < self.max_retries:
                    sleep_time = self.calculate_backoff(attempt)
                    logger.info(
                        "Retry attempt %d/%d in %.2fs after: %s",
                        attempt + 1,
                        self.max_retries,
                        sleep_time,
                        exc,
                    )
                    time.sleep(sleep_time)
                else:
                    logger.error(
                        "All %d retry attempts exhausted. Last error: %s",
                        self.max_retries,
                        exc,
                    )
        if last_exception is not None:
            raise last_exception
        raise RuntimeError("Unexpected state: no exception but no result")


class RetryCircuitBreaker:
    """Combines retry with exponential backoff, jitter, and circuit breaker."""

    def __init__(
        self,
        max_retries: int = 3,
        backoff_base: float = 1.0,
        breaker_threshold: int = 5,
    ) -> None:
        self.retry = RetryWithBackoff(
            max_retries=max_retries, backoff_base=backoff_base
        )
        self.breaker = CircuitBreaker(threshold=breaker_threshold)

    def call(self, func: Callable[[], Any], *args, **kwargs) -> Any:
        # First check breaker
        if not self.breaker._can_attempt():
            raise CircuitBreakerOpenError("Circuit breaker is OPEN")
        # Try with retry
        last_exception: Optional[Exception] = None
        for attempt in range(self.retry.max_retries + 1):
            try:
                result = func(*args, **kwargs)
                self.breaker._on_success()
                return result
            except Exception as exc:
                last_exception = exc
                self.breaker._on_failure()
                if attempt < self.retry.max_retries:
                    # Check breaker before retrying
                    if not self.breaker._can_attempt():
                        logger.error("Circuit breaker OPEN during retry")
                        raise CircuitBreakerOpenError(
                            "Circuit breaker is OPEN"
                        ) from exc
                    sleep_time = self.retry.calculate_backoff(attempt)
                    logger.info(
                        "Retry attempt %d/%d in %.2fs after: %s",
                        attempt + 1,
                        self.retry.max_retries,
                        sleep_time,
                        exc,
                    )
                    time.sleep(sleep_time)
                else:
                    logger.error(
                        "All %d retry attempts exhausted. Last error: %s",
                        self.retry.max_retries,
                        exc,
                    )
        if last_exception is not None:
            raise last_exception
        raise RuntimeError("Unexpected retry state")
