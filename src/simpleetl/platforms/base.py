"""
Base class for platform runners.
"""

from abc import ABC, abstractmethod
from ..core.job import ETLJob


class PlatformRunner(ABC):
    """Abstract base class for platform-specific ETL job runners."""

    @abstractmethod
    def run_job(self, job: ETLJob) -> None:
        """
        Run the ETL job on the specific platform.

        Args:
            job: An instance of ETLJob to run.
        """
        pass