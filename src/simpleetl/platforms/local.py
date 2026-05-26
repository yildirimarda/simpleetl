"""
Local platform runner for ETL jobs.
"""

from .base import PlatformRunner
from ..core.job import ETLJob


class LocalPlatformRunner(PlatformRunner):
    """Run ETL jobs locally using pandas."""

    def run_job(self, job: ETLJob) -> None:
        """
        Run the ETL job in a local environment.

        Args:
            job: An instance of ETLJob to run.
        """
        # For local platform, we just run the job's run method with error handling
        job.run_with_error_handling()