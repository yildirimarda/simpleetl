"""
Databricks platform runner for ETL jobs.
"""

from .detector import is_databricks
from .base import PlatformRunner
from ..core.job import ETLJob


class DatabricksPlatformRunner(PlatformRunner):
    """Run ETL jobs on Databricks."""

    def run_job(self, job: ETLJob) -> None:
        """
        Run the ETL job on Databricks.

        Args:
            job: An instance of ETLJob to run.
        """
        # For Databricks, we would typically submit the job as a notebook or jar
        # This is a simplified implementation that delegates to local execution
        # In a real implementation, this would use the Databricks REST API or DBConnect

        # Check if we're running in a Databricks environment
        if is_databricks():
            # Running on Databricks - execute the job directly
            job.run_with_error_handling()
        else:
            # Not in Databricks environment - simulate or delegate
            # For now, we'll run locally with a warning
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(
                "Not running in Databricks environment. Executing job locally instead."
            )
            job.run_with_error_handling()