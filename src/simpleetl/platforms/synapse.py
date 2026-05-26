"""
Azure Synapse platform runner for ETL jobs.
"""

from .detector import is_azure_synapse
from .base import PlatformRunner
from ..core.job import ETLJob


class SynapsePlatformRunner(PlatformRunner):
    """Run ETL jobs on Azure Synapse."""

    def run_job(self, job: ETLJob) -> None:
        """
        Run the ETL job on Azure Synapse.

        Args:
            job: An instance of ETLJob to run.
        """
        # For Azure Synapse, we would typically submit the job as a stored procedure or use Spark pools
        # This is a simplified implementation that delegates to local execution
        # In a real implementation, this would use the Azure Synapse SDK or Spark

        # Check if we're running in an Azure Synapse environment
        if is_azure_synapse():
            # Running on Azure Synapse - execute the job directly
            job.run_with_error_handling()
        else:
            # Not in Synapse environment - simulate or delegate
            # For now, we'll run locally with a warning
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(
                "Not running in Azure Synapse environment. Executing job locally instead."
            )
            job.run_with_error_handling()