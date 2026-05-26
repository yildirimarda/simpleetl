"""
Command-line interface for SimpleETL framework.
"""

import argparse
import sys
from pathlib import Path

from .core.config import load_config
from .core.dag import DAG, DAGRunner
from .core.logger import get_logger
from .core.metrics import get_metrics

logger = get_logger(__name__)


def create_parser() -> argparse.ArgumentParser:
    """Create the argument parser."""
    parser = argparse.ArgumentParser(
        prog="simpleetl",
        description="SimpleETL - A lightweight ETL framework",
    )
    parser.add_argument(
        "--version", action="version", version="%(prog)s 0.1.0"
    )
    parser.add_argument(
        "--config", "-c",
        type=str,
        help="Path to the ETL job configuration file (YAML or JSON)",
    )
    parser.add_argument(
        "--platform", "-p",
        type=str,
        choices=["local", "glue", "databricks", "synapse"],
        default=None,
        help="Override the platform specified in the config",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate configuration without running the job",
    )
    parser.add_argument(
        "--list-formats",
        action="store_true",
        help="List all supported data formats",
    )
    parser.add_argument(
        "--detect-platform",
        action="store_true",
        help="Detect and display the current platform",
    )
    parser.add_argument(
        "--dag",
        type=str,
        help="Path to a DAG configuration file (YAML) for orchestrating multiple jobs",
    )
    parser.add_argument(
        "--max-parallel",
        type=int,
        default=1,
        help="Maximum number of parallel jobs when running a DAG (default: 1)",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue executing independent branches even if a node fails",
    )
    return parser


def list_formats() -> None:
    """List all supported data formats."""
    from .formats import FormatFactory

    formats = FormatFactory.supported_formats()
    print("Supported data formats:")
    for fmt, ext in sorted(formats.items()):
        print(f"  {fmt:12s} ({ext})")
    print("  database     (postgresql://, mysql://, mssql://, sqlite://)")


def detect_platform() -> None:
    """Detect and display the current platform."""
    from .platforms.detector import get_platform_info

    info = get_platform_info()
    print(f"Current platform: {info['platform']}")
    print(f"System: {info['system']}")
    print(f"Python: {info['python_version']}")
    print(f"  AWS Glue:     {info['is_glue']}")
    print(f"  Databricks:   {info['is_databricks']}")
    print(f"  Azure Synapse: {info['is_synapse']}")


def run_job(config_path: str, platform_override: str | None = None) -> None:
    """
    Run an ETL job from a configuration file.

    If the config specifies a 'job_class' (module.ClassName), it dynamically
    imports and instantiates the ETLJob subclass and executes it.
    Otherwise, logs the job details for validation.
    """
    import importlib

    config = load_config(config_path)
    logger.info(f"Loaded job config: {config.name}")
    logger.info(f"Description: {config.description}")
    logger.info(f"Platform: {platform_override or config.platform}")
    logger.info(f"Input format: {config.input_format}")
    logger.info(f"Output format: {config.output_format}")

    if platform_override:
        config.platform = platform_override

    metrics = get_metrics()
    metrics.inc_counter('etl_jobs_total')

    job_class_path = config.params.get("job_class")
    if not job_class_path:
        logger.info(
            f"Job '{config.name}' would run on platform '{config.platform}'. "
            "Set params.job_class to enable automatic execution."
        )
        return

    try:
        module_path, class_name = job_class_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        job_class = getattr(module, class_name)
        job = job_class(config)
        job.run_with_error_handling()
        logger.info(f"Job '{config.name}' completed successfully")
    except (ImportError, AttributeError, ValueError) as e:
        logger.error(f"Failed to load job class '{job_class_path}': {e}")
        raise SystemExit(1) from e


def run_dag(
    dag_config_path: str,
    max_parallel: int = 1,
    fail_fast: bool = True,
) -> None:
    """Load and execute a DAG from a YAML configuration file.

    Args:
        dag_config_path: Path to the DAG YAML file.
        max_parallel: Maximum number of concurrent jobs.
        fail_fast: If True, stop on first failure; otherwise continue
            independent branches.
    """
    dag_path = Path(dag_config_path)
    if not dag_path.exists():
        logger.error(f"DAG configuration file not found: {dag_path}")
        sys.exit(1)

    logger.info(f"Loading DAG from: {dag_path}")
    dag = DAG.from_yaml(str(dag_path))
    logger.info(f"DAG '{dag.name}' loaded with {len(dag.nodes)} nodes")

    plan = dag.get_execution_plan()
    logger.info(f"Execution order: {plan['topological_order']}")
    logger.info(f"Parallel groups: {plan['parallel_groups']}")

    runner = DAGRunner(
        max_parallel=max_parallel,
        fail_fast=fail_fast,
    )
    result = runner.run(dag)

    # Log summary
    logger.info(f"DAG '{dag.name}' finished with status: {result.status}")
    logger.info(f"Total duration: {result.duration:.2f}s")
    for name, node_result in result.node_results.items():
        logger.info(
            f"  {name}: {node_result.status.value} "
            f"({node_result.duration:.2f}s)"
            + (f" - ERROR: {node_result.error}" if node_result.error else "")
        )

    if result.status == "failed":
        sys.exit(1)


def main() -> None:
    """Main entry point for the CLI."""
    parser = create_parser()
    args = parser.parse_args()

    if args.list_formats:
        list_formats()
        return

    if args.detect_platform:
        detect_platform()
        return

    if args.dag:
        run_dag(
            args.dag,
            max_parallel=args.max_parallel,
            fail_fast=not args.continue_on_error,
        )
        return

    if args.config:
        if not Path(args.config).exists():
            logger.error(f"Configuration file not found: {args.config}")
            sys.exit(1)

        if args.dry_run:
            config = load_config(args.config)
            logger.info(f"Configuration valid: {config.name}")
            return

        run_job(args.config, args.platform)
        return

    parser.print_help()


if __name__ == "__main__":
    main()
