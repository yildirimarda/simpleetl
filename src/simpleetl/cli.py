"""
Command-line interface for SimpleETL framework.
"""

import argparse
import sys
from pathlib import Path
from typing import Dict, Optional

from .core.config import load_config
from .core.dag import DAG, DAGRunner
from .core.logger import get_logger
from .core.metrics import get_metrics

logger = get_logger(__name__)

_INIT_CONFIG_YAML = """\
# SimpleETL job configuration.
# Validate with: simpleetl --validate-config config.yaml
# Run with:      simpleetl --config config.yaml  (or: python job.py)
name: starter_job
description: Starter ETL job scaffolded by `simpleetl --init`
platform: local
input_format: csv
output_format: parquet
params:
  input_path: data/input.csv
  output_path: output/output.parquet

# Declarative data quality rules (uncomment to enable):
# validation_rules:
#   - type: not_null
#     column: id
#     severity: error
#   - type: in_range
#     column: value
#     min: 0
#     max: 1000
#     severity: warning
"""

_INIT_JOB_PY = '''\
"""Starter ETL job scaffolded by ``simpleetl --init``."""

import pandas as pd

from simpleetl.core.config import ETLJobConfig
from simpleetl.core.job import ETLJob
from simpleetl.formats.csv import CSVReader
from simpleetl.formats.parquet import ParquetWriter


class StarterJob(ETLJob):
    """Read a CSV, add a derived column, and write the result as Parquet."""

    def __init__(self, config: "ETLJobConfig | str") -> None:
        super().__init__(config)
        self.reader = CSVReader()
        self.writer = ParquetWriter()

    def extract(self) -> pd.DataFrame:
        """Read the input CSV configured in ``params.input_path``."""
        return self.reader.read(self.config.params["input_path"])

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Add a ``value_doubled`` column."""
        data = data.copy()
        data["value_doubled"] = data["value"] * 2
        return data

    def load(self, data: pd.DataFrame) -> None:
        """Write the result to ``params.output_path`` as Parquet."""
        output_path = self.config.params["output_path"]
        self.writer.write(data, output_path)
        self.logger.info(f"Wrote {len(data)} rows to {output_path}")

    def run(self) -> None:
        """Execute extract -> transform -> load."""
        self.load(self.transform(self.extract()))


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    job = StarterJob("config.yaml")
    job.run_with_error_handling()
'''

_INIT_SAMPLE_CSV = """\
id,name,value
1,alpha,10
2,beta,20
3,gamma,30
4,delta,40
"""

_INIT_README_MD = """\
# Starter SimpleETL Project

Scaffolded by `simpleetl --init`.

## Layout

- `config.yaml` — job configuration (validated by SimpleETL)
- `job.py` — the ETL job (extract / transform / load)
- `data/input.csv` — sample input data
- `output/` — job output lands here

## Run

From this directory: `python job.py` or `simpleetl --config config.yaml`.
Check the config without running: `simpleetl --validate-config config.yaml`.
"""


def _parse_params(param_list: list) -> Dict[str, str]:
    """Parse ``key=value`` strings from ``--param`` flags."""
    result: Dict[str, str] = {}
    for item in param_list or []:
        if "=" not in item:
            raise SystemExit(f"Invalid --param format '{item}'. Expected 'key=value'.")
        k, v = item.split("=", 1)
        result[k.strip()] = v.strip()
    return result


def create_parser() -> argparse.ArgumentParser:
    """Create the argument parser."""
    parser = argparse.ArgumentParser(
        prog="simpleetl",
        description="SimpleETL - A lightweight ETL framework",
    )
    parser.add_argument("--version", action="version", version="%(prog)s 1.3.0")
    parser.add_argument(
        "--config",
        "-c",
        type=str,
        help="Path to the ETL job configuration file (YAML or JSON)",
    )
    parser.add_argument(
        "--platform",
        "-p",
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
    parser.add_argument(
        "--init",
        type=str,
        metavar="DIR",
        help=(
            "Scaffold a starter project (config.yaml, job.py, sample data) "
            "in the given directory"
        ),
    )
    parser.add_argument(
        "--validate-config",
        type=str,
        metavar="FILE",
        dest="validate_config",
        help=(
            "Validate a job configuration file and print a human-readable "
            "summary without running the job"
        ),
    )
    parser.add_argument(
        "--param",
        metavar="KEY=VALUE",
        action="append",
        dest="params",
        default=[],
        help=(
            "Inject a template variable into the config (Jinja2). "
            "Can be repeated: --param date=2024-01-01 --param env=prod"
        ),
    )
    parser.add_argument(
        "profile_file",
        nargs="?",
        metavar="FILE",
        help=(
            "Profile a data file and print statistics. Usage: simpleetl profile <file>"
        ),
    )
    parser.add_argument(
        "--profile",
        action="store_true",
        help="Profile the file specified as a positional argument.",
    )
    parser.add_argument(
        "--profile-format",
        choices=["markdown", "json", "html"],
        default="markdown",
        help="Output format for the profile report (default: markdown).",
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


def profile_file(path: str, fmt: str = "markdown") -> None:
    """Profile a data file and print statistics to stdout.

    Args:
        path: Path to the data file (any format supported by FormatFactory).
        fmt: Output format — ``"markdown"``, ``"json"``, or ``"html"``.
    """
    from .core.profiling import DataProfiler
    from .formats import FormatFactory

    reader = FormatFactory.get_reader(path)
    df = reader.read(path)
    profiler = DataProfiler()
    report = profiler.profile(df)

    if fmt == "json":
        print(report.to_json())
    elif fmt == "html":
        print(report.to_html())
    else:
        print(report.to_markdown())


def init_project(target_dir: str) -> None:
    """Scaffold a starter SimpleETL project in *target_dir*.

    Creates ``config.yaml``, ``job.py``, ``data/input.csv``, ``README.md``
    and an empty ``output/`` directory.  The target directory must either
    not exist yet or be empty.

    Args:
        target_dir: Directory to create the project in.

    Raises:
        SystemExit: If the target exists and is a non-empty directory or
            a regular file.
    """
    target = Path(target_dir)
    if target.exists():
        if not target.is_dir():
            print(
                f"Error: '{target}' already exists and is not a directory.",
                file=sys.stderr,
            )
            sys.exit(1)
        if any(target.iterdir()):
            print(
                f"Error: directory '{target}' already exists and is not "
                "empty. Choose a new or empty directory for --init.",
                file=sys.stderr,
            )
            sys.exit(1)

    files = {
        "config.yaml": _INIT_CONFIG_YAML,
        "job.py": _INIT_JOB_PY,
        "data/input.csv": _INIT_SAMPLE_CSV,
        "README.md": _INIT_README_MD,
    }
    (target / "output").mkdir(parents=True, exist_ok=True)
    for rel_path, content in files.items():
        path = target / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)

    print(f"Initialized SimpleETL project in '{target}'")
    print()
    print("Created:")
    for rel_path in files:
        print(f"  {rel_path}")
    print("  output/")
    print()
    print("Next steps:")
    print(f"  cd {target}")
    print("  python job.py")
    print("  # or: simpleetl --config config.yaml")


def validate_config_file(
    config_path: str,
    template_vars: Optional[Dict[str, str]] = None,
) -> None:
    """Validate a job configuration file and print a summary.

    Loads the config via :func:`load_config` (Pydantic validation) and,
    when ``validation_rules`` are present, sanity-checks them with the
    ``QualityRuleEngine`` (skipped with a debug log if the module is not
    available).

    Args:
        config_path: Path to the configuration file (YAML or JSON).
        template_vars: Jinja2 template variables from ``--param`` flags.

    Raises:
        SystemExit: If the file is missing, fails validation, or contains
            invalid validation rules.
    """
    path = Path(config_path)
    if not path.exists():
        print(
            f"Error: configuration file not found: {config_path}",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        config = load_config(config_path, template_vars=template_vars or None)
    except Exception as exc:
        print(f"Configuration invalid: {exc}", file=sys.stderr)
        sys.exit(1)

    if config.validation_rules:
        try:
            from .core.quality_rules import (  # type: ignore[import-not-found]
                QualityRuleEngine,
            )
        except ImportError:
            logger.debug("quality_rules module unavailable; skipping rule check")
        else:
            try:
                QualityRuleEngine(config.validation_rules)
            except ValueError as exc:
                print(f"Invalid validation_rules: {exc}", file=sys.stderr)
                sys.exit(1)

    def _on_off(flag: bool) -> str:
        return "on" if flag else "off"

    print(f"Configuration valid: {config_path}")
    print(f"  Job name:         {config.name}")
    print(f"  Platform:         {config.platform}")
    print(f"  Input format:     {config.input_format}")
    print(f"  Output format:    {config.output_format}")
    print(f"  Incremental:      {_on_off(config.incremental)}")
    print(f"  Validation rules: {len(config.validation_rules)}")
    print(f"  Schema drift:     {_on_off(config.schema_drift.enabled)}")
    print(f"  Tracing:          {_on_off(config.tracing.enabled)}")


def run_job(
    config_path: str,
    platform_override: Optional[str] = None,
    template_vars: Optional[Dict[str, str]] = None,
) -> None:
    """
    Run an ETL job from a configuration file.

    If the config specifies a 'job_class' (module.ClassName), it dynamically
    imports and instantiates the ETLJob subclass and executes it.
    Otherwise, logs the job details for validation.
    """
    import importlib

    config = load_config(config_path, template_vars=template_vars or {})
    logger.info(f"Loaded job config: {config.name}")
    logger.info(f"Description: {config.description}")
    logger.info(f"Platform: {platform_override or config.platform}")
    logger.info(f"Input format: {config.input_format}")
    logger.info(f"Output format: {config.output_format}")

    if platform_override:
        config.platform = platform_override

    metrics = get_metrics()
    metrics.inc_counter("etl_jobs_total")

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

    if args.validate_config and (
        args.config or args.dag or args.profile or args.profile_file
    ):
        parser.error(
            "--validate-config cannot be combined with --config, --dag, or --profile"
        )

    if args.init:
        init_project(args.init)
        return

    if args.list_formats:
        list_formats()
        return

    if args.detect_platform:
        detect_platform()
        return

    if args.profile or args.profile_file:
        target = args.profile_file
        if not target:
            logger.error("Specify a file to profile: simpleetl --profile <file>")
            sys.exit(1)
        profile_file(target, fmt=args.profile_format)
        return

    template_vars = _parse_params(args.params)

    if args.validate_config:
        validate_config_file(args.validate_config, template_vars=template_vars)
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
            config = load_config(args.config, template_vars=template_vars or None)
            logger.info(f"Configuration valid: {config.name}")
            return

        run_job(args.config, args.platform, template_vars=template_vars or None)
        return

    parser.print_help()


if __name__ == "__main__":
    main()
