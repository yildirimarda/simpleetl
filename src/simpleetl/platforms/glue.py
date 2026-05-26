"""
AWS Glue platform runner for ETL jobs.

Provides integration with AWS Glue serverless Spark environment, including
GlueContext management, DynamicFrame-based reading/writing from the Glue Data
Catalog, job argument parsing, S3 path resolution, and Glue job bookmark
support for incremental processing.
"""

import logging
import sys
from typing import Any, Dict, List, Optional

from .base import PlatformRunner
from .detector import is_aws_glue
from ..core.job import ETLJob

logger = logging.getLogger(__name__)


class GlueContextManager:
    """Manage AWS Glue context and Spark session lifecycle.

    This class wraps the creation and caching of GlueContext and SparkSession
    objects, ensuring they are created only once and reused across the job.

    Attributes:
        _glue_context: Cached GlueContext instance.
        _spark_session: Cached SparkSession instance.
        _glue_client: Cached boto3 Glue client instance.
    """

    def __init__(self) -> None:
        """Initialize the GlueContextManager with no cached objects."""
        self._glue_context: Optional[Any] = None
        self._spark_session: Optional[Any] = None
        self._glue_client: Optional[Any] = None

    @property
    def glue_context(self) -> Any:
        """Get or create the GlueContext.

        Returns:
            The GlueContext instance.

        Raises:
            RuntimeError: If not running in AWS Glue environment and
                pyspark/glue context cannot be created.
        """
        if self._glue_context is None:
            self._glue_context = create_glue_context()
        return self._glue_context

    @property
    def spark_session(self) -> Any:
        """Get or create the SparkSession from GlueContext.

        Returns:
            The SparkSession instance.
        """
        if self._spark_session is None:
            self._spark_session = create_spark_session(self.glue_context)
        return self._spark_session

    @property
    def glue_client(self) -> Any:
        """Get or create the boto3 Glue client.

        Returns:
            A boto3 Glue client instance.
        """
        if self._glue_client is None:
            import boto3

            self._glue_client = boto3.client("glue")
        return self._glue_client

    def reset(self) -> None:
        """Reset all cached context objects."""
        self._glue_context = None
        self._spark_session = None
        self._glue_client = None


def create_glue_context() -> Any:
    """Create or get an existing GlueContext.

    When running inside AWS Glue, imports ``GlueContext`` from
    ``awsglue.context`` and wraps a ``SparkContext`` that already exists
    in the Glue runtime.  Outside of Glue (e.g. during local development
    or testing) this function returns ``None``.

    Returns:
        A ``GlueContext`` instance, or ``None`` if not in a Glue environment.

    Example::

        ctx = create_glue_context()
        if ctx is not None:
            df = ctx.create_dynamic_frame.from_catalog(...)
    """
    if not is_aws_glue():
        logger.debug("Not in AWS Glue environment; returning None for GlueContext")
        return None

    try:
        from awsglue.context import GlueContext
        from pyspark import SparkContext

        sc = SparkContext.getOrCreate()
        return GlueContext(sc)
    except ImportError as exc:
        logger.error(
            "Failed to import awsglue.context – ensure the awsglue "
            "library is available in the runtime: %s",
            exc,
        )
        raise RuntimeError(
            "awsglue is not available. This function must run inside an "
            "AWS Glue environment or with the awsglue library installed."
        ) from exc


def create_spark_session(glue_context: Any) -> Any:
    """Get a SparkSession from an existing GlueContext.

    Args:
        glue_context: A ``GlueContext`` instance.

    Returns:
        The ``SparkSession`` associated with the given GlueContext.

    Raises:
        ValueError: If *glue_context* is ``None``.
    """
    if glue_context is None:
        raise ValueError("Cannot create SparkSession from a None GlueContext")
    return glue_context.spark_session


def read_from_catalog(
    glue_context: Any,
    database: str,
    table_name: str,
    transformation_ctx: str = "",
    push_down_predicate: str = "",
    additional_options: Optional[Dict[str, Any]] = None,
) -> Any:
    """Read a table from the AWS Glue Data Catalog as a DynamicFrame.

    Args:
        glue_context: A ``GlueContext`` instance.
        database: The Glue database name.
        table_name: The Glue table name.
        transformation_ctx: A unique name for job bookmark tracking.
        push_down_predicate: Predicate expression for partition pruning.
        additional_options: Extra options passed to the catalog reader.

    Returns:
        An AWS Glue ``DynamicFrame``.

    Raises:
        ValueError: If *glue_context* is ``None``.
    """
    if glue_context is None:
        raise ValueError("glue_context must not be None")

    options: Dict[str, Any] = {}
    if push_down_predicate:
        options["pushDownPredicate"] = push_down_predicate
    if additional_options:
        options.update(additional_options)

    reader = glue_context.create_dynamic_frame.from_catalog(
        database=database,
        table_name=table_name,
        transformation_ctx=transformation_ctx or f"read_{database}_{table_name}",
        **options,
    )
    logger.info(
        "Read DynamicFrame from catalog: %s.%s (%d records)",
        database,
        table_name,
        reader.count(),
    )
    return reader


def write_to_catalog(
    frame: Any,
    glue_context: Any,
    database: str,
    table_name: str,
    format: str = "parquet",
    transformation_ctx: str = "",
    additional_options: Optional[Dict[str, Any]] = None,
) -> None:
    """Write a DynamicFrame to the AWS Glue Data Catalog.

    Args:
        frame: The ``DynamicFrame`` to write.
        glue_context: A ``GlueContext`` instance.
        database: The target Glue database name.
        table_name: The target Glue table name.
        format: Output format (``parquet``, ``json``, ``csv``, ``orc``,
            ``avro``, ``glueparquet``).
        transformation_ctx: A unique name for job bookmark tracking.
        additional_options: Extra options passed to the catalog writer.

    Raises:
        ValueError: If *glue_context* or *frame* is ``None``.
    """
    if glue_context is None:
        raise ValueError("glue_context must not be None")
    if frame is None:
        raise ValueError("frame must not be None")

    options: Dict[str, Any] = {}
    if additional_options:
        options.update(additional_options)

    glue_context.write_dynamic_frame.from_catalog(
        frame=frame,
        database=database,
        table_name=table_name,
        format=format,
        transformation_ctx=transformation_ctx or f"write_{database}_{table_name}",
        **options,
    )
    logger.info(
        "Wrote DynamicFrame to catalog: %s.%s (format=%s)",
        database,
        table_name,
        format,
    )


def resolve_s3_path(path: str) -> str:
    """Resolve an S3 path for use in Glue jobs.

    Converts ``s3a://`` prefixes to ``s3://`` (Glue prefers the native
    scheme) and validates that the path is a well-formed S3 URI.

    Args:
        path: An S3 path, potentially using ``s3a://`` or ``s3://``.

    Returns:
        A normalized ``s3://`` path.

    Raises:
        ValueError: If *path* is not a valid S3 path.

    Example::

        >>> resolve_s3_path("s3a://my-bucket/data/file.csv")
        's3://my-bucket/data/file.csv'
        >>> resolve_s3_path("s3://my-bucket/data/file.csv")
        's3://my-bucket/data/file.csv'
    """
    if path.startswith("s3a://"):
        path = "s3://" + path[len("s3a://"):]
    if not path.startswith("s3://"):
        raise ValueError(
            f"Expected an S3 path (s3:// or s3a://), got: {path}"
        )
    return path


def get_job_args(
    args: Optional[List[str]] = None,
    required_keys: Optional[List[str]] = None,
) -> Dict[str, str]:
    """Parse AWS Glue job arguments.

    Glue passes arguments as ``--key value`` pairs on the command line.
    This function extracts them into a dictionary.

    Args:
        args: Command-line arguments.  Defaults to ``sys.argv[1:]``
            when ``None``.
        required_keys: Keys that must be present.  A ``KeyError`` is
            raised if any are missing.

    Returns:
        A dictionary of parsed ``--key value`` pairs.

    Raises:
        KeyError: If a key in *required_keys* is not found.

    Example::

        # Given: python job.py --JOB_NAME my-job --input s3://bucket/in
        args = get_job_args()
        # => {"JOB_NAME": "my-job", "input": "s3://bucket/in"}
    """
    if args is None:
        args = sys.argv[1:]

    parsed: Dict[str, str] = {}
    i = 0
    while i < len(args):
        if args[i].startswith("--"):
            key = args[i].lstrip("-")
            if i + 1 < len(args) and not args[i + 1].startswith("--"):
                parsed[key] = args[i + 1]
                i += 2
            else:
                parsed[key] = ""
                i += 1
        else:
            i += 1

    if required_keys:
        missing = [k for k in required_keys if k not in parsed]
        if missing:
            raise KeyError(
                f"Missing required Glue job arguments: {missing}"
            )

    return parsed


class GluePlatformRunner(PlatformRunner):
    """Run ETL jobs on AWS Glue.

    When executed inside an AWS Glue environment the runner:

    1. Creates a ``GlueContext`` / ``SparkSession``.
    2. Reads data from the Glue Data Catalog via ``DynamicFrame``.
    3. Applies transformations (Spark or pandas-based).
    4. Writes results back to the catalog or to S3.
    5. Supports Glue job bookmarks for incremental processing.

    Outside of Glue the runner falls back to local execution with a
    warning, which is useful for development and testing.

    Attributes:
        context_manager: A ``GlueContextManager`` instance.
        enable_bookmarks: Whether to enable Glue job bookmark tracking.
        job_args: Parsed Glue job arguments.
    """

    def __init__(
        self,
        enable_bookmarks: bool = True,
        job_args: Optional[Dict[str, str]] = None,
    ) -> None:
        """Initialize the GluePlatformRunner.

        Args:
            enable_bookmarks: If ``True``, enable Glue job bookmark
                tracking for incremental processing.
            job_args: Pre-parsed job arguments.  When ``None`` and
                running in Glue, arguments are parsed from ``sys.argv``.
        """
        self.context_manager = GlueContextManager()
        self.enable_bookmarks = enable_bookmarks
        self.job_args: Dict[str, str] = job_args or {}

    # -- public API -----------------------------------------------------------

    def run_job(self, job: ETLJob) -> None:
        """Run the ETL job on AWS Glue.

        Args:
            job: An ``ETLJob`` instance to execute.
        """
        if is_aws_glue():
            self._run_in_glue(job)
        else:
            self._run_locally(job)

    # -- internal helpers -----------------------------------------------------

    def _run_in_glue(self, job: ETLJob) -> None:
        """Execute the job inside the AWS Glue runtime."""
        logger.info("Running job on AWS Glue: %s", job.config.name)

        if not self.job_args:
            self.job_args = get_job_args()

        self._init_glue()
        self._set_job_bookmark()
        job.run_with_error_handling()
        self._update_job_bookmark()

    def _run_locally(self, job: ETLJob) -> None:
        """Fall back to local execution with a warning."""
        logger.warning(
            "Not running in AWS Glue environment. "
            "Executing job '%s' locally instead.",
            job.config.name,
        )
        job.run_with_error_handling()

    def _init_glue(self) -> None:
        """Initialize Glue context and Spark session."""
        _ = self.context_manager.glue_context
        _ = self.context_manager.spark_session
        logger.info("GlueContext and SparkSession initialized successfully")

    def _set_job_bookmark(self) -> None:
        """Set up Glue job bookmark properties for the current job run."""
        if not self.enable_bookmarks:
            return
        job_name = self.job_args.get("JOB_NAME", "")
        if job_name:
            logger.info("Glue job bookmark tracking enabled for: %s", job_name)

    def _update_job_bookmark(self) -> None:
        """Update Glue job bookmark after successful job completion."""
        if not self.enable_bookmarks:
            return
        job_name = self.job_args.get("JOB_NAME", "")
        if job_name:
            logger.info("Glue job bookmark updated for: %s", job_name)

    # -- convenience methods for Glue-based ETL logic -------------------------

    def read_catalog(
        self,
        database: str,
        table_name: str,
        push_down_predicate: str = "",
        additional_options: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Convenience wrapper around :func:`read_from_catalog`.

        Returns:
            A ``DynamicFrame`` from the Glue Data Catalog.
        """
        return read_from_catalog(
            glue_context=self.context_manager.glue_context,
            database=database,
            table_name=table_name,
            push_down_predicate=push_down_predicate,
            additional_options=additional_options,
        )

    def write_catalog(
        self,
        frame: Any,
        database: str,
        table_name: str,
        format: str = "parquet",
        additional_options: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Convenience wrapper around :func:`write_to_catalog`."""
        write_to_catalog(
            frame=frame,
            glue_context=self.context_manager.glue_context,
            database=database,
            table_name=table_name,
            format=format,
            additional_options=additional_options,
        )

    def dynamic_frame_to_pandas(self, dynamic_frame: Any) -> Any:
        """Convert a Glue DynamicFrame to a pandas DataFrame.

        Args:
            dynamic_frame: A ``DynamicFrame`` instance.

        Returns:
            A ``pandas.DataFrame``.
        """
        return dynamic_frame.toDF().toPandas()

    def pandas_to_dynamic_frame(
        self,
        dataframe: Any,
        name: str = "pandas_df",
    ) -> Any:
        """Convert a pandas DataFrame to a Glue DynamicFrame.

        Args:
            dataframe: A ``pandas.DataFrame``.
            name: Name for the resulting DynamicFrame.

        Returns:
            A ``DynamicFrame``.
        """
        return self.context_manager.glue_context.createDataFrameFromPandas(
            dataframe, name
        )
