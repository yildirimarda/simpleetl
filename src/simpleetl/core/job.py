"""
Base ETL job interface and implementation.
"""

import random
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import logging
from .config import ETLJobConfig, load_config
from .checkpoint import CheckpointManager, InMemoryCheckpointStore
from .dlq import DeadLetterQueue
from .errors import (
    PartialFailureError,
    ErrorClassification,
    classify_error,
)
from .schema import Schema
from .schema_registry import FileSchemaRegistry, SchemaRegistry
from .hooks import (
    Hook,
    HookContext,
    HookRegistry,
    PRE_EXTRACT,
    POST_EXTRACT,
    PRE_TRANSFORM,
    POST_TRANSFORM,
    PRE_LOAD,
    POST_LOAD,
    ON_ERROR,
    ON_COMPLETE,
    get_hook_registry,
)
from .incremental import WatermarkManager

# Set up logger for the ETL framework
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())  # Avoid "No handler found" warnings


class ETLJob(ABC):
    """
    Abstract base class for all ETL jobs.

    Subclasses must implement the `run` method to define the ETL logic.
    """

    checkpoint_manager: Optional[CheckpointManager]

    def __init__(self, config: ETLJobConfig | str | Dict[str, Any]):
        """
        Initialize the ETL job with a configuration.

        Args:
            config: Either an ETLJobConfig instance, a path to a config file,
                    or a dictionary containing the configuration.
        """
        if isinstance(config, ETLJobConfig):
            self.config = config
        elif isinstance(config, (str, dict)):
            if isinstance(config, str):
                self.config = load_config(config)
            else:  # dict
                self.config = ETLJobConfig(**config)
        else:
            raise TypeError(
                "Config must be an ETLJobConfig instance, a path to a config file, or a dictionary."
            )

        # Set up job-specific logger
        self.logger = logging.getLogger(f"{__name__}.{self.config.name}")

        # Error handling configuration
        error_config = self.config.params.get("error_handling", {})
        self.error_mode = error_config.get("mode", "strict")  # strict or lenient
        self.max_errors = error_config.get("max_errors", 0)
        self.dlq_path = error_config.get("dlq_path", "")

        # Checkpoint manager
        checkpoint_config = self.config.params.get("checkpoint", {})
        checkpoint_config.get("dir", ".checkpoints")
        checkpoint_enabled = checkpoint_config.get("enabled", False)
        if checkpoint_enabled:
            self.checkpoint_manager = CheckpointManager(
                job_name=self.config.name,
                store=InMemoryCheckpointStore()
                if checkpoint_config.get("in_memory", False)
                else None,
            )
        else:
            self.checkpoint_manager = None

        # Dead letter queue
        self.dlq = DeadLetterQueue() if self.dlq_path else None

        # Schema registry
        self._schema_registry: Optional[SchemaRegistry] = None
        self._output_schemas: Dict[str, Schema] = {}

        # Schema evolution mode
        self.schema_evolution_mode: str = self.config.params.get(
            "schema_evolution_mode", "additive"
        )

        # Per-job hook registry (falls back to global registry)
        self._hook_registry: HookRegistry = get_hook_registry()

    @abstractmethod
    def run(self) -> None:
        """
        Execute the ETL job.

        This method must be implemented by subclasses to define the extract,
        transform, and load steps.
        """
        pass

    def extract(self, **kwargs: Any) -> Any:
        """
        Extract data from the source.

        This is a placeholder method that can be overridden by subclasses.
        By default, it returns None and logs a warning.

        Args:
            **kwargs: Additional keyword arguments (e.g., source,
                incremental_column, watermark_value for incremental mode).

        Returns:
            Extracted data (format depends on the source).
        """
        self._execute_hooks(PRE_EXTRACT)
        self.logger.warning(
            "extract() method not implemented for job %s. Returning None.",
            self.config.name,
        )
        data = None
        self._execute_hooks(POST_EXTRACT, data=data)
        return data

    def transform(self, data: Any) -> Any:
        """
        Transform the extracted data.

        This is a placeholder method that can be overridden by subclasses.
        By default, it returns the data unchanged.

        Args:
            data: The data extracted from the source.

        Returns:
            Transformed data.
        """
        self._execute_hooks(PRE_TRANSFORM, data=data)
        self.logger.debug(
            "transform() method not implemented for job %s. Returning data unchanged.",
            self.config.name,
        )
        result = data
        self._execute_hooks(POST_TRANSFORM, data=result)
        return result

    def load(self, data: Any) -> None:
        """
        Load the transformed data to the destination.

        This is a placeholder method that can be overridden by subclasses.
        By default, it logs a warning and does nothing.

        Args:
            data: The transformed data to load.
        """
        self._execute_hooks(PRE_LOAD, data=data)
        self.logger.warning(
            "load() method not implemented for job %s. Data not loaded.",
            self.config.name,
        )
        self._execute_hooks(POST_LOAD, data=data)

    # -- schema registry ----------------------------------------------------

    @property
    def schema_registry(self) -> Optional[SchemaRegistry]:
        """Return the schema registry, if configured."""
        return self._schema_registry

    @schema_registry.setter
    def schema_registry(
        self, registry: Union[SchemaRegistry, str, Path, None]
    ) -> None:
        """Set the schema registry.

        Args:
            registry: A SchemaRegistry instance, a path to a directory
                (creates a FileSchemaRegistry), or None to clear.
        """
        if registry is None:
            self._schema_registry = None
        elif isinstance(registry, SchemaRegistry):
            self._schema_registry = registry
        else:
            self._schema_registry = FileSchemaRegistry(registry)

    def register_output_schema(
        self,
        name: str,
        schema: Schema,
        version: int = 1,
    ) -> None:
        """Register an output schema for this job.

        If a schema registry is configured, the schema is persisted.
        The schema is also stored in-memory for validation.

        Args:
            name: Logical name for the output (e.g. ``"users"``).
            schema: Schema to register.
            version: Version number when using a schema registry.
        """
        self._output_schemas[name] = schema

        if self._schema_registry is not None:
            self._schema_registry.register_schema(name, version, schema)
            self.logger.info(
                "Registered output schema '%s' version %d", name, version
            )
        else:
            self.logger.debug(
                "Stored output schema '%s' in-memory (no registry configured)",
                name,
            )

    def validate_against_schema(
        self,
        name: str,
        df: Any,
        strict_nullability: bool = False,
        strict_types: bool = False,
    ) -> None:
        """Validate a DataFrame against a registered output schema.

        Args:
            name: Name of the registered output schema.
            df: DataFrame to validate.
            strict_nullability: Enforce non-nullable constraints.
            strict_types: Enforce dtype constraints.

        Raises:
            KeyError: If *name* is not a registered output schema.
            SchemaValidationError: If validation fails.
        """

        if name not in self._output_schemas:
            raise KeyError(
                f"No output schema registered under name '{name}'. "
                f"Available: {list(self._output_schemas)}"
            )

        schema = self._output_schemas[name]
        schema.validate(
            df,
            strict_nullability=strict_nullability,
            strict_types=strict_types,
        )
        self.logger.info(
            "DataFrame validated successfully against schema '%s'", name
        )

    # -- format options helpers ------------------------------------------------

    def get_format_options(self, format_name: Optional[str] = None) -> Dict[str, Any]:
        """Get format-specific options for reading/writing.

        Merges global params with format-specific options from config.

        Args:
            format_name: Format name to get options for. If None, uses
                input_format for extract and output_format for load context.
                This method returns merged dict without context awareness.

        Returns:
            Dictionary of format-specific options.
        """
        format_opts = self.config.format_options.get(format_name, {})
        # Merge with any chunk_size from batch_size config
        if self.config.batch_size:
            format_opts = dict(format_opts)
            format_opts.setdefault("chunk_size", self.config.batch_size)
        return format_opts

    # -- hook helpers -------------------------------------------------------

    def register_hook(
        self, hook_point: str, hook: Hook, priority: int = 0
    ) -> None:
        """Register a hook on this job's hook registry.

        Args:
            hook_point: One of the hook point constants (e.g. ``pre_extract``).
            hook: The hook instance to register.
            priority: Execution priority (higher runs first).
        """
        self._hook_registry.register(hook_point, hook, priority)

    def _execute_hooks(
        self, hook_point: str, data: Any = None, error: Optional[Exception] = None
    ) -> HookContext:
        """Execute all hooks registered for *hook_point*.

        Args:
            hook_point: The hook point to trigger.
            data: Optional data to include in the context.
            error: Optional error to include in the context.

        Returns:
            The ``HookContext`` that was passed to each hook.
        """
        import time as _time

        ctx = HookContext(
            job=self,
            phase=hook_point,
            data=data,
            error=error,
            start_time=_time.time(),
        )
        self._hook_registry.execute(hook_point, ctx)
        return ctx

    def _setup_logging(self) -> None:
        """
        Set up logging for the job if not already configured.
        Uses the log level from the job configuration.
        """
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

            # Set log level from config, default to INFO if invalid
            try:
                level = getattr(logging, self.config.log_level.upper())
            except AttributeError:
                level = logging.INFO
                self.logger.warning(
                    "Invalid log level '%s' in config, defaulting to INFO",
                    self.config.log_level,
                )
            self.logger.setLevel(level)

    def _calculate_backoff(self, attempt: int, base_delay: float) -> float:
        """
        Calculate exponential backoff with jitter.

        Uses "full jitter" approach: sleep = random(0, min(cap, base * 2^attempt)).

        Args:
            attempt: Current attempt number (0-indexed).
            base_delay: Base delay in seconds.

        Returns:
            Sleep duration in seconds.
        """
        cap = 60.0  # Maximum backoff cap
        exp_delay = base_delay * (2 ** attempt)
        capped = min(exp_delay, cap)
        return random.uniform(0, capped)

    def run_with_error_handling(self) -> None:
        """
        Run the ETL job with error handling, logging, and retry mechanisms.

        This method wraps the `run` method in a retry loop with exponential
        backoff and jitter. Supports checkpoint resume and DLQ integration.
        """
        self._setup_logging()

        max_retries = self.config.max_retries
        base_delay = self.config.retry_delay

        # Check for checkpoint resume
        if self.checkpoint_manager and self.checkpoint_manager.should_resume():
            checkpoint = self.checkpoint_manager.load_checkpoint()
            if checkpoint:
                self.logger.info(
                    "Resuming job '%s' from checkpoint: phase=%s, records=%d",
                    self.config.name, checkpoint.phase, checkpoint.records_processed,
                )

        for attempt in range(max_retries + 1):
            try:
                if attempt > 0:
                    self.logger.info(
                        "Starting ETL job attempt %d/%d: %s",
                        attempt + 1, max_retries + 1, self.config.name,
                    )
                else:
                    self.logger.info("Starting ETL job: %s", self.config.name)

                self.run()

                if attempt > 0:
                    self.logger.info(
                        "ETL job completed successfully on attempt %d: %s",
                        attempt + 1, self.config.name,
                    )
                else:
                    self.logger.info(
                        "ETL job completed successfully: %s", self.config.name
                    )

                # Execute on_complete hooks
                self._execute_hooks(ON_COMPLETE)

                # Clean up checkpoint on success
                if self.checkpoint_manager:
                    self.checkpoint_manager.delete_checkpoint()

                return

            except Exception as e:
                classification = classify_error(e)
                self.logger.warning(
                    "ETL job attempt %d/%d failed (classification=%s): %s",
                    attempt + 1, max_retries + 1, classification.value, str(e),
                )

                # Execute on_error hooks
                self._execute_hooks(ON_ERROR, error=e)

                if attempt < max_retries:
                    # Don't retry permanent errors
                    if classification == ErrorClassification.PERMANENT:
                        self.logger.error(
                            "Permanent error detected, not retrying: %s", str(e)
                        )
                        raise

                    # Exponential backoff with jitter
                    sleep_time = self._calculate_backoff(attempt, base_delay)
                    self.logger.info("Retrying in %.2f seconds...", sleep_time)
                    time.sleep(sleep_time)
                else:
                    self.logger.error(
                        "ETL job failed after %d attempts: %s",
                        max_retries + 1, self.config.name, exc_info=True,
                    )
                    raise

    def run_with_partial_failure(
        self,
        records: List[Any],
        transform_fn=None,
        load_fn=None,
    ) -> Dict[str, Any]:
        """
        Process records individually, collecting failures instead of failing fast.

        In lenient mode, each record is processed independently. Failed records
        are added to the DLQ and counted, but do not stop processing of
        remaining records.

        Args:
            records: List of records to process.
            transform_fn: Optional transform function. Defaults to self.transform.
            load_fn: Optional load function. Defaults to self.load.

        Returns:
            Dictionary with processing results:
                - total: Total number of records
                - succeeded: Number of successfully processed records
                - failed: Number of failed records
                - errors: List of (index, error_message) tuples

        Raises:
            PartialFailureError: If any records fail and error_mode is 'strict'.
        """
        self._setup_logging()

        if transform_fn is None:
            transform_fn = self.transform
        if load_fn is None:
            load_fn = self.load

        total = len(records)
        succeeded = 0
        failed_records: List[tuple[int, str]] = []

        self.logger.info(
            "Processing %d records with partial failure handling (mode=%s, max_errors=%d)",
            total, self.error_mode, self.max_errors,
        )

        for idx, record in enumerate(records):
            try:
                transformed = transform_fn(record)
                load_fn(transformed)
                succeeded += 1

                # Checkpoint after each successful record if enabled
                if self.checkpoint_manager:
                    self.checkpoint_manager.save_checkpoint(
                        phase="transform_load",
                        records_processed=succeeded,
                        metadata={"last_processed_index": idx},
                    )

            except Exception as e:
                error_msg = str(e)
                failed_records.append((idx, error_msg))

                # Add to DLQ
                if self.dlq:
                    self.dlq.add_entry(
                        record_data=record,
                        error=e,
                        phase="transform_load",
                        record_index=idx,
                    )

                self.logger.warning(
                    "Record %d failed: %s", idx, error_msg
                )

                # In strict mode, fail immediately
                if self.error_mode == "strict":
                    raise PartialFailureError(
                        f"Record {idx} failed in strict mode",
                        failed_records=[(idx, error_msg)],
                        success_count=succeeded,
                        job_name=self.config.name,
                    )

                # In lenient mode, check max_errors threshold
                if self.max_errors > 0 and len(failed_records) >= self.max_errors:
                    self.logger.error(
                        "Max errors (%d) exceeded, stopping processing",
                        self.max_errors,
                    )
                    break

        # Write DLQ to file if configured
        if self.dlq and self.dlq.count > 0 and self.dlq_path:
            self.dlq.write_to_dlq(self.dlq_path, format="jsonl")
            self.logger.info(
                "Wrote %d failed records to DLQ: %s",
                self.dlq.count, self.dlq_path,
            )

        result = {
            "total": total,
            "succeeded": succeeded,
            "failed": len(failed_records),
            "errors": failed_records,
        }

        self.logger.info(
            "Partial failure processing complete: %d succeeded, %d failed out of %d total",
            succeeded, len(failed_records), total,
        )

        # Raise PartialFailureError if any records failed
        if failed_records:
            raise PartialFailureError(
                f"{len(failed_records)} of {total} records failed processing",
                failed_records=failed_records,
                success_count=succeeded,
                job_name=self.config.name,
            )

        return result

    def run_incremental(self, source: str, **kwargs) -> None:
        """Run the ETL job in incremental (delta) mode.

        Loads the watermark from the previous run, extracts only records
        newer than the watermark, transforms, loads, and updates the
        watermark to the maximum value of the incremental column.

        Args:
            source: Data source identifier (table name, file path, etc.).
            **kwargs: Additional keyword arguments passed to extract/transform/load.

        Raises:
            ValueError: If incremental is not enabled in config or
                incremental_column is not set.
        """
        if not self.config.incremental:
            raise ValueError(
                "Incremental loading is not enabled in config. "
                "Set incremental=True in the job configuration."
            )
        if not self.config.incremental_column:
            raise ValueError(
                "incremental_column must be set in config for incremental loading."
            )

        self._setup_logging()
        self.logger.info(
            "Starting incremental ETL job '%s' on source '%s' using column '%s'",
            self.config.name,
            source,
            self.config.incremental_column,
        )

        # Step 1: Load watermark from previous run
        wm_manager = WatermarkManager.from_config(self.config)
        watermark = wm_manager.get_watermark(self.config.name, source)
        watermark_value = watermark.value if watermark else None

        if watermark_value is not None:
            self.logger.info(
                "Resuming from watermark: %s = %s",
                self.config.incremental_column,
                watermark_value,
            )
        else:
            self.logger.info("No previous watermark found. Running full extract.")

        # Step 2: Extract only new/changed records
        self._execute_hooks(PRE_EXTRACT)
        data = self.extract(
            source=source,
            incremental_column=self.config.incremental_column,
            watermark_value=watermark_value,
            **kwargs,
        )
        self._execute_hooks(POST_EXTRACT, data=data)

        if data is None:
            self.logger.info("No new data to process. Exiting.")
            return

        import pandas as pd

        if isinstance(data, pd.DataFrame) and data.empty:
            self.logger.info("No new data to process. Exiting.")
            return

        # Step 3: Transform
        self._execute_hooks(PRE_TRANSFORM, data=data)
        transformed_data = self.transform(data)
        self._execute_hooks(POST_TRANSFORM, data=transformed_data)

        # Step 4: Load
        self._execute_hooks(PRE_LOAD, data=transformed_data)
        self.load(transformed_data, **kwargs)
        self._execute_hooks(POST_LOAD, data=transformed_data)

        # Step 5: Update watermark to max value of incremental column
        if isinstance(transformed_data, pd.DataFrame):
            inc_col = self.config.incremental_column
            if inc_col in transformed_data.columns:
                max_value = transformed_data[inc_col].max()
                wm_manager.set_watermark(
                    job_name=self.config.name,
                    source=source,
                    column=inc_col,
                    value=max_value,
                )
                self.logger.info(
                    "Watermark updated: %s = %s", inc_col, max_value
                )
            else:
                self.logger.warning(
                    "Incremental column '%s' not found in output data. "
                    "Watermark not updated.",
                    inc_col,
                )
        else:
            self.logger.warning(
                "Cannot auto-update watermark for non-DataFrame data. "
                "Use set_watermark() manually."
            )

        self._execute_hooks(ON_COMPLETE)
        self.logger.info("Incremental ETL job completed: %s", self.config.name)

    def merge_load(
        self,
        data,
        destination: str,
        table_name: str,
        merge_keys: list,
        **kwargs,
    ) -> None:
        """Load data using UPSERT (merge) semantics.

        Supports PostgreSQL ON CONFLICT, MySQL REPLACE, and SQLite REPLACE
        via the DatabaseWriter.merge() method.

        Args:
            data: pandas DataFrame to load.
            destination: Database connection string or SQLAlchemy engine.
            table_name: Target table name.
            merge_keys: Columns to match for merge (e.g., ["id"]).
            **kwargs: Additional keyword arguments passed to merge().
        """
        from simpleetl.formats.database import DatabaseWriter

        self._setup_logging()
        self.logger.info(
            "Merge loading %d records into '%s' on keys %s",
            len(data) if hasattr(data, "__len__") else "?",
            table_name,
            merge_keys,
        )

        writer = DatabaseWriter()
        writer.merge(
            data=data,
            destination=destination,
            table_name=table_name,
            merge_keys=merge_keys,
            **kwargs,
        )

        self.logger.info("Merge load completed successfully for '%s'", table_name)

    def validate_output(
        self,
        data,
        schema_name: Optional[str] = None,
        strict_nullability: bool = False,
        strict_types: bool = False,
    ) -> None:
        """Validate transformed data against a registered output schema.

        Automatically discovers schema_name from config if not provided.

        Args:
            data: pandas DataFrame to validate.
            schema_name: Name of the registered schema. Defaults to
                ``config.params.get("output_schema")`` or config.name.
            strict_nullability: Enforce non-nullable constraints.
            strict_types: Enforce dtype constraints.

        Raises:
            KeyError: If no schema is registered.
            SchemaValidationError: If validation fails.
        """
        import pandas as pd

        if schema_name is None:
            schema_name = self.config.params.get("output_schema", self.config.name)

        if data is None:
            raise ValueError("Cannot validate None data")

        if not isinstance(data, pd.DataFrame):
            raise TypeError(
                f"Data must be a pandas DataFrame, got {type(data).__name__}"
            )

        self._setup_logging()
        self.logger.info("Validating output data against schema '%s'", schema_name)

        try:
            self.validate_against_schema(
                schema_name,
                data,
                strict_nullability=strict_nullability,
                strict_types=strict_types,
            )
        except KeyError:
            raise KeyError(
                f"No output schema registered for '{schema_name}'. "
                f"Use register_output_schema() to register one."
            ) from None
