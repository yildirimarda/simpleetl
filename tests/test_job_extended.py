"""
Extended tests for the ETL job execution engine.

Covers uncovered lines in job.py including:
- Invalid config type in ETLJob.__init__
- checkpoint_config.get("dir", ...) line
- extract() placeholder warning
- schema_registry property getter
- schema_registry setter with None, SchemaRegistry instance, and path string
- register_output_schema with and without registry
- validate_against_schema - KeyError for unknown name, successful validation
- validate_output - None data, non-DataFrame, schema discovery, helpful errors
- register_hook
- run_with_error_handling checkpoint resume
- run_with_error_handling on_success checkpoint delete
- run_with_error_handling permanent error (no retry)
- run_with_partial_failure - strict mode, lenient mode, max_errors, DLQ integration
- run_incremental non-DataFrame warning
"""

import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from simpleetl.core.job import ETLJob
from simpleetl.core.config import ETLJobConfig
from simpleetl.core.schema import Schema, ColumnDef
from simpleetl.core.schema_registry import FileSchemaRegistry, SchemaRegistry
from simpleetl.core.errors import PartialFailureError
from simpleetl.core.hooks import Hook, PRE_EXTRACT, POST_LOAD
from simpleetl.core.checkpoint import Checkpoint


# ---------------------------------------------------------------------------
# Test job subclass
# ---------------------------------------------------------------------------

class ConcreteTestJob(ETLJob):
    """A concrete ETL job for testing purposes."""

    def __init__(self, config, should_fail=False, fail_count=0, error_type=None):
        super().__init__(config)
        self.should_fail = should_fail
        self.fail_count = fail_count
        self.call_count = 0
        self.error_type = error_type or ConnectionError

    def run(self) -> None:
        self.call_count += 1
        if self.should_fail and self.call_count <= self.fail_count:
            raise self.error_type(f"Intentional failure on call {self.call_count}")


# ---------------------------------------------------------------------------
# 1. ETLJob.__init__ with invalid config type (lines 59-65)
# ---------------------------------------------------------------------------

class TestInitInvalidConfigType:
    """Tests for ETLJob.__init__ rejecting invalid config types."""

    def test_invalid_config_type_int(self):
        """Passing an int as config should raise TypeError."""
        with pytest.raises(TypeError, match="Config must be an ETLJobConfig"):
            ConcreteTestJob(123)

    def test_invalid_config_type_list(self):
        """Passing a list as config should raise TypeError."""
        with pytest.raises(TypeError, match="Config must be an ETLJobConfig"):
            ConcreteTestJob([1, 2, 3])

    def test_invalid_config_type_none(self):
        """Passing None as config should raise TypeError."""
        with pytest.raises(TypeError, match="Config must be an ETLJobConfig"):
            ConcreteTestJob(None)

    def test_invalid_config_type_object(self):
        """Passing a plain object as config should raise TypeError."""
        with pytest.raises(TypeError, match="Config must be an ETLJobConfig"):
            ConcreteTestJob(object())


# ---------------------------------------------------------------------------
# 2. checkpoint_config.get("dir", ...) line (line 83)
# ---------------------------------------------------------------------------

class TestCheckpointConfigDir:
    """Tests for checkpoint config dir default value."""

    def test_checkpoint_dir_default(self):
        """When checkpoint config has no 'dir' key, .checkpoints is used as default."""
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
            params={
                "checkpoint": {
                    "enabled": True,
                    "in_memory": True,
                }
            },
        )
        job = ConcreteTestJob(config)
        assert job.checkpoint_manager is not None

    def test_checkpoint_dir_custom(self):
        """When checkpoint config has a custom 'dir', it is used."""
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
            params={
                "checkpoint": {
                    "enabled": True,
                    "dir": "/tmp/custom_checkpoints",
                }
            },
        )
        job = ConcreteTestJob(config)
        assert job.checkpoint_manager is not None

    def test_checkpoint_disabled(self):
        """When checkpoint is disabled, checkpoint_manager is None."""
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
            params={
                "checkpoint": {
                    "enabled": False,
                }
            },
        )
        job = ConcreteTestJob(config)
        assert job.checkpoint_manager is None


# ---------------------------------------------------------------------------
# 3. extract() placeholder warning (line 115)
# ---------------------------------------------------------------------------

class TestExtractPlaceholder:
    """Tests for the default extract() placeholder method."""

    def test_extract_returns_none_and_warns(self):
        """Default extract() returns None and logs a warning."""
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
        )
        job = ConcreteTestJob(config)
        with patch.object(job.logger, "warning") as mock_warning:
            result = job.extract()
            assert result is None
            mock_warning.assert_called_once()

    def test_extract_warning_message_contains_job_name(self):
        """The extract warning should mention the job name."""
        config = ETLJobConfig(
            name="my_special_job",
            input_format="csv",
            output_format="csv",
        )
        job = ConcreteTestJob(config)
        with patch.object(job.logger, "warning") as mock_warning:
            job.extract()
            call_args = mock_warning.call_args
            assert "my_special_job" in call_args[0][1]


# ---------------------------------------------------------------------------
# 4. schema_registry property getter (line 180)
# ---------------------------------------------------------------------------

class TestSchemaRegistryGetter:
    """Tests for the schema_registry property getter."""

    def test_schema_registry_default_is_none(self):
        """By default, schema_registry should be None."""
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
        )
        job = ConcreteTestJob(config)
        assert job.schema_registry is None

    def test_schema_registry_returns_set_registry(self):
        """After setting a registry, the getter should return it."""
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
        )
        job = ConcreteTestJob(config)
        mock_registry = MagicMock(spec=SchemaRegistry)
        job.schema_registry = mock_registry
        assert job.schema_registry is mock_registry


# ---------------------------------------------------------------------------
# 5. schema_registry setter (lines 192-197)
# ---------------------------------------------------------------------------

class TestSchemaRegistrySetter:
    """Tests for the schema_registry setter with various input types."""

    def test_set_schema_registry_none(self):
        """Setting schema_registry to None should clear it."""
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
        )
        job = ConcreteTestJob(config)
        job.schema_registry = None
        assert job._schema_registry is None

    def test_set_schema_registry_instance(self):
        """Setting schema_registry with a SchemaRegistry instance."""
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
        )
        job = ConcreteTestJob(config)
        mock_registry = MagicMock(spec=SchemaRegistry)
        job.schema_registry = mock_registry
        assert job._schema_registry is mock_registry

    def test_set_schema_registry_with_string_path(self):
        """Setting schema_registry with a string path creates FileSchemaRegistry."""
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
        )
        job = ConcreteTestJob(config)
        with tempfile.TemporaryDirectory() as tmpdir:
            job.schema_registry = tmpdir
            assert isinstance(job._schema_registry, FileSchemaRegistry)

    def test_set_schema_registry_with_path_object(self):
        """Setting schema_registry with a Path object creates FileSchemaRegistry."""
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
        )
        job = ConcreteTestJob(config)
        with tempfile.TemporaryDirectory() as tmpdir:
            job.schema_registry = Path(tmpdir)
            assert isinstance(job._schema_registry, FileSchemaRegistry)


# ---------------------------------------------------------------------------
# 6. register_output_schema (lines 215-223)
# ---------------------------------------------------------------------------

class TestRegisterOutputSchema:
    """Tests for register_output_schema with and without registry."""

    def _make_schema(self):
        return Schema(
            columns=[
                ColumnDef("id", "int64", nullable=False),
                ColumnDef("name", "object"),
            ]
        )

    def test_register_without_registry(self):
        """Registering a schema without a registry stores it in-memory only."""
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
        )
        job = ConcreteTestJob(config)
        schema = self._make_schema()
        with patch.object(job.logger, "debug") as mock_debug:
            job.register_output_schema("users", schema)
            mock_debug.assert_called_once()
        assert "users" in job._output_schemas
        assert job._output_schemas["users"] is schema

    def test_register_with_registry(self):
        """Registering a schema with a registry persists it and logs info."""
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
        )
        job = ConcreteTestJob(config)
        mock_registry = MagicMock(spec=SchemaRegistry)
        job.schema_registry = mock_registry

        schema = self._make_schema()
        with patch.object(job.logger, "info") as mock_info:
            job.register_output_schema("orders", schema, version=3)
            mock_info.assert_called_once()

        mock_registry.register_schema.assert_called_once_with("orders", 3, schema)
        assert "orders" in job._output_schemas

    def test_register_multiple_schemas(self):
        """Registering multiple schemas stores them all."""
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
        )
        job = ConcreteTestJob(config)
        schema1 = self._make_schema()
        schema2 = Schema(columns=[ColumnDef("x", "int64")])
        job.register_output_schema("first", schema1)
        job.register_output_schema("second", schema2)
        assert len(job._output_schemas) == 2


# ---------------------------------------------------------------------------
# 7. validate_against_schema (lines 248-260)
# ---------------------------------------------------------------------------

class TestValidateAgainstSchema:
    """Tests for validate_against_schema."""

    def _make_schema(self):
        return Schema(
            columns=[
                ColumnDef("id", "int64", nullable=False),
                ColumnDef("name", "object"),
            ]
        )

    def test_validate_unknown_name_raises_key_error(self):
        """Validating against an unregistered name raises KeyError."""
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
        )
        job = ConcreteTestJob(config)
        df = pd.DataFrame({"id": [1], "name": ["a"]})
        with pytest.raises(KeyError, match="No output schema registered"):
            job.validate_against_schema("nonexistent", df)

    def test_validate_success(self):
        """Validating a matching DataFrame succeeds."""
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
        )
        job = ConcreteTestJob(config)
        schema = self._make_schema()
        job.register_output_schema("users", schema)

        df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        with patch.object(job.logger, "info") as mock_info:
            job.validate_against_schema("users", df)
            mock_info.assert_called_once()

    def test_validate_key_error_lists_available(self):
        """KeyError message should list available schema names."""
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
        )
        job = ConcreteTestJob(config)
        schema = self._make_schema()
        job.register_output_schema("my_schema", schema)

        df = pd.DataFrame({"id": [1]})
        with pytest.raises(KeyError, match="my_schema"):
            job.validate_against_schema("wrong_name", df)


# ---------------------------------------------------------------------------
# 8. validate_output (lines 724-773)
# ---------------------------------------------------------------------------

class TestValidateOutput:
    """Tests for validate_output method."""

    def _make_schema(self):
        return Schema(
            columns=[
                ColumnDef("id", "int64", nullable=False),
                ColumnDef("name", "object"),
            ]
        )

    def test_validate_output_none_data_raises_value_error(self):
        """validate_output raises ValueError when data is None."""
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
        )
        job = ConcreteTestJob(config)
        schema = self._make_schema()
        job.register_output_schema("users", schema)

        with pytest.raises(ValueError, match="Cannot validate None data"):
            job.validate_output(None, schema_name="users")

    def test_validate_output_non_dataframe_raises_type_error(self):
        """validate_output raises TypeError when data is not a DataFrame."""
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
        )
        job = ConcreteTestJob(config)
        schema = self._make_schema()
        job.register_output_schema("users", schema)

        with pytest.raises(TypeError, match="Data must be a pandas DataFrame"):
            job.validate_output([1, 2, 3], schema_name="users")

    def test_validate_output_discovers_schema_from_params(self):
        """validate_output auto-discovers schema_name from config.params['output_schema']."""
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
            params={"output_schema": "auto_schema"},
        )
        job = ConcreteTestJob(config)
        schema = self._make_schema()
        job.register_output_schema("auto_schema", schema)

        df = pd.DataFrame({"id": [1], "name": ["a"]})
        with patch.object(job.logger, "info") as mock_info:
            job.validate_output(df)
            mock_info.assert_called()

    def test_validate_output_uses_config_name_as_fallback(self):
        """If no output_schema param, uses config.name as schema name."""
        config = ETLJobConfig(
            name="fallback_schema",
            input_format="csv",
            output_format="csv",
        )
        job = ConcreteTestJob(config)
        schema = self._make_schema()
        job.register_output_schema("fallback_schema", schema)

        df = pd.DataFrame({"id": [1], "name": ["a"]})
        with patch.object(job.logger, "info"):
            job.validate_output(df)

    def test_validate_output_success_logs_and_validates(self):
        """validate_output logs validation and calls validate_against_schema."""
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
        )
        job = ConcreteTestJob(config)
        schema = self._make_schema()
        job.register_output_schema("users", schema)

        df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        with patch.object(job.logger, "info") as mock_info:
            job.validate_output(df, schema_name="users")
            # Should log both the validation start and success
            assert mock_info.call_count == 2

    def test_validate_output_key_error_raises_with_helpful_message(self):
        """validate_output raises KeyError with message about using register_output_schema."""
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
        )
        job = ConcreteTestJob(config)

        df = pd.DataFrame({"id": [1], "name": ["a"]})
        with pytest.raises(KeyError, match="Use register_output_schema"):
            job.validate_output(df, schema_name="missing_schema")


# ---------------------------------------------------------------------------
# 9. register_hook (line 276)
# ---------------------------------------------------------------------------

class TestRegisterHook:
    """Tests for register_hook."""

    def test_register_hook_delegates_to_hook_registry(self):
        """register_hook should delegate to the job's hook registry."""
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
        )
        job = ConcreteTestJob(config)

        mock_hook = MagicMock(spec=Hook)
        mock_hook.name = "test_hook"

        with patch.object(job._hook_registry, "register") as mock_register:
            job.register_hook(PRE_EXTRACT, mock_hook, priority=10)
            mock_register.assert_called_once_with(PRE_EXTRACT, mock_hook, 10)

    def test_register_hook_with_priority(self):
        """register_hook should pass priority to the registry."""
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
        )
        job = ConcreteTestJob(config)

        mock_hook = MagicMock(spec=Hook)
        mock_hook.name = "priority_hook"

        with patch.object(job._hook_registry, "register") as mock_register:
            job.register_hook(POST_LOAD, mock_hook, priority=5)
            mock_register.assert_called_once_with(POST_LOAD, mock_hook, 5)


# ---------------------------------------------------------------------------
# 10. run_with_error_handling checkpoint resume (lines 359-361)
# ---------------------------------------------------------------------------

class TestRunErrorHandlingCheckpointResume:
    """Tests for checkpoint resume in run_with_error_handling."""

    def test_checkpoint_resume_logs_info(self):
        """When a checkpoint exists, run_with_error_handling should log resume info."""
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
            params={
                "checkpoint": {
                    "enabled": True,
                    "in_memory": True,
                }
            },
        )
        job = ConcreteTestJob(config)

        # Pre-populate a checkpoint
        store = job.checkpoint_manager.store
        checkpoint = Checkpoint(
            job_id=job.checkpoint_manager.job_id,
            job_name="test_job",
            phase="transform",
            records_processed=42,
        )
        store.save(checkpoint)

        with patch.object(job.logger, "info") as mock_info:
            job.run_with_error_handling()
            # Should have logged the resume message
            resume_calls = [
                c for c in mock_info.call_args_list
                if "Resuming" in str(c)
            ]
            assert len(resume_calls) == 1
            assert "transform" in str(resume_calls[0])
            assert "42" in str(resume_calls[0])


# ---------------------------------------------------------------------------
# 11. run_with_error_handling on_success checkpoint delete (line 393)
# ---------------------------------------------------------------------------

class TestRunErrorHandlingCheckpointDelete:
    """Tests for checkpoint deletion on success."""

    def test_checkpoint_deleted_on_success(self):
        """On successful run, checkpoint should be deleted."""
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
            params={
                "checkpoint": {
                    "enabled": True,
                    "in_memory": True,
                }
            },
        )
        job = ConcreteTestJob(config)

        # Pre-populate a checkpoint
        store = job.checkpoint_manager.store
        checkpoint = Checkpoint(
            job_id=job.checkpoint_manager.job_id,
            job_name="test_job",
            phase="extract",
            records_processed=10,
        )
        store.save(checkpoint)

        with patch("time.sleep"):
            job.run_with_error_handling()

        # Checkpoint should be deleted after success
        assert store.load(job.checkpoint_manager.job_id) is None


# ---------------------------------------------------------------------------
# 12. run_with_error_handling permanent error (lines 410-413)
# ---------------------------------------------------------------------------

class TestRunErrorHandlingPermanentError:
    """Tests for permanent error handling (no retry)."""

    def test_permanent_error_not_retried(self):
        """Permanent errors (e.g., FileNotFoundError) should not be retried."""
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
            max_retries=3,
            retry_delay=0.01,
        )
        job = ConcreteTestJob(
            config,
            should_fail=True,
            fail_count=10,
            error_type=FileNotFoundError,
        )

        with patch("time.sleep") as mock_sleep, \
             patch.object(job.logger, "error"):
            with pytest.raises(FileNotFoundError):
                job.run_with_error_handling()

            # Should have been called exactly once (no retries)
            assert job.call_count == 1
            # Should not have slept at all
            assert mock_sleep.call_count == 0

    def test_permanent_error_logs_error(self):
        """Permanent error should log the 'not retrying' message."""
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
            max_retries=3,
            retry_delay=0.01,
        )
        job = ConcreteTestJob(
            config,
            should_fail=True,
            fail_count=10,
            error_type=FileNotFoundError,
        )

        with patch.object(job.logger, "error") as mock_error, \
             patch.object(job.logger, "warning"):
            with pytest.raises(FileNotFoundError):
                job.run_with_error_handling()

            # Should have logged the permanent error message
            error_calls = [
                c for c in mock_error.call_args_list
                if "Permanent error" in str(c)
            ]
            assert len(error_calls) == 1


# ---------------------------------------------------------------------------
# 13. run_with_partial_failure (lines 454-547)
# ---------------------------------------------------------------------------

class TestRunWithPartialFailure:
    """Tests for run_with_partial_failure in various modes."""

    def _failing_transform(self, record):
        if record.startswith("bad"):
            raise ValueError(f"Bad record: {record}")
        return record.upper()

    def _load_fn(self, data):
        pass  # no-op

    # -- strict mode -------------------------------------------------------

    def test_partial_failure_strict_mode_raises(self):
        """In strict mode, first failure raises PartialFailureError immediately."""
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
            params={
                "error_handling": {
                    "mode": "strict",
                }
            },
        )
        job = ConcreteTestJob(config)
        records = ["good", "bad", "also_good"]

        with pytest.raises(PartialFailureError) as exc_info:
            job.run_with_partial_failure(
                records,
                transform_fn=self._failing_transform,
                load_fn=self._load_fn,
            )

        assert exc_info.value.success_count == 1
        assert exc_info.value.failure_count == 1
        assert exc_info.value.failed_records[0][0] == 1  # index of "bad"

    def test_partial_failure_strict_mode_all_succeed(self):
        """In strict mode, if all succeed, no error is raised."""
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
            params={
                "error_handling": {
                    "mode": "strict",
                }
            },
        )
        job = ConcreteTestJob(config)
        records = ["good", "also_good"]

        result = job.run_with_partial_failure(
            records,
            transform_fn=self._failing_transform,
            load_fn=self._load_fn,
        )

        assert result["total"] == 2
        assert result["succeeded"] == 2
        assert result["failed"] == 0

    # -- lenient mode ------------------------------------------------------

    def test_partial_failure_lenient_mode_collects_errors(self):
        """In lenient mode, failures are collected and PartialFailureError is raised at end."""
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
            params={
                "error_handling": {
                    "mode": "lenient",
                }
            },
        )
        job = ConcreteTestJob(config)
        records = ["good", "bad", "bad_too", "fine"]

        with pytest.raises(PartialFailureError) as exc_info:
            job.run_with_partial_failure(
                records,
                transform_fn=self._failing_transform,
                load_fn=self._load_fn,
            )

        assert exc_info.value.success_count == 2
        assert exc_info.value.failure_count == 2

    def test_partial_failure_lenient_all_succeed(self):
        """In lenient mode, if all succeed, returns result dict."""
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
            params={
                "error_handling": {
                    "mode": "lenient",
                }
            },
        )
        job = ConcreteTestJob(config)
        records = ["a", "b", "c"]

        result = job.run_with_partial_failure(
            records,
            transform_fn=self._failing_transform,
            load_fn=self._load_fn,
        )

        assert result["total"] == 3
        assert result["succeeded"] == 3
        assert result["failed"] == 0
        assert result["errors"] == []

    # -- max_errors threshold ----------------------------------------------

    def test_partial_failure_max_errors_stops_early(self):
        """In lenient mode, processing stops when max_errors is reached."""
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
            params={
                "error_handling": {
                    "mode": "lenient",
                    "max_errors": 2,
                }
            },
        )
        job = ConcreteTestJob(config)
        records = ["bad1", "bad2", "bad3", "good"]

        with pytest.raises(PartialFailureError) as exc_info:
            job.run_with_partial_failure(
                records,
                transform_fn=self._failing_transform,
                load_fn=self._load_fn,
            )

        # Should have stopped after 2 errors
        assert exc_info.value.failure_count == 2

    # -- DLQ integration ---------------------------------------------------

    def test_partial_failure_lenient_with_dlq(self):
        """In lenient mode with dlq_path, failed records are added to DLQ."""
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
            params={
                "error_handling": {
                    "mode": "lenient",
                    "dlq_path": "/tmp/test_dlq.jsonl",
                }
            },
        )
        job = ConcreteTestJob(config)
        records = ["good", "bad", "bad_again"]

        with pytest.raises(PartialFailureError):
            job.run_with_partial_failure(
                records,
                transform_fn=self._failing_transform,
                load_fn=self._load_fn,
            )

        # DLQ should have entries
        assert job.dlq is not None
        assert job.dlq.count == 2

    def test_partial_failure_dlq_write_to_file(self):
        """DLQ entries should be written to file when dlq_path is set."""
        with tempfile.TemporaryDirectory() as tmpdir:
            dlq_path = os.path.join(tmpdir, "dlq.jsonl")
            config = ETLJobConfig(
                name="test_job",
                input_format="csv",
                output_format="csv",
                params={
                    "error_handling": {
                        "mode": "lenient",
                        "dlq_path": dlq_path,
                    }
                },
            )
            job = ConcreteTestJob(config)
            records = ["good", "bad"]

            with pytest.raises(PartialFailureError):
                job.run_with_partial_failure(
                    records,
                    transform_fn=self._failing_transform,
                    load_fn=self._load_fn,
                )

            # DLQ file should exist and have content
            assert os.path.exists(dlq_path)
            with open(dlq_path) as f:
                lines = f.readlines()
            assert len(lines) == 1  # one failed record

    def test_partial_failure_no_dlq_without_path(self):
        """Without dlq_path, no DLQ is created."""
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
            params={
                "error_handling": {
                    "mode": "lenient",
                }
            },
        )
        job = ConcreteTestJob(config)
        records = ["good", "bad"]

        with pytest.raises(PartialFailureError):
            job.run_with_partial_failure(
                records,
                transform_fn=self._failing_transform,
                load_fn=self._load_fn,
            )

        assert job.dlq is None


# ---------------------------------------------------------------------------
# 14. run_incremental non-DataFrame warning (lines 641-647)
# ---------------------------------------------------------------------------

class TestRunIncrementalNonDataFrame:
    """Tests for run_incremental with non-DataFrame data."""

    def test_incremental_non_dataframe_warning(self):
        """When extract returns non-DataFrame data, a warning is logged."""
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
            incremental=True,
            incremental_column="updated_at",
        )

        class NonDataFrameJob(ConcreteTestJob):
            def extract(self, **kwargs):
                # Return a non-DataFrame (list of dicts)
                return [{"id": 1, "updated_at": "2024-01-01"}]

            def transform(self, data):
                return data

        job = NonDataFrameJob(config)

        with patch.object(job.logger, "warning") as mock_warning, \
             patch.object(job.logger, "info"):
            job.run_incremental("test_source")

            # Should have logged the non-DataFrame warning
            warning_calls = [
                c for c in mock_warning.call_args_list
                if "non-DataFrame" in str(c)
            ]
            assert len(warning_calls) == 1

    def test_incremental_non_dataframe_does_not_update_watermark(self):
        """When data is not a DataFrame, watermark is not auto-updated."""
        config = ETLJobConfig(
            name="test_job",
            input_format="csv",
            output_format="csv",
            incremental=True,
            incremental_column="updated_at",
        )

        class NonDataFrameJob(ConcreteTestJob):
            def extract(self, **kwargs):
                return [{"id": 1, "updated_at": "2024-01-01"}]

            def transform(self, data):
                return data

        job = NonDataFrameJob(config)

        with patch.object(job.logger, "warning") as mock_warning, \
             patch.object(job.logger, "info"):
            job.run_incremental("test_source")

            # Should have the non-DataFrame warning
            warning_calls = [
                c for c in mock_warning.call_args_list
                if "non-DataFrame" in str(c) or "manually" in str(c)
            ]
            assert len(warning_calls) >= 1
