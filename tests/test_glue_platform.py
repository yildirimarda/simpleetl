"""
Tests for the AWS Glue platform runner and Glue Data Catalog formats.

Mocks boto3, GlueContext, SparkSession, and DynamicFrame to test all
code paths without requiring an actual AWS Glue runtime.
"""

import sys
from unittest.mock import MagicMock, patch

import pytest

from simpleetl.core.job import ETLJob
from simpleetl.formats.glue_catalog import (
    SUPPORTED_FORMATS,
    GlueCatalogReader,
    GlueCatalogWriter,
)
from simpleetl.platforms.glue import (
    GlueContextManager,
    GluePlatformRunner,
    create_glue_context,
    create_spark_session,
    get_job_args,
    read_from_catalog,
    resolve_s3_path,
    write_to_catalog,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


class DummyJob(ETLJob):
    """A minimal ETL job for testing."""

    def __init__(self) -> None:
        self.config = MagicMock()
        self.config.name = "test-job"

    def run(self) -> None:
        pass


@pytest.fixture
def mock_glue_context():
    """Create a mock GlueContext with DynamicFrame support."""
    ctx = MagicMock(name="GlueContext")
    ctx.spark_session = MagicMock(name="SparkSession")

    # Mock DynamicFrame returned by from_catalog
    mock_frame = MagicMock(name="DynamicFrame")
    mock_frame.count.return_value = 42
    mock_frame.toDF.return_value = MagicMock(name="SparkDataFrame")
    ctx.create_dynamic_frame.from_catalog.return_value = mock_frame

    return ctx


@pytest.fixture
def mock_spark_session():
    """Create a mock SparkSession."""
    return MagicMock(name="SparkSession")


@pytest.fixture
def mock_glue_client():
    """Create a mock boto3 Glue client."""
    return MagicMock(name="GlueClient")


# ---------------------------------------------------------------------------
# Tests for resolve_s3_path
# ---------------------------------------------------------------------------


class TestResolveS3Path:
    def test_s3_scheme_passthrough(self):
        assert resolve_s3_path("s3://bucket/key") == "s3://bucket/key"

    def test_s3a_to_s3_conversion(self):
        assert resolve_s3_path("s3a://bucket/key") == "s3://bucket/key"

    def test_non_s3_path_raises(self):
        with pytest.raises(ValueError, match="Expected an S3 path"):
            resolve_s3_path("/local/path")

    def test_empty_string_raises(self):
        with pytest.raises(ValueError, match="Expected an S3 path"):
            resolve_s3_path("")

    def test_gcs_path_raises(self):
        with pytest.raises(ValueError, match="Expected an S3 path"):
            resolve_s3_path("gs://bucket/key")


# ---------------------------------------------------------------------------
# Tests for get_job_args
# ---------------------------------------------------------------------------


class TestGetJobArgs:
    def test_basic_parsing(self):
        args = ["--JOB_NAME", "my-job", "--input", "s3://bucket/in"]
        result = get_job_args(args)
        assert result == {"JOB_NAME": "my-job", "input": "s3://bucket/in"}

    def test_empty_args(self):
        assert get_job_args([]) == {}

    def test_flag_without_value(self):
        args = ["--verbose"]
        result = get_job_args(args)
        assert result == {"verbose": ""}

    def test_required_keys_present(self):
        args = ["--JOB_NAME", "my-job"]
        result = get_job_args(args, required_keys=["JOB_NAME"])
        assert result["JOB_NAME"] == "my-job"

    def test_required_keys_missing(self):
        args = ["--input", "s3://bucket/in"]
        with pytest.raises(KeyError, match="JOB_NAME"):
            get_job_args(args, required_keys=["JOB_NAME"])

    def test_multiple_missing_keys(self):
        args = []
        with pytest.raises(KeyError, match="KEY1"):
            get_job_args(args, required_keys=["KEY1", "KEY2"])

    def test_ignores_positional_args(self):
        args = ["positional", "--key", "value"]
        result = get_job_args(args)
        assert result == {"key": "value"}


# ---------------------------------------------------------------------------
# Tests for create_glue_context
# ---------------------------------------------------------------------------


class TestCreateGlueContext:
    def test_returns_none_outside_glue(self):
        with patch("simpleetl.platforms.glue.is_aws_glue", return_value=False):
            result = create_glue_context()
            assert result is None

    def test_creates_context_inside_glue(self):
        mock_ctx = MagicMock(name="GlueContext")
        mock_sc = MagicMock(name="SparkContext")

        mock_glue_mod = MagicMock()
        mock_glue_mod.GlueContext.return_value = mock_ctx

        mock_spark_mod = MagicMock()
        mock_spark_mod.SparkContext.getOrCreate.return_value = mock_sc

        with patch("simpleetl.platforms.glue.is_aws_glue", return_value=True), \
             patch.dict(sys.modules, {"awsglue": MagicMock(), "awsglue.context": mock_glue_mod, "pyspark": mock_spark_mod, "pyspark.context": mock_spark_mod}):
            result = create_glue_context()
            assert result is mock_ctx
            mock_spark_mod.SparkContext.getOrCreate.assert_called_once()
            mock_glue_mod.GlueContext.assert_called_once_with(mock_sc)

    def test_import_error_raises_runtime_error(self):
        with patch("simpleetl.platforms.glue.is_aws_glue", return_value=True), \
             patch.dict(sys.modules, {"awsglue": None, "awsglue.context": None}):
            # Simulate ImportError by making the import fail
            with patch("simpleetl.platforms.glue.is_aws_glue", return_value=True):
                import builtins
                real_import = builtins.__import__

                def fake_import(name, *args, **kwargs):
                    if "awsglue" in name:
                        raise ImportError("No module named 'awsglue'")
                    return real_import(name, *args, **kwargs)

                with patch("builtins.__import__", side_effect=fake_import):
                    with pytest.raises(RuntimeError, match="awsglue is not available"):
                        create_glue_context()


# ---------------------------------------------------------------------------
# Tests for create_spark_session
# ---------------------------------------------------------------------------


class TestCreateSparkSession:
    def test_returns_spark_session_from_context(self, mock_glue_context):
        result = create_spark_session(mock_glue_context)
        assert result is mock_glue_context.spark_session

    def test_none_context_raises(self):
        with pytest.raises(ValueError, match="None GlueContext"):
            create_spark_session(None)


# ---------------------------------------------------------------------------
# Tests for read_from_catalog
# ---------------------------------------------------------------------------


class TestReadFromCatalog:
    def test_basic_read(self, mock_glue_context):
        frame = read_from_catalog(
            glue_context=mock_glue_context,
            database="my_db",
            table_name="my_table",
        )
        assert frame is not None
        mock_glue_context.create_dynamic_frame.from_catalog.assert_called_once()

    def test_with_push_down_predicate(self, mock_glue_context):
        read_from_catalog(
            glue_context=mock_glue_context,
            database="my_db",
            table_name="my_table",
            push_down_predicate="year=2024",
        )
        call_kwargs = (
            mock_glue_context.create_dynamic_frame.from_catalog.call_args
        )
        assert call_kwargs.kwargs["pushDownPredicate"] == "year=2024"

    def test_with_additional_options(self, mock_glue_context):
        read_from_catalog(
            glue_context=mock_glue_context,
            database="my_db",
            table_name="my_table",
            additional_options={"groupFiles": "inPartition"},
        )
        call_kwargs = (
            mock_glue_context.create_dynamic_frame.from_catalog.call_args
        )
        assert call_kwargs.kwargs["groupFiles"] == "inPartition"

    def test_none_context_raises(self):
        with pytest.raises(ValueError, match="glue_context must not be None"):
            read_from_catalog(
                glue_context=None,
                database="my_db",
                table_name="my_table",
            )

    def test_custom_transformation_ctx(self, mock_glue_context):
        read_from_catalog(
            glue_context=mock_glue_context,
            database="my_db",
            table_name="my_table",
            transformation_ctx="custom_ctx",
        )
        call_kwargs = (
            mock_glue_context.create_dynamic_frame.from_catalog.call_args
        )
        assert call_kwargs.kwargs["transformation_ctx"] == "custom_ctx"


# ---------------------------------------------------------------------------
# Tests for write_to_catalog
# ---------------------------------------------------------------------------


class TestWriteToCatalog:
    def test_basic_write(self, mock_glue_context):
        mock_frame = MagicMock(name="DynamicFrame")
        write_to_catalog(
            frame=mock_frame,
            glue_context=mock_glue_context,
            database="my_db",
            table_name="my_table",
        )
        mock_glue_context.write_dynamic_frame.from_catalog.assert_called_once()

    def test_write_with_format(self, mock_glue_context):
        mock_frame = MagicMock(name="DynamicFrame")
        write_to_catalog(
            frame=mock_frame,
            glue_context=mock_glue_context,
            database="my_db",
            table_name="my_table",
            format="json",
        )
        call_kwargs = (
            mock_glue_context.write_dynamic_frame.from_catalog.call_args
        )
        assert call_kwargs.kwargs["format"] == "json"

    def test_none_context_raises(self):
        mock_frame = MagicMock(name="DynamicFrame")
        with pytest.raises(ValueError, match="glue_context must not be None"):
            write_to_catalog(
                frame=mock_frame,
                glue_context=None,
                database="my_db",
                table_name="my_table",
            )

    def test_none_frame_raises(self, mock_glue_context):
        with pytest.raises(ValueError, match="frame must not be None"):
            write_to_catalog(
                frame=None,
                glue_context=mock_glue_context,
                database="my_db",
                table_name="my_table",
            )

    def test_additional_options(self, mock_glue_context):
        mock_frame = MagicMock(name="DynamicFrame")
        write_to_catalog(
            frame=mock_frame,
            glue_context=mock_glue_context,
            database="my_db",
            table_name="my_table",
            additional_options={"compression": "gzip"},
        )
        call_kwargs = (
            mock_glue_context.write_dynamic_frame.from_catalog.call_args
        )
        assert call_kwargs.kwargs["compression"] == "gzip"


# ---------------------------------------------------------------------------
# Tests for GlueContextManager
# ---------------------------------------------------------------------------


class TestGlueContextManager:
    def test_initial_state(self):
        mgr = GlueContextManager()
        assert mgr._glue_context is None
        assert mgr._spark_session is None
        assert mgr._glue_client is None

    def test_reset(self, mock_glue_context, mock_spark_session):
        mgr = GlueContextManager()
        mgr._glue_context = mock_glue_context
        mgr._spark_session = mock_spark_session
        mgr._glue_client = MagicMock()
        mgr.reset()
        assert mgr._glue_context is None
        assert mgr._spark_session is None
        assert mgr._glue_client is None

    def test_glue_context_lazy_creation(self):
        mgr = GlueContextManager()
        mock_ctx = MagicMock(name="GlueContext")
        with patch("simpleetl.platforms.glue.create_glue_context", return_value=mock_ctx):
            result = mgr.glue_context
            assert result is mock_ctx

    def test_glue_context_caching(self):
        mgr = GlueContextManager()
        mock_ctx = MagicMock(name="GlueContext")
        mgr._glue_context = mock_ctx
        # Should return cached value without calling create
        with patch("simpleetl.platforms.glue.create_glue_context") as mock_create:
            result = mgr.glue_context
            assert result is mock_ctx
            mock_create.assert_not_called()

    def test_spark_session_lazy_creation(self, mock_glue_context):
        mgr = GlueContextManager()
        mock_spark = MagicMock(name="SparkSession")
        mock_glue_context.spark_session = mock_spark
        with patch("simpleetl.platforms.glue.create_glue_context", return_value=mock_glue_context):
            result = mgr.spark_session
            assert result is mock_spark

    def test_glue_client_lazy_creation(self):
        mgr = GlueContextManager()
        mock_client = MagicMock(name="GlueClient")
        with patch("boto3.client", return_value=mock_client) as mock_boto:
            result = mgr.glue_client
            assert result is mock_client
            mock_boto.assert_called_once_with("glue")


# ---------------------------------------------------------------------------
# Tests for GluePlatformRunner
# ---------------------------------------------------------------------------


class TestGluePlatformRunner:
    def test_in_glue_environment(self):
        job = DummyJob()
        runner = GluePlatformRunner()

        with patch("simpleetl.platforms.glue.is_aws_glue", return_value=True), \
             patch.object(runner, "_run_in_glue") as mock_glue_run:
            runner.run_job(job)
            mock_glue_run.assert_called_once_with(job)

    def test_outside_glue_environment(self):
        job = DummyJob()
        runner = GluePlatformRunner()

        with patch("simpleetl.platforms.glue.is_aws_glue", return_value=False), \
             patch.object(runner, "_run_locally") as mock_local_run:
            runner.run_job(job)
            mock_local_run.assert_called_once_with(job)

    def test_run_in_glue_initializes_context(self):
        job = DummyJob()
        runner = GluePlatformRunner()
        runner.job_args = {"JOB_NAME": "test-job"}

        with patch.object(runner, "_init_glue") as mock_init, \
             patch.object(runner, "_set_job_bookmark") as mock_set, \
             patch.object(runner, "_update_job_bookmark") as mock_update, \
             patch.object(job, "run_with_error_handling") as mock_run:
            runner._run_in_glue(job)
            mock_init.assert_called_once()
            mock_set.assert_called_once()
            mock_run.assert_called_once()
            mock_update.assert_called_once()

    def test_run_in_glue_parses_args_when_empty(self):
        job = DummyJob()
        runner = GluePlatformRunner()
        runner.job_args = {}

        with patch("simpleetl.platforms.glue.get_job_args", return_value={"JOB_NAME": "j"}), \
             patch.object(runner, "_init_glue"), \
             patch.object(runner, "_set_job_bookmark"), \
             patch.object(runner, "_update_job_bookmark"), \
             patch.object(job, "run_with_error_handling"):
            runner._run_in_glue(job)
            assert runner.job_args == {"JOB_NAME": "j"}

    def test_run_locally_warns(self, caplog):
        job = DummyJob()
        runner = GluePlatformRunner()

        with patch.object(job, "run_with_error_handling"):
            runner._run_locally(job)
            assert "Not running in AWS Glue environment" in caplog.text

    def test_bookmarks_disabled(self):
        runner = GluePlatformRunner(enable_bookmarks=False)
        runner.job_args = {"JOB_NAME": "test-job"}
        # Should be no-ops
        runner._set_job_bookmark()
        runner._update_job_bookmark()

    def test_read_catalog_convenience(self, mock_glue_context):
        runner = GluePlatformRunner()
        runner.context_manager._glue_context = mock_glue_context

        frame = runner.read_catalog("my_db", "my_table")
        assert frame is not None
        mock_glue_context.create_dynamic_frame.from_catalog.assert_called_once()

    def test_write_catalog_convenience(self, mock_glue_context):
        runner = GluePlatformRunner()
        runner.context_manager._glue_context = mock_glue_context
        mock_frame = MagicMock(name="DynamicFrame")

        runner.write_catalog(mock_frame, "my_db", "my_table", format="json")
        mock_glue_context.write_dynamic_frame.from_catalog.assert_called_once()

    def test_dynamic_frame_to_pandas(self, mock_glue_context):
        runner = GluePlatformRunner()
        runner.context_manager._glue_context = mock_glue_context

        mock_spark_df = MagicMock(name="SparkDF")
        mock_pandas_df = MagicMock(name="pandas")
        mock_glue_context.spark_session = MagicMock()
        mock_frame = MagicMock(name="DynamicFrame")
        mock_frame.toDF.return_value = mock_spark_df
        mock_spark_df.toPandas.return_value = mock_pandas_df

        result = runner.dynamic_frame_to_pandas(mock_frame)
        assert result is mock_pandas_df

    def test_pandas_to_dynamic_frame(self, mock_glue_context):
        runner = GluePlatformRunner()
        runner.context_manager._glue_context = mock_glue_context

        mock_df = MagicMock(name="pandas")
        mock_dynamic = MagicMock(name="DynamicFrame")
        mock_glue_context.createDataFrameFromPandas.return_value = mock_dynamic

        result = runner.pandas_to_dynamic_frame(mock_df, "test_df")
        assert result is mock_dynamic
        mock_glue_context.createDataFrameFromPandas.assert_called_once_with(
            mock_df, "test_df"
        )


# ---------------------------------------------------------------------------
# Tests for GlueCatalogReader
# ---------------------------------------------------------------------------


class TestGlueCatalogReader:
    def test_read(self, mock_glue_context):
        reader = GlueCatalogReader(glue_context=mock_glue_context)
        frame = reader.read("my_db", "my_table")
        assert frame is not None
        mock_glue_context.create_dynamic_frame.from_catalog.assert_called_once()

    def test_read_with_push_down_predicate(self, mock_glue_context):
        reader = GlueCatalogReader(glue_context=mock_glue_context)
        reader.read("my_db", "my_table", push_down_predicate="year=2024")
        call_kwargs = (
            mock_glue_context.create_dynamic_frame.from_catalog.call_args
        )
        assert call_kwargs.kwargs["pushDownPredicate"] == "year=2024"

    def test_read_empty_database_raises(self, mock_glue_context):
        reader = GlueCatalogReader(glue_context=mock_glue_context)
        with pytest.raises(ValueError, match="database must not be empty"):
            reader.read("", "my_table")

    def test_read_empty_table_raises(self, mock_glue_context):
        reader = GlueCatalogReader(glue_context=mock_glue_context)
        with pytest.raises(ValueError, match="table_name must not be empty"):
            reader.read("my_db", "")

    def test_read_as_pandas(self, mock_glue_context):
        reader = GlueCatalogReader(glue_context=mock_glue_context)

        mock_spark_df = MagicMock(name="SparkDF")
        mock_pandas_df = MagicMock(name="pandas")
        mock_frame = mock_glue_context.create_dynamic_frame.from_catalog.return_value
        mock_frame.toDF.return_value = mock_spark_df
        mock_spark_df.toPandas.return_value = mock_pandas_df

        result = reader.read_as_pandas("my_db", "my_table")
        assert result is mock_pandas_df

    def test_lazy_context_creation(self):
        reader = GlueCatalogReader()
        mock_ctx = MagicMock(name="GlueContext")
        mock_frame = MagicMock(name="DynamicFrame")
        mock_frame.count.return_value = 0
        mock_ctx.create_dynamic_frame.from_catalog.return_value = mock_frame

        with patch("simpleetl.platforms.glue.create_glue_context", return_value=mock_ctx):
            reader.read("my_db", "my_table")
            assert reader._glue_context is mock_ctx


# ---------------------------------------------------------------------------
# Tests for GlueCatalogWriter
# ---------------------------------------------------------------------------


class TestGlueCatalogWriter:
    def test_write(self, mock_glue_context):
        writer = GlueCatalogWriter(glue_context=mock_glue_context)
        mock_frame = MagicMock(name="DynamicFrame")
        writer.write(mock_frame, "my_db", "my_table")
        mock_glue_context.write_dynamic_frame.from_catalog.assert_called_once()

    def test_write_with_format(self, mock_glue_context):
        writer = GlueCatalogWriter(glue_context=mock_glue_context)
        mock_frame = MagicMock(name="DynamicFrame")
        writer.write(mock_frame, "my_db", "my_table", format="json")
        call_kwargs = (
            mock_glue_context.write_dynamic_frame.from_catalog.call_args
        )
        assert call_kwargs.kwargs["format"] == "json"

    def test_write_empty_database_raises(self, mock_glue_context):
        writer = GlueCatalogWriter(glue_context=mock_glue_context)
        mock_frame = MagicMock(name="DynamicFrame")
        with pytest.raises(ValueError, match="database must not be empty"):
            writer.write(mock_frame, "", "my_table")

    def test_write_empty_table_raises(self, mock_glue_context):
        writer = GlueCatalogWriter(glue_context=mock_glue_context)
        mock_frame = MagicMock(name="DynamicFrame")
        with pytest.raises(ValueError, match="table_name must not be empty"):
            writer.write(mock_frame, "my_db", "")

    def test_write_unsupported_format_raises(self, mock_glue_context):
        writer = GlueCatalogWriter(glue_context=mock_glue_context)
        mock_frame = MagicMock(name="DynamicFrame")
        with pytest.raises(ValueError, match="Unsupported format"):
            writer.write(mock_frame, "my_db", "my_table", format="xml")

    def test_supported_formats_constant(self):
        expected = {"parquet", "json", "csv", "orc", "avro", "glueparquet"}
        assert SUPPORTED_FORMATS == expected

    def test_write_from_pandas(self, mock_glue_context):
        writer = GlueCatalogWriter(glue_context=mock_glue_context)
        mock_pandas = MagicMock(name="pandas")
        mock_dynamic = MagicMock(name="DynamicFrame")
        mock_glue_context.createDataFrameFromPandas.return_value = mock_dynamic

        writer.write_from_pandas(mock_pandas, "my_db", "my_table")
        mock_glue_context.createDataFrameFromPandas.assert_called_once_with(
            mock_pandas, "my_table"
        )
        mock_glue_context.write_dynamic_frame.from_catalog.assert_called_once()

    def test_write_from_pandas_no_context_raises(self):
        writer = GlueCatalogWriter(glue_context=None)
        mock_pandas = MagicMock(name="pandas")

        with patch("simpleetl.platforms.glue.create_glue_context", return_value=None):
            with pytest.raises(RuntimeError, match="GlueContext is not available"):
                writer.write_from_pandas(mock_pandas, "my_db", "my_table")

    def test_lazy_context_creation(self):
        writer = GlueCatalogWriter()
        mock_ctx = MagicMock(name="GlueContext")
        mock_frame = MagicMock(name="DynamicFrame")

        with patch("simpleetl.platforms.glue.create_glue_context", return_value=mock_ctx):
            writer.write(mock_frame, "my_db", "my_table")
            assert writer._glue_context is mock_ctx


# ---------------------------------------------------------------------------
# Integration-style tests
# ---------------------------------------------------------------------------


class TestGlueIntegration:
    """Test end-to-end Glue platform runner with catalog read/write."""

    def test_full_glue_job_flow(self):
        """Simulate a complete Glue job: read catalog -> transform -> write."""
        DummyJob()
        runner = GluePlatformRunner(enable_bookmarks=True)
        runner.job_args = {"JOB_NAME": "integration-test"}

        mock_ctx = MagicMock(name="GlueContext")
        mock_frame = MagicMock(name="DynamicFrame")
        mock_frame.count.return_value = 100
        mock_ctx.create_dynamic_frame.from_catalog.return_value = mock_frame
        mock_ctx.spark_session = MagicMock(name="SparkSession")

        runner.context_manager._glue_context = mock_ctx

        # Read from catalog
        frame = runner.read_catalog("source_db", "source_table")
        assert frame.count() == 100

        # Write to catalog
        runner.write_catalog(
            frame, "target_db", "target_table", format="parquet"
        )
        mock_ctx.write_dynamic_frame.from_catalog.assert_called_once()

    def test_glue_runner_with_custom_job_args(self):
        """Test that pre-parsed job args are preserved."""
        custom_args = {"JOB_NAME": "custom", "input": "s3://bucket/in"}
        runner = GluePlatformRunner(job_args=custom_args)
        assert runner.job_args == custom_args

    def test_glue_runner_bookmark_lifecycle(self):
        """Test bookmark set/update cycle."""
        runner = GluePlatformRunner(enable_bookmarks=True)
        runner.job_args = {"JOB_NAME": "bookmark-test"}

        with patch("simpleetl.platforms.glue.logger") as mock_logger:
            runner._set_job_bookmark()
            runner._update_job_bookmark()
            # Both calls should log info messages
            assert mock_logger.info.call_count == 2
