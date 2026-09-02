"""Tests for GlueCatalogHook and config-driven catalog registration."""

from unittest.mock import MagicMock, patch


from simpleetl.core.config import ETLJobConfig
from simpleetl.core.hooks import POST_LOAD
from simpleetl.core.job import ETLJob
from simpleetl.formats.glue_catalog import GlueCatalogHook


class DummyJob(ETLJob):
    def run(self) -> None:
        pass


class TestGlueCatalogHook:
    def test_hook_registered_when_config_flag_enabled(self):
        config = ETLJobConfig(
            name="catalog-test",
            input_format="csv",
            output_format="csv",
            register_glue_catalog=True,
            params={
                "glue_catalog": {
                    "database": "my_db",
                    "table_name": "my_table",
                    "format": "json",
                }
            },
        )
        job = DummyJob(config)
        hooks = job._config_hooks.get(POST_LOAD, [])
        assert len(hooks) == 1
        assert isinstance(hooks[0], GlueCatalogHook)
        assert hooks[0].database == "my_db"
        assert hooks[0].table_name == "my_table"
        assert hooks[0].format == "json"

    def test_hook_not_registered_when_flag_disabled(self):
        config = ETLJobConfig(
            name="catalog-test",
            input_format="csv",
            output_format="csv",
            register_glue_catalog=False,
        )
        job = DummyJob(config)
        assert (
            POST_LOAD not in job._config_hooks
            or len(job._config_hooks.get(POST_LOAD, [])) == 0
        )

    def test_hook_skips_when_data_is_none(self):
        hook = GlueCatalogHook(database="db", table_name="t", format="parquet")
        ctx = MagicMock()
        ctx.phase = POST_LOAD
        ctx.data = None
        hook.execute(ctx)  # Must not raise

    def test_hook_writes_dynamic_frame(self):
        hook = GlueCatalogHook(database="db", table_name="t", format="parquet")
        mock_frame = MagicMock(name="DynamicFrame")
        mock_frame.count.return_value = 10

        ctx = MagicMock()
        ctx.phase = POST_LOAD
        ctx.data = mock_frame

        with patch("simpleetl.formats.glue_catalog.GlueCatalogWriter") as MockWriter:
            mock_writer = MagicMock()
            MockWriter.return_value = mock_writer
            hook.execute(ctx)
            mock_writer.write.assert_called_once_with(
                frame=mock_frame,
                database="db",
                table_name="t",
                format="parquet",
            )
