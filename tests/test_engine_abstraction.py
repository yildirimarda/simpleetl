"""
Tests for the deep engine abstraction (polars engine integration).
"""

import simpleetl
from simpleetl.core.config import ETLJobConfig
from simpleetl.core.engine import VALID_ENGINES, validate_engine


def test_engine_abstraction_exports():
    """Engine abstraction symbols are exported from the package."""
    assert simpleetl.validate_engine is validate_engine
    assert simpleetl.VALID_ENGINES == VALID_ENGINES
    assert "pandas" in simpleetl.VALID_ENGINES
    assert "polars" in simpleetl.VALID_ENGINES


def test_validate_engine_valid():
    assert validate_engine("pandas") == "pandas"
    assert validate_engine("polars") == "polars"


def test_validate_engine_invalid():
    import pytest

    with pytest.raises(ValueError, match="Unknown engine"):
        validate_engine("spark")


def test_etl_config_engine_default():
    config = ETLJobConfig(name="test", input_format="csv", output_format="parquet")
    assert config.engine == "pandas"


def test_etl_config_engine_polars():
    config = ETLJobConfig(
        name="test",
        input_format="csv",
        output_format="parquet",
        engine="polars",
    )
    assert config.engine == "polars"


def test_get_format_options_merges_engine():
    from simpleetl.core.job import ETLJob

    class DummyJob(ETLJob):
        def run(self):
            pass

    job = DummyJob(
        {
            "name": "test",
            "input_format": "csv",
            "output_format": "parquet",
            "engine": "polars",
        }
    )
    opts = job.get_format_options()
    assert opts.get("engine") == "polars"
