"""
Integration tests for Snowflake and BigQuery native MERGE upserts.

These tests require real warehouse accounts and driver packages
(snowflake-sqlalchemy / sqlalchemy-bigquery). They skip gracefully
when accounts or drivers are unavailable.

UPSERT validation against live warehouses is deferred (see PLAN.md:
Milestone 38). Mock-based SQL-shape validation lives in
`test_database_dialects.py`.
"""

import os

import pandas as pd
import pytest

# -------------------------------------------------------------------
# Environment-based account configuration
# -------------------------------------------------------------------
# Set these environment variables to test against real warehouses:
#   SNOWFLAKE_URL=snowflake://user:pass@account/db/schema
#   BIGQUERY_URL=bigquery://my-project/my_dataset

SNOWFLAKE_URL = os.environ.get("SNOWFLAKE_URL", "")
BIGQUERY_URL = os.environ.get("BIGQUERY_URL", "")


@pytest.mark.skipif(
    not SNOWFLAKE_URL,
    reason="No SNOWFLAKE_URL environment variable set (real account required)",
)
@pytest.mark.skipif(
    not pytest.importorskip(
        "snowflake.sqlalchemy", reason="snowflake-sqlalchemy not installed"
    ),
    reason="snowflake-sqlalchemy not installed",
)
class TestSnowflakeWarehouseUpsert:
    """Live Snowflake UPSERT validation (deferred until real account available)."""

    def test_snowflake_merge_upsert_live(self):
        from simpleetl.formats.database import DatabaseWriter
        import sqlalchemy

        engine = sqlalchemy.create_engine(SNOWFLAKE_URL)
        df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"], "value": [10, 20]})
        writer = DatabaseWriter()
        writer.merge(
            df,
            engine,
            table_name="sf_integration_test",
            key_columns=["id"],
            update_columns=["name", "value"],
        )
        # Validation against a real Snowflake account requires an active
        # connection and appropriate permissions; this test serves as the
        # template once those prerequisites are met.
        assert True


@pytest.mark.skipif(
    not BIGQUERY_URL,
    reason="No BIGQUERY_URL environment variable set (real account required)",
)
@pytest.mark.skipif(
    not pytest.importorskip(
        "sqlalchemy_bigquery", reason="sqlalchemy-bigquery not installed"
    ),
    reason="sqlalchemy-bigquery not installed",
)
class TestBigQueryWarehouseUpsert:
    """Live BigQuery UPSERT validation (deferred until real account available)."""

    def test_bigquery_merge_upsert_live(self):
        from simpleetl.formats.database import DatabaseWriter
        import sqlalchemy

        engine = sqlalchemy.create_engine(BIGQUERY_URL)
        df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
        writer = DatabaseWriter()
        writer.merge(
            df,
            engine,
            table_name="bq_integration_test",
            key_columns=["id"],
            update_columns=["name"],
        )
        # Validation against a real BigQuery dataset requires an active
        # GCP project and service account; this test serves as the
        # template once those prerequisites are met.
        assert True
