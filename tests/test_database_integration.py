"""
Integration tests with real PostgreSQL and MySQL databases.

Requires services defined in docker-compose.yml:
  - postgres: localhost:5432 (db=etl_test, user=etl_user, password=etl_password)
  - mysql: localhost:3306    (db=etl_test, user=etl_user, password=etl_password)

Tests skip gracefully when drivers or services are unavailable.
"""

import pandas as pd
import pytest

# -------------------------------------------------------------------
# Connection helpers
# -------------------------------------------------------------------

POSTGRES_URL = "postgresql://etl_user:etl_password@localhost:5432/etl_test"
MYSQL_URL = "mysql://etl_user:etl_password@localhost:3306/etl_test"


def _postgres_available():
    try:
        import sqlalchemy

        engine = sqlalchemy.create_engine(POSTGRES_URL, connect_timeout=2)
        with engine.connect() as conn:
            result = conn.execute(sqlalchemy.text("SELECT 1"))
            return result.scalar() == 1
    except Exception:
        return False


def _mysql_available():
    try:
        import sqlalchemy

        engine = sqlalchemy.create_engine(MYSQL_URL, connect_timeout=2)
        with engine.connect() as conn:
            result = conn.execute(sqlalchemy.text("SELECT 1"))
            return result.scalar() == 1
    except Exception:
        return False


# -------------------------------------------------------------------
# PostgreSQL integration
# -------------------------------------------------------------------


pytestmark_postgres = pytest.mark.skipif(
    not _postgres_available(),
    reason="Real PostgreSQL service not available at localhost:5432",
)


@pytest.mark.skipif(
    not pytest.importorskip("psycopg2", reason="psycopg2 not installed"),
)
class TestPostgreSQLIntegration:
    """Real PostgreSQL integration tests."""

    @pytest.mark.skipif(
        not _postgres_available(),
        reason="PostgreSQL not reachable",
    )
    def test_postgres_read_write_roundtrip(self):
        from simpleetl.formats.database import DatabaseReader, DatabaseWriter
        import sqlalchemy

        engine = sqlalchemy.create_engine(POSTGRES_URL)
        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
            }
        )
        writer = DatabaseWriter()
        writer.write(df, engine, table_name="pg_integration_test", if_exists="replace")

        reader = DatabaseReader()
        result = reader.read(
            engine, sql="SELECT * FROM pg_integration_test ORDER BY id"
        )
        assert len(result) == 3
        assert set(result.columns) == {"id", "name", "age"}
        assert result.iloc[0]["name"] == "Alice"

        # Cleanup
        with engine.begin() as conn:
            conn.execute(sqlalchemy.text("DROP TABLE IF EXISTS pg_integration_test"))

    @pytest.mark.skipif(
        not _postgres_available(),
        reason="PostgreSQL not reachable",
    )
    def test_postgres_merge_upsert(self):
        from simpleetl.formats.database import DatabaseWriter
        import sqlalchemy

        engine = sqlalchemy.create_engine(POSTGRES_URL)
        # Create table with primary key
        with engine.begin() as conn:
            conn.execute(
                sqlalchemy.text(
                    "CREATE TABLE IF NOT EXISTS pg_merge_test (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)"
                )
            )
            conn.execute(sqlalchemy.text("DELETE FROM pg_merge_test"))
            conn.execute(
                sqlalchemy.text(
                    "INSERT INTO pg_merge_test (id, name, score) VALUES (1, 'Alice', 10), (2, 'Bob', 20)"
                )
            )

        df_update = pd.DataFrame(
            {
                "id": [1, 3],
                "name": ["Alice_Updated", "Charlie"],
                "score": [15, 30],
            }
        )
        writer = DatabaseWriter()
        rows = writer.merge(
            df_update,
            engine,
            table_name="pg_merge_test",
            key_columns=["id"],
            update_columns=["name", "score"],
        )
        assert isinstance(rows, int)

        result = pd.read_sql("SELECT * FROM pg_merge_test ORDER BY id", engine)
        assert len(result) == 3
        # Verify upsert changed Alice's score
        row_alice = result[result["id"] == 1].iloc[0]
        assert row_alice["name"] == "Alice_Updated"
        assert row_alice["score"] == 15

        # Cleanup
        with engine.begin() as conn:
            conn.execute(sqlalchemy.text("DROP TABLE IF EXISTS pg_merge_test"))

    @pytest.mark.skipif(
        not _postgres_available(),
        reason="PostgreSQL not reachable",
    )
    def test_postgres_chunked_read(self):
        from simpleetl.formats.database import DatabaseReader
        import sqlalchemy

        engine = sqlalchemy.create_engine(POSTGRES_URL)
        with engine.begin() as conn:
            conn.execute(
                sqlalchemy.text(
                    "CREATE TABLE IF NOT EXISTS pg_chunked_test (id INTEGER)"
                )
            )
            conn.execute(sqlalchemy.text("DELETE FROM pg_chunked_test"))
            conn.execute(
                sqlalchemy.text(
                    "INSERT INTO pg_chunked_test (id) SELECT generate_series(1, 10)"
                )
            )

        reader = DatabaseReader()
        chunks = list(
            reader.read_chunks(
                engine,
                sql="SELECT * FROM pg_chunked_test ORDER BY id",
                chunk_size=3,
            )
        )
        total = sum(len(c) for c in chunks)
        assert total == 10
        assert len(chunks) >= 3

        with engine.begin() as conn:
            conn.execute(sqlalchemy.text("DROP TABLE IF EXISTS pg_chunked_test"))


# -------------------------------------------------------------------
# MySQL integration
# -------------------------------------------------------------------


pytest.importorskip("mysql.connector", reason="mysql-connector-python not installed")


@pytest.mark.skipif(
    not _mysql_available(),
    reason="MySQL service not available at localhost:3306",
)
class TestMySQLIntegration:
    """Real MySQL integration tests."""

    def test_mysql_read_write_roundtrip(self):
        from simpleetl.formats.database import DatabaseReader, DatabaseWriter
        import sqlalchemy

        engine = sqlalchemy.create_engine(MYSQL_URL)
        df = pd.DataFrame(
            {
                "id": [1, 2],
                "name": ["Alice", "Bob"],
            }
        )
        writer = DatabaseWriter()
        writer.write(
            df, engine, table_name="mysql_integration_test", if_exists="replace"
        )

        reader = DatabaseReader()
        result = reader.read(
            engine, sql="SELECT * FROM mysql_integration_test ORDER BY id"
        )
        assert len(result) == 2
        assert set(result.columns) == {"id", "name"}

        # Cleanup
        with engine.begin() as conn:
            conn.execute(sqlalchemy.text("DROP TABLE IF EXISTS mysql_integration_test"))

    def test_mysql_merge_upsert(self):
        from simpleetl.formats.database import DatabaseWriter
        import sqlalchemy

        engine = sqlalchemy.create_engine(MYSQL_URL)
        with engine.begin() as conn:
            conn.execute(
                sqlalchemy.text(
                    "CREATE TABLE IF NOT EXISTS mysql_merge_test (id INTEGER PRIMARY KEY, name VARCHAR(255), score INTEGER)"
                )
            )
            conn.execute(sqlalchemy.text("DELETE FROM mysql_merge_test"))
            conn.execute(
                sqlalchemy.text(
                    "INSERT INTO mysql_merge_test (id, name, score) VALUES (1, 'Alice', 10)"
                )
            )

        df_update = pd.DataFrame(
            {
                "id": [1, 2],
                "name": ["Alice_Updated", "Bob"],
                "score": [99, 20],
            }
        )
        writer = DatabaseWriter()
        rows = writer.merge(
            df_update,
            engine,
            table_name="mysql_merge_test",
            key_columns=["id"],
            update_columns=["name", "score"],
        )
        assert isinstance(rows, int)

        result = pd.read_sql("SELECT * FROM mysql_merge_test ORDER BY id", engine)
        assert len(result) == 2
        row_alice = result[result["id"] == 1].iloc[0]
        assert row_alice["name"] == "Alice_Updated"
        assert row_alice["score"] == 99

        # Cleanup
        with engine.begin() as conn:
            conn.execute(sqlalchemy.text("DROP TABLE IF EXISTS mysql_merge_test"))
