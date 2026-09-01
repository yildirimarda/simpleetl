"""Broker-backed integration tests for Kafka source/sink.

Requires a real Kafka broker (default localhost:9092). Tests skip
gracefully when the broker is unreachable, matching the database
integration test pattern.
"""

import time

import pandas as pd
import pytest

from simpleetl.formats.kafka import KafkaReader, KafkaWriter

# -------------------------------------------------------------------
# Broker availability
# -------------------------------------------------------------------

_KAFKA_BOOTSTRAP = "localhost:9092"


def _broker_available() -> bool:
    try:
        import confluent_kafka
    except ImportError:
        return False
    try:
        conf = {
            "bootstrap.servers": _KAFKA_BOOTSTRAP,
            "group.id": "simpleetl-integration-check",
            "session.timeout.ms": 3000,
        }
        admin = confluent_kafka.Consumer(conf)
        # list_topics triggers a broker connection
        admin.list_topics(timeout=2)
        admin.close()
        return True
    except Exception:
        return False


# -------------------------------------------------------------------
# Skip markers
# -------------------------------------------------------------------

pytest.importorskip("confluent_kafka", reason="confluent-kafka not installed")

pytestmark_broker = pytest.mark.skipif(
    not _broker_available(),
    reason=f"Real Kafka broker not available at {_KAFKA_BOOTSTRAP}",
)


# -------------------------------------------------------------------
# Integration tests
# -------------------------------------------------------------------


@pytest.mark.skipif(
    not _broker_available(),
    reason=f"Real Kafka broker not available at {_KAFKA_BOOTSTRAP}",
)
class TestKafkaBrokerIntegration:
    """Real broker-backed read/write tests."""

    def test_write_and_read_roundtrip(self):
        topic = "simpleetl_integration_test"
        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
            }
        )
        writer = KafkaWriter(
            bootstrap_servers=_KAFKA_BOOTSTRAP,
            topic=topic,
            flush_timeout=10.0,
        )
        writer.write(df)

        # Give the broker a moment to make the messages visible
        time.sleep(0.5)

        reader = KafkaReader(
            bootstrap_servers=_KAFKA_BOOTSTRAP,
            topic=topic,
            group_id="simpleetl-integration-reader",
            max_messages=3,
            timeout=5.0,
            auto_offset_reset="earliest",
            commit=False,
        )
        result = reader.read()

        assert len(result) == 3
        assert "_kafka_topic" in result.columns
        assert "_kafka_offset" in result.columns
        assert list(result["id"]) == [1, 2, 3]

    def test_read_chunks_from_broker(self):
        topic = "simpleetl_integration_chunks"
        df = pd.DataFrame(
            {
                "value": ["a", "b", "c", "d", "e"],
            }
        )
        writer = KafkaWriter(
            bootstrap_servers=_KAFKA_BOOTSTRAP,
            topic=topic,
            flush_timeout=10.0,
        )
        writer.write(df)
        time.sleep(0.5)

        reader = KafkaReader(
            bootstrap_servers=_KAFKA_BOOTSTRAP,
            topic=topic,
            group_id="simpleetl-integration-chunks-reader",
            max_messages=5,
            timeout=5.0,
            auto_offset_reset="earliest",
            commit=False,
        )
        chunks = list(reader.read_chunks(chunk_size=2))
        total_rows = sum(len(chunk) for chunk in chunks)
        assert total_rows == 5
        assert all(isinstance(chunk, pd.DataFrame) for chunk in chunks)

    def test_key_column_used_on_broker(self):
        topic = "simpleetl_integration_key"
        df = pd.DataFrame(
            {
                "order_id": [42, 99],
                "name": ["X", "Y"],
            }
        )
        writer = KafkaWriter(
            bootstrap_servers=_KAFKA_BOOTSTRAP,
            topic=topic,
            key_column="order_id",
            flush_timeout=10.0,
        )
        writer.write(df)
        time.sleep(0.5)

        reader = KafkaReader(
            bootstrap_servers=_KAFKA_BOOTSTRAP,
            topic=topic,
            group_id="simpleetl-integration-key-reader",
            max_messages=2,
            timeout=5.0,
            auto_offset_reset="earliest",
            commit=False,
        )
        result = reader.read()
        assert len(result) == 2
        assert "_kafka_key" in result.columns
        # Key values are stringified by the writer
        assert result["_kafka_key"].iloc[0] == "42.0"

    def test_read_empty_topic_returns_empty_frame(self):
        # Use a non-existent topic with a very short timeout
        reader = KafkaReader(
            bootstrap_servers=_KAFKA_BOOTSTRAP,
            topic="simpleetl_nonexistent_topic_12345",
            group_id="simpleetl-empty-reader",
            max_messages=10,
            timeout=0.5,
            auto_offset_reset="earliest",
            commit=False,
        )
        df = reader.read()
        assert df.empty
