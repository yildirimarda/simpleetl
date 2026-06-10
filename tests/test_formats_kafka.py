"""Tests for the Kafka source/sink (v1.3, PLAN 10.2).

Fully mocked: a fake ``confluent_kafka`` module is injected into
``sys.modules`` so no broker connection or socket is ever made.
"""

import json
import sys
import types
from typing import Any, Optional
from unittest.mock import MagicMock

import pandas as pd
import pytest

from simpleetl.formats.factory import FormatFactory
from simpleetl.formats.kafka import (
    KafkaReader,
    KafkaWriter,
    parse_kafka_uri,
)


# ---------------------------------------------------------------------------
# Fake confluent_kafka module
# ---------------------------------------------------------------------------


class FakeKafkaError:
    """Mimics confluent_kafka.KafkaError."""

    _PARTITION_EOF = -191

    def __init__(self, code: int = 1, reason: str = "boom") -> None:
        self._code = code
        self._reason = reason

    def code(self) -> int:
        return self._code

    def __str__(self) -> str:
        return self._reason


class FakeKafkaException(Exception):
    """Mimics confluent_kafka.KafkaException."""


class FakeMessage:
    """Mimics confluent_kafka.Message."""

    def __init__(
        self,
        value: Any = b"{}",
        *,
        key: Any = None,
        topic: str = "orders",
        partition: int = 0,
        offset: int = 0,
        timestamp: tuple = (1, 1700000000000),
        error: Optional[FakeKafkaError] = None,
    ) -> None:
        self._value = value
        self._key = key
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._timestamp = timestamp
        self._error = error

    def value(self) -> Any:
        return self._value

    def key(self) -> Any:
        return self._key

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def timestamp(self) -> tuple:
        return self._timestamp

    def error(self) -> Optional[FakeKafkaError]:
        return self._error


@pytest.fixture
def fake_kafka(monkeypatch):
    """Inject a fake confluent_kafka module into sys.modules."""
    module = types.ModuleType("confluent_kafka")
    module.Consumer = MagicMock(name="Consumer")  # type: ignore[attr-defined]
    module.Producer = MagicMock(name="Producer")  # type: ignore[attr-defined]
    module.KafkaError = FakeKafkaError  # type: ignore[attr-defined]
    module.KafkaException = FakeKafkaException  # type: ignore[attr-defined]
    module.TIMESTAMP_NOT_AVAILABLE = 0  # type: ignore[attr-defined]
    module.TIMESTAMP_CREATE_TIME = 1  # type: ignore[attr-defined]
    # Sensible defaults: no queued messages, clean flush.
    module.Consumer.return_value.poll.return_value = None
    module.Producer.return_value.flush.return_value = 0
    monkeypatch.setitem(sys.modules, "confluent_kafka", module)
    return module


def _queue_messages(fake_kafka, messages) -> MagicMock:
    """Make the consumer poll() return *messages* then None forever."""
    consumer = fake_kafka.Consumer.return_value
    queue = list(messages)

    def poll(timeout: float = 0.0):
        return queue.pop(0) if queue else None

    consumer.poll.side_effect = poll
    return consumer


# ---------------------------------------------------------------------------
# parse_kafka_uri
# ---------------------------------------------------------------------------


class TestParseKafkaUri:
    def test_single_host(self):
        servers, topic = parse_kafka_uri("kafka://localhost:9092/orders")
        assert servers == "localhost:9092"
        assert topic == "orders"

    def test_multiple_hosts(self):
        servers, topic = parse_kafka_uri("kafka://h1:9092,h2:9093/events")
        assert servers == "h1:9092,h2:9093"
        assert topic == "events"

    def test_missing_topic_raises(self):
        with pytest.raises(ValueError, match="missing a topic"):
            parse_kafka_uri("kafka://localhost:9092")

    def test_empty_topic_raises(self):
        with pytest.raises(ValueError, match="missing a topic"):
            parse_kafka_uri("kafka://localhost:9092/")

    def test_missing_servers_raises(self):
        with pytest.raises(ValueError, match="broker list"):
            parse_kafka_uri("kafka:///orders")

    def test_wrong_scheme_raises(self):
        with pytest.raises(ValueError, match="Not a Kafka URI"):
            parse_kafka_uri("http://localhost:9092/orders")


# ---------------------------------------------------------------------------
# KafkaReader — initialisation and target resolution
# ---------------------------------------------------------------------------


class TestKafkaReaderInit:
    def test_defaults(self):
        reader = KafkaReader("localhost:9092", "orders")
        assert reader.group_id == "simpleetl"
        assert reader.max_messages == 1000
        assert reader.timeout == 10.0
        assert reader.auto_offset_reset == "earliest"
        assert reader.value_format == "json"
        assert reader.commit is True

    def test_invalid_value_format_raises(self):
        with pytest.raises(ValueError, match="Invalid value_format"):
            KafkaReader("localhost:9092", "orders", value_format="xml")

    def test_invalid_value_format_override_raises(self, fake_kafka):
        reader = KafkaReader("localhost:9092", "orders")
        with pytest.raises(ValueError, match="Invalid value_format"):
            list(reader.read_chunks(value_format="avro"))

    def test_missing_servers_raises(self):
        with pytest.raises(ValueError, match="bootstrap servers"):
            KafkaReader(topic="orders").read()

    def test_missing_topic_raises(self):
        with pytest.raises(ValueError, match="topic"):
            KafkaReader(bootstrap_servers="localhost:9092").read()


# ---------------------------------------------------------------------------
# KafkaReader — read()
# ---------------------------------------------------------------------------


class TestKafkaReaderRead:
    def test_read_happy_path(self, fake_kafka):
        consumer = _queue_messages(
            fake_kafka,
            [
                FakeMessage(
                    b'{"id": 1, "name": "Alice"}',
                    key=b"k1",
                    offset=7,
                    timestamp=(1, 1700000000123),
                ),
                FakeMessage('{"id": 2, "name": "Bob"}', offset=8),
            ],
        )
        reader = KafkaReader("localhost:9092", "orders", max_messages=2)
        df = reader.read()

        assert len(df) == 2
        assert list(df["id"]) == [1, 2]
        for column in (
            "_kafka_topic",
            "_kafka_partition",
            "_kafka_offset",
            "_kafka_timestamp",
            "_kafka_key",
        ):
            assert column in df.columns
        assert df["_kafka_topic"].iloc[0] == "orders"
        assert df["_kafka_partition"].iloc[0] == 0
        assert df["_kafka_offset"].iloc[0] == 7
        assert df["_kafka_timestamp"].iloc[0] == 1700000000123
        assert df["_kafka_key"].iloc[0] == "k1"
        assert consumer.close.called

    def test_read_uri_source(self, fake_kafka):
        _queue_messages(fake_kafka, [FakeMessage(b'{"x": 1}')])
        df = KafkaReader(max_messages=1).read("kafka://h1:9092,h2:9093/events")

        config = fake_kafka.Consumer.call_args[0][0]
        assert config["bootstrap.servers"] == "h1:9092,h2:9093"
        assert config["group.id"] == "simpleetl"
        assert config["enable.auto.commit"] is False
        fake_kafka.Consumer.return_value.subscribe.assert_called_once_with(
            ["events"]
        )
        assert len(df) == 1

    def test_read_bare_topic_source(self, fake_kafka):
        _queue_messages(fake_kafka, [FakeMessage(b'{"x": 1}')])
        KafkaReader("localhost:9092", max_messages=1).read("clicks")
        fake_kafka.Consumer.return_value.subscribe.assert_called_once_with(
            ["clicks"]
        )

    def test_read_consumer_config_merged(self, fake_kafka):
        reader = KafkaReader(
            "localhost:9092",
            "orders",
            max_messages=1,
            timeout=0.05,
            consumer_config={"session.timeout.ms": 45000},
        )
        reader.read()
        config = fake_kafka.Consumer.call_args[0][0]
        assert config["session.timeout.ms"] == 45000

    def test_read_max_messages_bound(self, fake_kafka):
        consumer = _queue_messages(
            fake_kafka,
            [FakeMessage(json.dumps({"n": i}).encode()) for i in range(5)],
        )
        df = KafkaReader("localhost:9092", "orders", max_messages=3).read()
        assert len(df) == 3
        assert consumer.poll.call_count == 3

    def test_read_timeout_budget_exhausted(self, fake_kafka):
        consumer = fake_kafka.Consumer.return_value  # poll → None forever
        reader = KafkaReader("localhost:9092", "orders", timeout=0.05)
        df = reader.read()
        assert df.empty
        assert consumer.poll.called
        assert consumer.close.called

    def test_read_json_decode_errors_skipped_with_warning(
        self, fake_kafka, caplog
    ):
        _queue_messages(
            fake_kafka,
            [
                FakeMessage(b"not json at all", offset=1),
                FakeMessage(b"\xff\xfe\xfa", offset=2),  # invalid UTF-8
                FakeMessage(b'{"ok": true}', offset=3),
            ],
        )
        reader = KafkaReader("localhost:9092", "orders", max_messages=3)
        with caplog.at_level("WARNING"):
            df = reader.read()
        assert len(df) == 1
        assert bool(df["ok"].iloc[0]) is True
        assert "Skipping undecodable" in caplog.text
        assert "Skipped 2 undecodable" in caplog.text

    def test_read_tombstone_skipped_in_json_mode(self, fake_kafka, caplog):
        _queue_messages(
            fake_kafka,
            [FakeMessage(None), FakeMessage(b'{"a": 1}')],
        )
        reader = KafkaReader("localhost:9092", "orders", max_messages=2)
        with caplog.at_level("WARNING"):
            df = reader.read()
        assert len(df) == 1
        assert "Skipped 1 undecodable" in caplog.text

    def test_read_raw_value_format(self, fake_kafka):
        _queue_messages(
            fake_kafka,
            [FakeMessage(b"hello bytes"), FakeMessage(None)],
        )
        reader = KafkaReader(
            "localhost:9092", "orders", max_messages=2, value_format="raw"
        )
        df = reader.read()
        assert len(df) == 2
        assert df["value"].iloc[0] == "hello bytes"
        assert pd.isna(df["value"].iloc[1])
        assert "_kafka_offset" in df.columns

    def test_read_non_dict_json_lands_in_value_column(self, fake_kafka):
        _queue_messages(
            fake_kafka,
            [FakeMessage(b"[1, 2, 3]"), FakeMessage(b"42")],
        )
        df = KafkaReader("localhost:9092", "orders", max_messages=2).read()
        assert len(df) == 2
        assert df["value"].iloc[0] == [1, 2, 3]
        assert df["value"].iloc[1] == 42

    def test_read_timestamp_not_available_is_none(self, fake_kafka):
        _queue_messages(
            fake_kafka,
            [FakeMessage(b'{"a": 1}', timestamp=(0, -1))],
        )
        df = KafkaReader("localhost:9092", "orders", max_messages=1).read()
        assert pd.isna(df["_kafka_timestamp"].iloc[0])

    def test_read_commits_when_enabled(self, fake_kafka):
        consumer = _queue_messages(fake_kafka, [FakeMessage(b'{"a": 1}')])
        KafkaReader("localhost:9092", "orders", max_messages=1).read()
        consumer.commit.assert_called_once_with(asynchronous=False)

    def test_read_does_not_commit_when_disabled(self, fake_kafka):
        consumer = _queue_messages(fake_kafka, [FakeMessage(b'{"a": 1}')])
        KafkaReader(
            "localhost:9092", "orders", max_messages=1, commit=False
        ).read()
        assert not consumer.commit.called

    def test_read_does_not_commit_when_nothing_consumed(self, fake_kafka):
        consumer = fake_kafka.Consumer.return_value  # poll → None forever
        KafkaReader("localhost:9092", "orders", timeout=0.05).read()
        assert not consumer.commit.called

    def test_read_partition_eof_skipped(self, fake_kafka):
        eof = FakeMessage(error=FakeKafkaError(FakeKafkaError._PARTITION_EOF))
        _queue_messages(fake_kafka, [eof, FakeMessage(b'{"a": 1}')])
        df = KafkaReader("localhost:9092", "orders", max_messages=1).read()
        assert len(df) == 1

    def test_read_error_raises_and_closes_consumer(self, fake_kafka):
        broken = FakeMessage(error=FakeKafkaError(42, "broker down"))
        consumer = _queue_messages(fake_kafka, [broken])
        reader = KafkaReader("localhost:9092", "orders")
        with pytest.raises(FakeKafkaException):
            reader.read()
        assert consumer.close.called
        assert not consumer.commit.called


# ---------------------------------------------------------------------------
# KafkaReader — read_chunks()
# ---------------------------------------------------------------------------


class TestKafkaReaderReadChunks:
    def test_read_chunks_batching(self, fake_kafka):
        _queue_messages(
            fake_kafka,
            [FakeMessage(json.dumps({"n": i}).encode()) for i in range(5)],
        )
        reader = KafkaReader("localhost:9092", "orders", max_messages=5)
        chunks = list(reader.read_chunks(chunk_size=2))
        assert [len(chunk) for chunk in chunks] == [2, 2, 1]
        assert all(isinstance(chunk, pd.DataFrame) for chunk in chunks)

    def test_read_chunks_commits_per_batch(self, fake_kafka):
        consumer = _queue_messages(
            fake_kafka,
            [FakeMessage(json.dumps({"n": i}).encode()) for i in range(5)],
        )
        reader = KafkaReader("localhost:9092", "orders", max_messages=5)
        list(reader.read_chunks(chunk_size=2))
        assert consumer.commit.call_count == 3

    def test_read_chunks_no_commit_when_disabled(self, fake_kafka):
        consumer = _queue_messages(
            fake_kafka,
            [FakeMessage(json.dumps({"n": i}).encode()) for i in range(4)],
        )
        reader = KafkaReader(
            "localhost:9092", "orders", max_messages=4, commit=False
        )
        list(reader.read_chunks(chunk_size=2))
        assert not consumer.commit.called

    def test_read_chunks_closes_consumer(self, fake_kafka):
        consumer = _queue_messages(fake_kafka, [FakeMessage(b'{"a": 1}')])
        reader = KafkaReader("localhost:9092", "orders", max_messages=1)
        list(reader.read_chunks(chunk_size=10))
        assert consumer.close.called


# ---------------------------------------------------------------------------
# KafkaWriter
# ---------------------------------------------------------------------------


class TestKafkaWriter:
    def test_write_produces_one_message_per_row(self, fake_kafka):
        producer = fake_kafka.Producer.return_value
        df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        KafkaWriter("localhost:9092", "orders").write(df)

        assert producer.produce.call_count == 3
        first_call = producer.produce.call_args_list[0]
        assert first_call[0][0] == "orders"
        payload = json.loads(first_call[1]["value"])
        assert payload == {"id": 1, "name": "a"}
        assert first_call[1]["key"] is None
        producer.poll.assert_called_with(0)
        producer.flush.assert_called_once_with(30.0)
        config = fake_kafka.Producer.call_args[0][0]
        assert config["bootstrap.servers"] == "localhost:9092"

    def test_write_key_column_used_and_kept_in_payload(self, fake_kafka):
        producer = fake_kafka.Producer.return_value
        df = pd.DataFrame({"id": [7, None], "v": ["x", "y"]})
        KafkaWriter(
            "localhost:9092", "orders", key_column="id"
        ).write(df)

        calls = producer.produce.call_args_list
        assert calls[0][1]["key"] == "7.0"  # None upcasts the column to float
        assert "id" in json.loads(calls[0][1]["value"])  # not dropped
        assert calls[1][1]["key"] is None  # None key value → no key

    def test_write_missing_key_column_raises(self, fake_kafka):
        df = pd.DataFrame({"a": [1]})
        writer = KafkaWriter("localhost:9092", "orders", key_column="nope")
        with pytest.raises(ValueError, match="key_column 'nope'"):
            writer.write(df)
        assert not fake_kafka.Producer.called

    def test_write_uri_destination(self, fake_kafka):
        producer = fake_kafka.Producer.return_value
        df = pd.DataFrame({"a": [1]})
        KafkaWriter().write(df, "kafka://h1:9092,h2:9093/events")
        config = fake_kafka.Producer.call_args[0][0]
        assert config["bootstrap.servers"] == "h1:9092,h2:9093"
        assert producer.produce.call_args[0][0] == "events"

    def test_write_producer_config_merged(self, fake_kafka):
        df = pd.DataFrame({"a": [1]})
        KafkaWriter(
            "localhost:9092",
            "orders",
            producer_config={"linger.ms": 5},
        ).write(df)
        config = fake_kafka.Producer.call_args[0][0]
        assert config["linger.ms"] == 5

    def test_write_serialises_non_json_values_as_strings(self, fake_kafka):
        producer = fake_kafka.Producer.return_value
        df = pd.DataFrame({"ts": [pd.Timestamp("2026-01-02 03:04:05")]})
        KafkaWriter("localhost:9092", "orders").write(df)
        payload = json.loads(producer.produce.call_args[1]["value"])
        assert payload["ts"] == "2026-01-02 03:04:05"

    def test_write_delivery_error_raises(self, fake_kafka):
        producer = fake_kafka.Producer.return_value
        outcomes = iter([None, FakeKafkaError(5, "queue full")])

        def produce(topic, value=None, key=None, on_delivery=None):
            on_delivery(next(outcomes), None)

        producer.produce.side_effect = produce
        df = pd.DataFrame({"a": [1, 2]})
        writer = KafkaWriter("localhost:9092", "orders")
        with pytest.raises(RuntimeError, match="1 Kafka message.*queue full"):
            writer.write(df)

    def test_write_flush_timeout_raises(self, fake_kafka):
        fake_kafka.Producer.return_value.flush.return_value = 2
        df = pd.DataFrame({"a": [1, 2, 3]})
        writer = KafkaWriter("localhost:9092", "orders", flush_timeout=1.5)
        with pytest.raises(
            RuntimeError, match="2 message\\(s\\) still undelivered"
        ):
            writer.write(df)

    def test_write_empty_dataframe(self, fake_kafka):
        producer = fake_kafka.Producer.return_value
        df = pd.DataFrame({"a": pd.Series([], dtype="int64")})
        KafkaWriter("localhost:9092", "orders").write(df)
        assert not producer.produce.called
        assert producer.flush.called

    def test_write_missing_topic_raises(self, fake_kafka):
        with pytest.raises(ValueError, match="topic"):
            KafkaWriter("localhost:9092").write(pd.DataFrame({"a": [1]}))


# ---------------------------------------------------------------------------
# Missing optional dependency
# ---------------------------------------------------------------------------


class TestKafkaImportError:
    def test_reader_raises_clear_import_error(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "confluent_kafka", None)
        reader = KafkaReader("localhost:9092", "orders")
        with pytest.raises(ImportError, match=r"simpleetl\[kafka\]"):
            reader.read()

    def test_writer_raises_clear_import_error(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "confluent_kafka", None)
        writer = KafkaWriter("localhost:9092", "orders")
        with pytest.raises(ImportError, match=r"simpleetl\[kafka\]"):
            writer.write(pd.DataFrame({"a": [1]}))


# ---------------------------------------------------------------------------
# Factory routing
# ---------------------------------------------------------------------------


class TestFactoryKafkaRouting:
    def test_get_reader_routes_kafka_uri(self):
        reader = FormatFactory.get_reader("kafka://localhost:9092/orders")
        assert isinstance(reader, KafkaReader)

    def test_get_writer_routes_kafka_uri(self):
        writer = FormatFactory.get_writer("kafka://localhost:9092/orders")
        assert isinstance(writer, KafkaWriter)

    def test_detect_format(self):
        info = FormatFactory.detect_format("kafka://localhost:9092/orders")
        assert info["format"] == "kafka"
        assert info["mime_type"] == "application/json"

    def test_supported_formats_lists_kafka(self):
        formats = FormatFactory.supported_formats()
        assert formats["kafka"] == "kafka://"
