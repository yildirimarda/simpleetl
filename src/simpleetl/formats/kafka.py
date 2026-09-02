"""
Apache Kafka source and sink for SimpleETL.

Requires the ``confluent-kafka`` optional dependency::

    pip install simpleetl[kafka]
    # or
    pip install confluent-kafka

Sources and destinations are addressed either with explicit
``bootstrap_servers``/``topic`` parameters or with a single URI string
usable in configs::

    kafka://host:port/topic
    kafka://host1:9092,host2:9092/topic

The reader consumes JSON messages into a DataFrame and adds these
metadata columns to every record:

- ``_kafka_topic``: topic the message was read from.
- ``_kafka_partition``: partition number.
- ``_kafka_offset``: message offset within the partition.
- ``_kafka_timestamp``: broker/producer timestamp in milliseconds, or
  *None* when the broker provides no timestamp.
- ``_kafka_key``: message key decoded as UTF-8, or *None*.
"""

import json as _json
import logging
import time
from typing import Any, Dict, Iterator, List, Optional, Tuple

import pandas as pd

from .base import DataReader, DataWriter

logger = logging.getLogger(__name__)

#: URI scheme prefix recognised by the factory and by ``parse_kafka_uri``.
URI_PREFIX = "kafka://"

_VALID_VALUE_FORMATS = {"json", "raw"}

#: Upper bound for a single ``Consumer.poll`` call, in seconds.  The
#: overall ``timeout`` budget is enforced across multiple polls.
_POLL_INTERVAL = 1.0


def _require_confluent_kafka() -> None:
    try:
        import confluent_kafka  # noqa: F401
    except ImportError:
        raise ImportError(
            "confluent-kafka is required for Kafka format support. "
            "Install it with: pip install simpleetl[kafka]"
        )


def parse_kafka_uri(uri: str) -> Tuple[str, str]:
    """Parse a ``kafka://`` URI into ``(bootstrap_servers, topic)``.

    Accepted format: ``kafka://host:port[,host2:port2,...]/topic``.

    Args:
        uri: Kafka URI string.

    Returns:
        Tuple of comma-separated bootstrap servers and the topic name.

    Raises:
        ValueError: If the URI does not start with ``kafka://`` or is
            missing the broker list or the topic.
    """
    if not uri.startswith(URI_PREFIX):
        raise ValueError(f"Not a Kafka URI (expected '{URI_PREFIX}' prefix): '{uri}'.")
    remainder = uri[len(URI_PREFIX) :]
    servers, sep, topic = remainder.partition("/")
    if not servers:
        raise ValueError(
            f"Kafka URI is missing the broker list: '{uri}'. Expected "
            "format: 'kafka://host:port[,host2:port2]/topic'."
        )
    if not sep or not topic:
        raise ValueError(
            f"Kafka URI is missing a topic: '{uri}'. Expected format: "
            "'kafka://host:port[,host2:port2]/topic'."
        )
    return servers, topic


def _resolve_target(
    source: Optional[str],
    default_servers: Optional[str],
    default_topic: Optional[str],
) -> Tuple[str, str]:
    """Resolve a call into ``(bootstrap_servers, topic)``.

    *source* may be a ``kafka://`` URI, a bare topic name, or *None*
    (constructor defaults are used for whatever is missing).
    """
    servers = default_servers
    topic = default_topic
    if source:
        if source.startswith(URI_PREFIX):
            servers, topic = parse_kafka_uri(source)
        else:
            topic = source
    if not servers:
        raise ValueError(
            "No Kafka bootstrap servers specified. Pass "
            "bootstrap_servers='host:port' or use a "
            "'kafka://host:port/topic' source string."
        )
    if not topic:
        raise ValueError(
            "No Kafka topic specified. Pass topic='name' or use a "
            "'kafka://host:port/topic' source string."
        )
    return servers, topic


def _is_missing(value: Any) -> bool:
    """True for missing scalar values (None, NaN, NaT, pd.NA)."""
    return value is None or (pd.api.types.is_scalar(value) and bool(pd.isna(value)))


def _decode_key(raw: Any) -> Any:
    """Decode a message key to text, leaving non-bytes values as-is."""
    if isinstance(raw, bytes):
        return raw.decode("utf-8", errors="replace")
    return raw


def _decode_value(raw: Any, value_format: str) -> Optional[Dict[str, Any]]:
    """Decode a message value into a record dict.

    Returns *None* when the message cannot be decoded (invalid UTF-8 or
    invalid JSON in ``"json"`` mode, including tombstone/*None* values)
    so the caller can skip and count it.
    """
    if value_format == "raw":
        if isinstance(raw, bytes):
            return {"value": raw.decode("utf-8", errors="replace")}
        return {"value": raw}
    if raw is None:
        return None
    try:
        text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
        parsed = _json.loads(text)
    except (UnicodeDecodeError, ValueError):
        return None
    if isinstance(parsed, dict):
        return dict(parsed)
    return {"value": parsed}


class KafkaReader(DataReader):
    """Consume messages from a Kafka topic into a pandas DataFrame.

    Each consumed message becomes one DataFrame row.  JSON object values
    are flattened into columns; non-dict JSON values (numbers, strings,
    lists) land in a ``value`` column.  Undecodable messages are skipped
    with a warning and counted.  The metadata columns ``_kafka_topic``,
    ``_kafka_partition``, ``_kafka_offset``, ``_kafka_timestamp`` and
    ``_kafka_key`` are added to every record (see module docstring).

    Args:
        bootstrap_servers: Comma-separated broker list
            (``"host1:9092,host2:9092"``).  May also be supplied via a
            ``kafka://`` source string on each call.
        topic: Topic to consume.  May also be supplied via the source
            string (URI form or bare topic name).
        group_id: Consumer group id (default: ``"simpleetl"``).
        max_messages: Maximum number of messages to consume per call
            (default: 1000).  Skipped/undecodable messages count toward
            this bound.
        timeout: Overall poll budget in seconds per call (default: 10.0).
            Reading stops when the budget is exhausted, even if fewer
            than *max_messages* messages arrived.
        auto_offset_reset: Where to start when the group has no committed
            offset — ``"earliest"`` (default) or ``"latest"``.
        value_format: ``"json"`` (default) decodes message values as
            JSON; ``"raw"`` keeps the value as text in a ``value``
            column.
        consumer_config: Extra confluent-kafka consumer settings, merged
            over the generated configuration.
        commit: Commit offsets after a successful read (default: True).
            Offsets are committed synchronously, and only when at least
            one record was decoded.  Auto-commit is always disabled.

    Example::

        reader = KafkaReader("localhost:9092", "orders", max_messages=500)
        df = reader.read()

        # Single-string config form
        df = KafkaReader().read("kafka://localhost:9092/orders")
    """

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        topic: Optional[str] = None,
        *,
        group_id: str = "simpleetl",
        max_messages: int = 1000,
        timeout: float = 10.0,
        auto_offset_reset: str = "earliest",
        value_format: str = "json",
        consumer_config: Optional[Dict[str, Any]] = None,
        commit: bool = True,
    ) -> None:
        if value_format not in _VALID_VALUE_FORMATS:
            raise ValueError(
                f"Invalid value_format '{value_format}'. "
                f"Must be one of {sorted(_VALID_VALUE_FORMATS)}."
            )
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.max_messages = max_messages
        self.timeout = timeout
        self.auto_offset_reset = auto_offset_reset
        self.value_format = value_format
        self.consumer_config: Dict[str, Any] = consumer_config or {}
        self.commit = commit

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _create_consumer(self, bootstrap_servers: str, topic: str) -> Any:
        """Build a confluent-kafka Consumer subscribed to *topic*."""
        _require_confluent_kafka()
        from confluent_kafka import Consumer  # noqa: PLC0415

        config: Dict[str, Any] = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": self.auto_offset_reset,
            "enable.auto.commit": False,
        }
        config.update(self.consumer_config)
        consumer = Consumer(config)
        consumer.subscribe([topic])
        return consumer

    def _consume(
        self,
        consumer: Any,
        max_messages: int,
        timeout: float,
        value_format: str,
    ) -> Iterator[Dict[str, Any]]:
        """Yield one record dict per successfully decoded message.

        Stops after *max_messages* polled messages or once the *timeout*
        budget is exhausted, whichever comes first.  Partition-EOF
        events are ignored; other consumer errors raise KafkaException.
        """
        from confluent_kafka import (  # noqa: PLC0415
            TIMESTAMP_NOT_AVAILABLE,
            KafkaError,
            KafkaException,
        )

        deadline = time.monotonic() + timeout
        consumed = 0
        skipped = 0
        while consumed < max_messages:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            message = consumer.poll(min(remaining, _POLL_INTERVAL))
            if message is None:
                continue
            error = message.error()
            if error is not None:
                if error.code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(error)
            consumed += 1
            record = _decode_value(message.value(), value_format)
            if record is None:
                skipped += 1
                logger.warning(
                    "Skipping undecodable Kafka message at %s[%s] offset %s",
                    message.topic(),
                    message.partition(),
                    message.offset(),
                )
                continue
            ts_type, ts_value = message.timestamp()
            record["_kafka_topic"] = message.topic()
            record["_kafka_partition"] = message.partition()
            record["_kafka_offset"] = message.offset()
            record["_kafka_timestamp"] = (
                None if ts_type == TIMESTAMP_NOT_AVAILABLE else ts_value
            )
            record["_kafka_key"] = _decode_key(message.key())
            yield record
        if skipped:
            logger.warning("Skipped %d undecodable Kafka message(s) in total", skipped)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def read(
        self,
        source: Optional[str] = None,
        *,
        topic: Optional[str] = None,
        max_messages: Optional[int] = None,
        timeout: Optional[float] = None,
        value_format: Optional[str] = None,
        commit: Optional[bool] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """Consume messages from a topic into a single DataFrame.

        Args:
            source: ``kafka://host:port/topic`` URI, a bare topic name,
                or *None* (constructor values are used).
            topic: Topic override (takes precedence over *source*).
            max_messages: Per-call override of the message bound.
            timeout: Per-call override of the poll budget (seconds).
            value_format: Per-call override — ``"json"`` or ``"raw"``.
            commit: Per-call override of offset committing.

        Returns:
            DataFrame with one row per decoded message, including the
            ``_kafka_*`` metadata columns.  Empty (no columns) when no
            message arrived within the budget.

        Raises:
            ValueError: If brokers/topic cannot be resolved or
                *value_format* is invalid.
            ImportError: If ``confluent-kafka`` is not installed.
        """
        effective_max = self.max_messages if max_messages is None else max_messages
        frames = list(
            self.read_chunks(
                source,
                chunk_size=effective_max,
                topic=topic,
                max_messages=effective_max,
                timeout=timeout,
                value_format=value_format,
                commit=commit,
            )
        )
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def read_chunks(
        self,
        source: Optional[str] = None,
        chunk_size: int = 1000,
        max_buffer_mb: float = 0,
        *,
        topic: Optional[str] = None,
        max_messages: Optional[int] = None,
        timeout: Optional[float] = None,
        value_format: Optional[str] = None,
        commit: Optional[bool] = None,
        **kwargs: Any,
    ) -> Iterator[pd.DataFrame]:
        """Consume a topic in batches, yielding one DataFrame per batch.

        Accumulates up to *chunk_size* decoded records per chunk and
        keeps polling until *max_messages* messages were consumed or the
        *timeout* budget is exhausted.  When committing is enabled,
        offsets are committed synchronously after each batch is read
        from the broker (before the chunk is yielded).  The consumer is
        always closed, even on error.

        Args:
            source: ``kafka://host:port/topic`` URI, a bare topic name,
                or *None* (constructor values are used).
            chunk_size: Maximum number of records per yielded DataFrame.
            topic: Topic override (takes precedence over *source*).
            max_messages: Per-call override of the total message bound.
            timeout: Per-call override of the poll budget (seconds).
            value_format: Per-call override — ``"json"`` or ``"raw"``.
            commit: Per-call override of offset committing.

        Yields:
            DataFrame batches of at most *chunk_size* rows.

        Raises:
            ValueError: If brokers/topic cannot be resolved or
                *value_format* is invalid.
            ImportError: If ``confluent-kafka`` is not installed.
        """
        servers, topic_name = _resolve_target(
            source, self.bootstrap_servers, topic or self.topic
        )
        effective_max = self.max_messages if max_messages is None else max_messages
        effective_timeout = self.timeout if timeout is None else timeout
        effective_format = value_format or self.value_format
        effective_commit = self.commit if commit is None else commit
        if effective_format not in _VALID_VALUE_FORMATS:
            raise ValueError(
                f"Invalid value_format '{effective_format}'. "
                f"Must be one of {sorted(_VALID_VALUE_FORMATS)}."
            )

        consumer = self._create_consumer(servers, topic_name)
        try:
            batch: List[Dict[str, Any]] = []
            for record in self._consume(
                consumer, effective_max, effective_timeout, effective_format
            ):
                batch.append(record)
                if len(batch) >= chunk_size:
                    if effective_commit:
                        consumer.commit(asynchronous=False)
                    yield pd.DataFrame(batch)
                    batch = []
            if batch:
                if effective_commit:
                    consumer.commit(asynchronous=False)
                yield pd.DataFrame(batch)
        finally:
            consumer.close()


class KafkaWriter(DataWriter):
    """Produce DataFrame rows as JSON messages to a Kafka topic.

    Each row is serialised with ``json.dumps(..., default=str)`` so
    non-JSON-native values (timestamps, decimals) become strings.  When
    *key_column* is set, that column's value is used as the message key
    (stringified); the column is NOT dropped — it stays in the JSON
    payload as well.

    Args:
        bootstrap_servers: Comma-separated broker list.  May also be
            supplied via a ``kafka://`` destination string on each call.
        topic: Topic to produce to.  May also be supplied via the
            destination string (URI form or bare topic name).
        key_column: Column whose value becomes the message key.  Rows
            with a missing key value (None/NaN) are sent without a key.
        producer_config: Extra confluent-kafka producer settings, merged
            over the generated configuration.
        flush_timeout: Seconds to wait for outstanding deliveries on
            flush (default: 30.0).

    Example::

        writer = KafkaWriter("localhost:9092", "orders", key_column="id")
        writer.write(df)

        # Single-string config form
        KafkaWriter().write(df, "kafka://localhost:9092/orders")
    """

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        topic: Optional[str] = None,
        *,
        key_column: Optional[str] = None,
        producer_config: Optional[Dict[str, Any]] = None,
        flush_timeout: float = 30.0,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.key_column = key_column
        self.producer_config: Dict[str, Any] = producer_config or {}
        self.flush_timeout = flush_timeout

    def write(
        self,
        data: pd.DataFrame,
        destination: Optional[str] = None,
        *,
        topic: Optional[str] = None,
        key_column: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """Produce every row of *data* as a JSON message.

        ``Producer.poll(0)`` is called after each produce to serve
        delivery callbacks, and the producer is flushed at the end.

        Args:
            data: DataFrame whose rows are serialised as JSON objects.
            destination: ``kafka://host:port/topic`` URI, a bare topic
                name, or *None* (constructor values are used).
            topic: Topic override (takes precedence over *destination*).
            key_column: Per-call override of the key column.

        Raises:
            ValueError: If brokers/topic cannot be resolved or
                *key_column* is not a column of *data*.
            RuntimeError: If the flush times out with messages still
                undelivered, or any message failed delivery.
            ImportError: If ``confluent-kafka`` is not installed.
        """
        servers, topic_name = _resolve_target(
            destination, self.bootstrap_servers, topic or self.topic
        )
        effective_key_column = key_column or self.key_column
        if (
            effective_key_column is not None
            and effective_key_column not in data.columns
        ):
            raise ValueError(
                f"key_column '{effective_key_column}' is not a column of "
                f"the DataFrame. Available columns: {list(data.columns)}."
            )

        _require_confluent_kafka()
        from confluent_kafka import Producer  # noqa: PLC0415

        config: Dict[str, Any] = {"bootstrap.servers": servers}
        config.update(self.producer_config)
        producer = Producer(config)

        delivery_errors: List[str] = []

        def _on_delivery(error: Any, message: Any) -> None:
            if error is not None:
                delivery_errors.append(str(error))

        records = data.to_dict(orient="records")
        for record in records:
            key: Optional[str] = None
            if effective_key_column is not None:
                key_value = record[effective_key_column]
                if not _is_missing(key_value):
                    key = str(key_value)
            producer.produce(
                topic_name,
                value=_json.dumps(record, default=str),
                key=key,
                on_delivery=_on_delivery,
            )
            producer.poll(0)

        pending = producer.flush(self.flush_timeout)
        if pending > 0:
            raise RuntimeError(
                f"Kafka flush timed out after {self.flush_timeout}s with "
                f"{pending} message(s) still undelivered to topic "
                f"'{topic_name}'."
            )
        if delivery_errors:
            raise RuntimeError(
                f"{len(delivery_errors)} Kafka message(s) failed delivery "
                f"to topic '{topic_name}': {delivery_errors[0]}"
            )


__all__ = ["KafkaReader", "KafkaWriter", "parse_kafka_uri"]
