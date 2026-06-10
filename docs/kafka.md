# Kafka Source & Sink

SimpleETL v1.3 adds Apache Kafka support via
[confluent-kafka](https://github.com/confluentinc/confluent-kafka-python):
consume topics into DataFrames and produce DataFrame rows as JSON
messages.

## Installation

```bash
pip install simpleetl[kafka]
```

## Source String Format

```
kafka://host:port[,host2:port2,...]/topic
```

Examples: `kafka://localhost:9092/orders`,
`kafka://broker1:9092,broker2:9093/events`. The format factory routes
these automatically, so they work directly as `input_path`/`output_path`
in job configs.

## Reading

```python
from simpleetl import KafkaReader

reader = KafkaReader(
    bootstrap_servers="localhost:9092",
    topic="orders",
    group_id="etl-orders",
    max_messages=5000,      # stop after N messages...
    timeout=30.0,           # ...or after this many seconds
)
df = reader.read()
```

- JSON message values are flattened into columns; non-dict JSON lands in
  a `value` column; undecodable messages are skipped with a warning.
- `value_format="raw"` keeps values as text instead of JSON-decoding.
- Metadata columns are attached to every row: `_kafka_topic`,
  `_kafka_partition`, `_kafka_offset`, `_kafka_timestamp` (ms),
  `_kafka_key`.
- Offsets are committed synchronously after a successful read
  (`commit=False` disables this); auto-commit is always off.
- Continuous consumption: `read_chunks(chunk_size)` yields one DataFrame
  per batch and commits per batch.

## Writing

```python
from simpleetl import KafkaWriter

writer = KafkaWriter(
    bootstrap_servers="localhost:9092",
    topic="orders-out",
    key_column="order_id",   # used as message key, kept in the payload
)
writer.write(df)
```

Each row becomes one JSON message. The writer flushes on completion and
raises `RuntimeError` when messages fail delivery or remain unsent after
`flush_timeout`.

## Extra Client Configuration

Pass any confluent-kafka setting through `consumer_config` /
`producer_config`:

```python
KafkaReader(..., consumer_config={"security.protocol": "SASL_SSL"})
KafkaWriter(..., producer_config={"compression.type": "zstd"})
```

## Notes

- Unit tests in SimpleETL are fully mocked; run your own broker-backed
  integration tests for production topologies.
- Exactly-once semantics are not provided: the reader is at-least-once
  (commit after read), the writer is at-least-once (delivery callbacks +
  flush).
