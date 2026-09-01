# QuestDB Sink connector for Apache Kafka
The connector reads data from Kafka topics and writes to [QuestDB](https://questdb.io/) tables.
The connector implements Apache Kafka [Sink Connector API](https://kafka.apache.org/documentation/#connect_development).

## Documentation
Documentation is maintained on [QuestDB.com](https://questdb.com/docs/third-party-tools/kafka/#questdb-kafka-connect-connector) 

## QWP transport

The QuestDB WebSocket Protocol transport requires Kafka Connect 3.6 or newer,
`questdb-client` 1.3.8, and QuestDB 10 or newer. Select it with a `ws::` or
`wss::` client configuration string, for example:

```properties
client.conf.string=ws::addr=questdb:9000;sf_max_total_bytes=268435456;
```

QWP delivery is at least once. A reconnect, a split frame, or a multi-table
server failure can replay rows that QuestDB already committed. Configure
`DEDUP UPSERT KEYS` on target tables whenever duplicate rows are not acceptable.

The connector keeps Kafka as the durable log, so QWP store-and-forward is
memory-only: `sf_dir` and `sf_durability` are rejected. `max.inflight.rows` is
a soft pause threshold, and the current poll batch may overshoot it.
`sf_max_total_bytes` caps the client's encoded store-and-forward segments; it
does not bound the heap retained by the connector's `SinkRecord` payloads.
Keep `sf_append_deadline_millis` below the worker consumer's
`max.poll.interval.ms` (or set `consumer.override.max.poll.interval.ms` so the
connector can validate the relationship).

Shutdown is deliberately brief. Kafka Connect allows all tasks on a worker a
combined `task.shutdown.graceful.timeout.ms` (5s by default) and cannot
interrupt a task that overruns it. The closing `preCommit` publishes pending
rows and waits up to `qwp.commit.ack.timeout.ms` (500ms by default) before it
selects the offsets to commit. A later acknowledgement during `Sender.close()`
cannot change that selection, so the connector defaults the client's
`close_flush_timeout_millis` to `0`: close still releases its resources but
does not wait for acknowledgements again. Unacknowledged offsets are withheld
and Kafka redelivers those records, so duplicates remain possible. An explicit
client setting is preserved. Partition revocation does not drain at all:
offsets for the revoked partitions were already decided by the preceding
`preCommit`, so waiting would only stall the rebalance for the whole consumer
group.

Delivery is validated by a chaos integration test (containers killed mid-stream
while 5M records flow, asserting an exact deduplicated row count), a
multi-worker test proving each worker resolves its own transport, and tests
pinning the server behaviour the design relies on.

Offsets are committed only after the corresponding QWP frame is acknowledged.
By default, only deterministic `SCHEMA_MISMATCH` terminal errors are eligible
for record isolation and the configured Kafka Connect DLQ. Other terminal,
security, protocol, capacity, and transport failures fail the task. Advanced
users can explicitly extend `qwp.dlq.terminal.categories`; doing so can blame
valid records for server or client faults and is not recommended.

Client-side row validation has a QWP limitation: the WebSocket sender uses the
same plain `LineSenderException` for some invalid row arguments and for
connection, buffer-recycle, and store-and-forward capacity failures observed
from row calls. The connector therefore cannot safely send a record to the DLQ
solely because row construction threw that exception; it fails the task
instead. Mapping errors represented as `InvalidDataException` remain
record-DLQ eligible, and typed `LineSenderServerException` rejections continue
through the terminal-category policy above.

The most common way to meet that limitation is schemaless JSON whose field
types drift between records - `{"v":1}` followed by `{"v":1.5}`. The WebSocket
sender remembers the type it sent for each column for the lifetime of the
connection, so it raises the mismatch itself, before the row reaches QuestDB,
and the task fails. The `http::` transport does not keep that state: the server
rejects the row instead, which is a typed rejection and can be sent to the DLQ.
Pin the column type to avoid it - declare the field in `doubles`, or publish
with a schema - which is worth doing regardless, since a column's type in
QuestDB is otherwise decided by whichever record happens to arrive first.

## Raw JSON fast path (experimental)

This option is newer than the transport above and has no production mileage
yet, so it is still marked experimental even though it is covered by a
differential test suite that pins it against the standard path.

Kafka Connect converts every record before the connector sees it, and for JSON
that costs two throwaway object graphs per record: `JsonConverter` builds a
Jackson tree and then converts it into a map of boxed values. In profiling of
this connector the converter accounted for ~58% of the sink task's CPU and
~84% of everything it allocated.

`value.format=json` skips that. Hand the connector the raw bytes and it parses
them once, straight into rows:

```properties
value.converter=org.apache.kafka.connect.converters.ByteArrayConverter
value.format=json
```

If the producer writes the envelope `JsonConverter` emits with
`schemas.enable=true` (`{"schema": {...}, "payload": {...}}`), use
`value.format=json_envelope` instead: the schema is ignored and the payload
becomes the row. Types are inferred from the JSON either way. The mode is
never guessed from the data: pick the one matching your producer, because
sending enveloped records with plain `value.format=json` flattens the envelope
into `schema_*` and `payload_*` columns.

Measured on a single task with 5M records, interleaved A/B against the same
pipeline using `JsonConverter`: 709k -> 1,048k rows/s, or **+48%**.

The fast path honours `table`, `symbols`, `doubles`, `timestamp.field.name`,
`timestamp.units`, `timestamp.string.fields`, `include.key`, `key.prefix`,
`value.prefix` and `skip.unsupported.types`, flattens nested objects with `_`,
and supports 1D/2D/3D numeric arrays with the same jagged-array, null-element
and element-type rules as the standard path. A differential test feeds a
payload corpus through both paths and asserts they emit the same columns and
values, and reject the same payloads.

Limitations and differences:

- Transformations that inspect or modify the payload cannot be used: with
  `ByteArrayConverter` an SMT sees opaque bytes. Topic-level SMTs such as
  `RegexRouter` are unaffected.
- Duplicate field names in one JSON object are the one behavioural difference:
  the converter's map keeps the last value, while the fast path writes the
  column twice and QuestDB keeps the first. JSON names should be unique
  (RFC 8259), and detecting duplicates would cost a lookup per field, so this
  is documented rather than prevented.
- Composed timestamps (multiple `timestamp.string.fields`) are not supported
  and are rejected at startup.
- On the `tcp::` transport a malformed payload fails the task even when a dead
  letter queue is configured, and it does so on every restart. The parse error
  surfaces part-way through building the row, and TCP cannot discard a partial
  row, so continuing would emit a malformed line. `JsonConverter` does not have
  this problem because it fails before the connector sees the record, in a
  stage Kafka Connect can route to the DLQ itself. Use `http::` or `ws::` if
  you need dead letter queue support with this option.
- Only the value is parsed by the connector. The key still goes through the
  key converter, and only JSON is supported (`value.format=json` or
  `json_envelope`).
- The schema in an envelope is ignored: column types come from the JSON
  values, so a schema declaring INT8 or FLOAT32 still yields QuestDB LONG or
  DOUBLE. Use `doubles` when an integer-looking field must be a double.
- Objects nested inside arrays are not valid array elements, exactly as on the
  standard path: they fail, or are skipped with `skip.unsupported.types=true`.
- JSON nested deeper than 64 levels is rejected as invalid data. Without the
  limit such a payload overflows the stack, and Kafka Connect cannot route an
  `Error` to the dead letter queue, so the record would kill the task on every
  restart.
- Top-level values that are not JSON objects (`123`, `"text"`, `[1,2,3]`) are
  rejected. The standard path writes them into a `value` column.
- A field with an empty name (`{"": 1}`) produces an illegal column name. On
  QWP this fails the task; the standard path substitutes `value`.
- Naming an object- or array-valued field in `symbols` flattens or writes it as
  an array instead of stringifying it, so the auto-created schema differs from
  the standard path.
- Integers larger than `Long.MAX_VALUE` are written as doubles. The standard
  path silently overflows them into a wrapped negative long, so the two paths
  differ here on purpose.
- Column order follows the JSON document rather than the converter's map
  iteration order, which changes the column order of auto-created tables.

## Sample Projects
This repository contains a number of [sample projects.](kafka-questdb-connector-samples) showing how to use the connector. It also demonstrates how to use the connector together with Debezium for Change Data Capture.

## Distribution
Releases are published on GitHub: https://github.com/questdb/kafka-questdb-connector/releases/
It's also available in [Confluent Hub](https://www.confluent.io/hub/questdb/kafka-questdb-connector).

## Issues
If you encounter any issues, please [create an issue](https://github.com/questdb/kafka-questdb-connector/issues/new) in this repository.

## License
This project is licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.
