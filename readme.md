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

QWP delivery is at least once. A reconnect, a rejection, a byte-triggered split,
or a multi-table server failure can replay rows that QuestDB already committed.
Before rejection recovery the connector removes every acknowledged checkpoint it
can identify. A byte-triggered client flush or a multi-frame connector flush can
still leave an acknowledged prefix inside the rejected checkpoint, and that
prefix is replayed. Configure `DEDUP UPSERT KEYS` on target tables whenever
duplicate rows are not acceptable.

The connector keeps Kafka as the durable log, so QWP store-and-forward is
memory-only: `sf_dir` and `sf_durability` are rejected. `max.inflight.rows` is
a soft pause threshold, and the current poll batch may overshoot it.
`sf_max_total_bytes` caps the client's encoded store-and-forward segments; it
does not include Kafka Connect's current poll batch. The connector itself retains
only per-flush offset checkpoints, not `SinkRecord` payloads.
Keep `sf_append_deadline_millis` below the worker consumer's
`max.poll.interval.ms` (or set `consumer.override.max.poll.interval.ms` so the
connector can validate the relationship).

Shutdown is deliberately brief. Kafka Connect allows all tasks on a worker a
combined `task.shutdown.graceful.timeout.ms` (5s by default) and cannot
interrupt a task that overruns it. The closing `preCommit` publishes pending
rows and waits up to `qwp.commit.ack.timeout.ms` (500ms by default) before it
selects the offsets to commit. A later acknowledgement during `Sender.close()`
cannot change that selection, so the connector defaults the client's
`close_flush_timeout_millis` to `0`. Sender close runs on a daemon thread and
the task waits at most one second for it; a wedged native close finishes in the
background instead of overrunning Connect's worker-wide shutdown budget.
Unacknowledged offsets are withheld and Kafka redelivers those records, so
duplicates remain possible. An explicit client setting is preserved. Partition revocation does not drain at all:
offsets for the revoked partitions were already decided by the preceding
`preCommit`, so waiting would only stall the rebalance for the whole consumer
group.

Delivery is validated by a chaos integration test (containers killed mid-stream
while 5M records flow, asserting an exact deduplicated row count), a
multi-worker test proving each worker resolves its own transport, and tests
pinning the server behaviour the design relies on.

Offsets are committed only after the corresponding QWP checkpoint is acknowledged.
For QWP, `auto_flush_rows` and `auto_flush_interval` define connector checkpoint
boundaries; the client-side row trigger is disabled and the time trigger is pushed
out to the largest supported interval so they cannot publish an untracked frame
immediately before a checkpoint. The client byte trigger remains enabled and
capped by the server-advertised batch size to protect wide batches.

By default, only deterministic `SCHEMA_MISMATCH` terminal errors are eligible
for quarantine and the configured Kafka Connect DLQ. Quarantine re-fetches the
unacknowledged window from Kafka, then delivers poll batches synchronously and
bisects rejected batches until it identifies the bad record. A quarantine chunk
never exceeds `auto_flush_rows`; consequently `dlq.send.batch.on.error=true`
reports at most that checkpoint-sized chunk rather than the whole poll batch.
Its per-chunk wait is bounded by `qwp.quarantine.ack.timeout.ms` (1s by default), so recovery on a
high-latency link is intentionally slower than normal pipelined delivery. Other
terminal, security, and protocol failures fail the task. Transport and local
store-and-forward capacity failures retire the sender and rewind the affected
partitions; `progress.timeout.ms` is the bound on a persistent outage. Advanced
users can explicitly extend `qwp.dlq.terminal.categories`; doing so can blame
valid records for server or client faults and is not recommended.

Client-side row validation on a healthy sender is record-local and follows the
normal Kafka Connect DLQ policy. Before classifying a plain client exception,
the connector probes the sender's failure latch: a latched server rejection
goes through quarantine, while a latched connection failure retires and rewinds
the sender. DLQ handling requires both a reporter and `errors.tolerance=all`.
As usual, pin drifting schemaless numeric fields with `doubles` or a schema so
QuestDB does not depend on which type arrives first.

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
  QWP it follows the configured record-error/DLQ policy; the standard path
  substitutes `value`.
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
