# QuestDB Sink connector for Apache Kafka
The connector reads data from Kafka topics and writes to [QuestDB](https://questdb.io/) tables.
The connector implements Apache Kafka [Sink Connector API](https://kafka.apache.org/documentation/#connect_development).

## Documentation
Documentation is maintained on [QuestDB.com](https://questdb.com/docs/third-party-tools/kafka/#questdb-kafka-connect-connector) 

## Experimental QWP transport

QuestDB WebSocket Protocol transport is experimental. It requires Kafka
Connect 3.6 or newer, `questdb-client` 1.3.8, and QuestDB 10 or newer. Select it
with a `ws::` or `wss::` client configuration string, for example:

```properties
client.conf.string=ws::addr=questdb:9000;sf_max_total_bytes=268435456;
```

QWP delivery is at least once. A reconnect, a split frame, or a multi-table
server failure can replay rows that QuestDB already committed. Configure
`DEDUP UPSERT KEYS` on target tables whenever duplicate rows are not acceptable.

The connector keeps Kafka as the durable log, so QWP store-and-forward is
memory-only: `sf_dir` and `sf_durability` are rejected. `max.inflight.rows` is
a soft pause threshold; `sf_max_total_bytes` is the hard client-memory
backstop. Keep `sf_append_deadline_millis` below the worker consumer's
`max.poll.interval.ms` (or set `consumer.override.max.poll.interval.ms` so the
connector can validate the relationship).

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

## Sample Projects
This repository contains a number of [sample projects.](kafka-questdb-connector-samples) showing how to use the connector. It also demonstrates how to use the connector together with Debezium for Change Data Capture.

## Distribution
Releases are published on GitHub: https://github.com/questdb/kafka-questdb-connector/releases/
It's also available in [Confluent Hub](https://www.confluent.io/hub/questdb/kafka-questdb-connector).

## Issues
If you encounter any issues, please [create an issue](https://github.com/questdb/kafka-questdb-connector/issues/new) in this repository.

## License
This project is licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.
