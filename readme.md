# QuestDB Sink connector for Apache Kafka
The connector reads data from Kafka topics and writes to [QuestDB](https://questdb.io/) tables.
The connector implements Apache Kafka [Sink Connector API](https://kafka.apache.org/documentation/#connect_development).

## Documentation
Documentation is maintained on [QuestDB.com](https://questdb.com/docs/third-party-tools/kafka/#questdb-kafka-connect-connector) 

## QWP transport

The QuestDB WebSocket Protocol transport keeps writes pipelined and commits Kafka
offsets only after QuestDB acknowledges the data. It requires QuestDB 10 or newer
and Kafka Connect 3.6 or newer. Select it with a `ws::` or `wss::` client
configuration string; `http::` and `tcp::` keep working unchanged.

```properties
client.conf.string=ws::addr=questdb:9000;
```

Two things to check before switching:

- Delivery is at least once. Reconnects and rejections can replay rows QuestDB
  already holds. Configure `DEDUP UPSERT KEYS` on target tables whenever
  duplicate rows are not acceptable.
- Bad records reach the dead letter queue only with `errors.tolerance=all` and a
  DLQ topic configured, and by default only for `SCHEMA_MISMATCH` rejections.
  Without them a schema mismatch fails the task.

Two client settings are constrained. Store-and-forward is memory-only because
Kafka is already the durable log, so `sf_dir` and `sf_durability` are rejected.
Keep `sf_append_deadline_millis` below the worker consumer's
`max.poll.interval.ms`; set `consumer.override.max.poll.interval.ms` if you want
the connector to check that relationship for you.

The QuestDB.com documentation covers the rest: the `qwp.*` settings, how
`auto_flush_rows` and `auto_flush_interval` set checkpoint boundaries, and how
outages, rejections and shutdown are handled.

## Raw JSON fast path (experimental)

For JSON payloads the connector can skip `JsonConverter` and parse the bytes
directly into rows, which measured +48% end-to-end throughput on a single task.

```properties
value.converter=org.apache.kafka.connect.converters.ByteArrayConverter
value.format=json
```

Use `value.format=json_envelope` when the producer wraps payloads in
`JsonConverter`'s schema envelope. The mode is never guessed from the data, so
pick the one matching your producer.

The main limitation is that SMTs which inspect or modify the payload cannot be
used, because with `ByteArrayConverter` they see opaque bytes; topic-level SMTs
such as `RegexRouter` still work. The documentation lists the remaining
differences from the standard path.

## Sample Projects
This repository contains a number of [sample projects.](kafka-questdb-connector-samples) showing how to use the connector. It also demonstrates how to use the connector together with Debezium for Change Data Capture.

## Distribution
Releases are published on GitHub: https://github.com/questdb/kafka-questdb-connector/releases/
It's also available in [Confluent Hub](https://www.confluent.io/hub/questdb/kafka-questdb-connector).

## Issues
If you encounter any issues, please [create an issue](https://github.com/questdb/kafka-questdb-connector/issues/new) in this repository.

## License
This project is licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.
