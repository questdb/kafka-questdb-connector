# Sample Projects

**Start here:** the [Faker](faker) sample. It is the shortest path from `docker compose up` to a `trades` table
in QuestDB that you can query.

All samples stream trades, the same data as the `trades` table on [demo.questdb.io](https://demo.questdb.io),
so what you learn from one applies to the next. Each sample adds one concept on top of the previous one.

## Learning path
### 1. [Faker](faker) - JSON trades over QWP
A Node.js application generates trades as JSON and sends them to Kafka. The QuestDB Kafka connector reads
the topic over the QWP (`ws::`) transport and creates the `trades` table. Covers the connector configuration,
choosing the designated timestamp, and deduplication for at-least-once delivery.

### 2. [Protobuf with Schema Registry](protobuf-schema-registry) - typed messages
The same trade stream, produced by a Java application as Protobuf messages with the Confluent Schema Registry.
Adds the Protobuf converter and how Protobuf types map to QuestDB columns.

### 3. [Stocks](stocks) - change data capture with Debezium
A multi-system pipeline: a Java application updates stock prices in Postgres, Debezium streams the changes to
Kafka, the QuestDB connector turns them into price history, and Grafana charts it. Adds Debezium's `unwrap`
transform and a Grafana dashboard on QuestDB.

## Outside the learning path
### [Confluent Docker images](confluent-docker-images) - Confluent Hub install
Installs the connector from the [Confluent Hub](https://www.confluent.io/hub/questdb/kafka-questdb-connector)
into the Confluent Kafka Connect image and starts it from the Kafka UI. For users of Confluent Platform. Its
pinned stack is older than the other samples and is not actively updated.
