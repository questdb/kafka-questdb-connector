# Samples Projects

There are 4 sample projects:
## [Faker](faker)
Simplistic project which uses a simple node.js application to create JSON entries in Apache Kafka and QuestDB Kafka Connect Sink to feed generated data from Kafka to QuestDB.

## [Stocks](stocks)
This project uses Debezium to stream data from Postgres to Kafka and QuestDB Kafka Connect Sink to feed data from Kafka to QuestDB. It also uses Grafana to visualize the data.

## [Confluent-Docker-Images](confluent-docker-images)
This project uses Confluent Docker images to create a Kafka cluster and QuestDB Kafka Connect Sink to feed data from Kafka to QuestDB. It installs the QuestDB Kafka Connect Sink from the [Confluent Hub](https://www.confluent.io/hub/questdb/kafka-questdb-connector).

## [Protobuf-Schema-Registry](protobuf-schema-registry)
This project uses a Java application to produce Protobuf messages to Apache Kafka with the Confluent Schema Registry and QuestDB Kafka Connect Sink configured with the Protobuf converter to feed the data from Kafka to QuestDB. It includes a step-by-step guide on configuring the connector for Protobuf.
