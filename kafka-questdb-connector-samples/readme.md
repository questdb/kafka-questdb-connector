# Samples Projects

There are 3 sample projects:
## [Faker](faker)
Simplistic project which uses a simple node.js application to create JSON entries in Apache Kafka and QuestDB Kafka Connect Sink to feed generated data from Kafka to QuestDB.

## [Stocks](stocks)
This project uses Debezium to stream data from Postgres to Kafka and QuestDB Kafka Connect Sink to feed data from Kafka to QuestDB. It also uses Grafana to visualize the data.

## [Confluent-Docker-Images](confluent-docker-images)
This project uses Confluent Docker images to create a Kafka cluster and QuestDB Kafka Connect Sink to feed data from Kafka to QuestDB. It installs the QuestDB Kafka Connect Sink from the [Confluent Hub](https://www.confluent.io/hub/questdb/kafka-questdb-connector).