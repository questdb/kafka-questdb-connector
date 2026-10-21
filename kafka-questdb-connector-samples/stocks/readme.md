# Sample Project: Feeding changes from Postgres to QuestDB
## What does this sample do?
This sample project demonstrates how to feed changes from a Postgres table to QuestDB. It uses the [Debezium Postgres connector](https://debezium.io/documentation/reference/stable/connectors/postgresql.html) to capture changes from a [Postgres database](https://www.postgresql.org/) and feed them to a [Kafka](https://kafka.apache.org/) topic. The [Kafka QuestDB connector](https://github.com/questdb/kafka-questdb-connector) then reads from the Kafka topic and writes the changes to a [QuestDB](https://questdb.com/) table. QuestDB is used for analytical queries on data and to feed the data to a Grafana dashboard for visualization.

The project can be seen as a reference architecture for a data pipeline that feeds changes from a Postgres database to QuestDB. Postgres is an excellent [transaction/OLTP](https://en.wikipedia.org/wiki/Online_transaction_processing) database. It excels with simple short-running queries. Hence, the `stock` table contains only the most recent snapshot of the data. It stores no history at all. 

QuestDB is a time-series database which shines with time-series analytics. It is a great fit for storing historical data. The `stock` table inside QuestDB contains the full history of the `stock` table in Postgres. Whenever a stock price in Postgres is updated the change is written to QuestDB as a new row. 

This is the third and last step of the [learning path](../readme.md). The [faker](../faker) and
[protobuf-schema-registry](../protobuf-schema-registry) samples feed a Kafka topic from a producer you control.
This one adds a second system, Postgres, and Debezium in between: the events in Kafka are database changes, not
messages an application wrote. It also adds Debezium's `unwrap` transform and a Grafana dashboard on QuestDB.

## Prerequisites
- Git
- Working Docker environment, including docker-compose
- Internet access to download dependencies

The project was tested on MacOS with M1, but it should work on other platforms too. Please open a new issue if it's not working for you.

Bear in mind the sample starts multiple containers. It's running fine on my machines with 16GB RAM, but chances are it will struggle on machines with less RAM.

## Running the sample
1. Clone this repository via `git clone https://github.com/questdb/kafka-questdb-connector.git`
2. `cd kafka-questdb-connector/kafka-questdb-connector-samples/stocks/` to enter the directory with this sample.
3. Run `docker compose up --build --wait`. It builds the images, starts Postgres, the Java stock price updater, Apache Kafka, Kafka Connect with the Debezium and QuestDB connectors, QuestDB and Grafana in the background, and returns once Kafka Connect is ready to accept connectors. The first run takes a few minutes.
4. At this point we have all infrastructure running, the Java application keeps updating stock prices in Postgres. However, the rest of the pipeline is not yet running. We need to start the two Kafka Connect connectors. Kafka Connect has a REST API, so we can use `curl` to start them.
5. Start the Debezium connector from [debezium-source.json](debezium-source.json):
    ```shell
    curl -X POST -H "Content-Type: application/json" -d @debezium-source.json localhost:8083/connectors
    ```
   It captures changes from Postgres and feeds them to Kafka.
6. Start the QuestDB Kafka Connect sink from [questdb-sink.json](questdb-sink.json):
    ```shell
    curl -X POST -H "Content-Type: application/json" -d @questdb-sink.json localhost:8083/connectors
    ```
   It reads the changes from Kafka and writes them to QuestDB.
7. Go to QuestDB Web Console running at http://localhost:19000/ and execute following query:
    ```sql
    select * from stock;
    ```
   It should return some rows. If it does not return any rows or returns a _table not found_ error then wait a few seconds and try again.
8. Go to  Grafana Dashboard running at http://localhost:3000/d/stocks/stocks?orgId=1&refresh=5s&viewPanel=2. It should show some data. If it does not show any data, wait a few seconds, refresh try again.
9. Play with the Grafana dashboard a bit. You can change the aggregation interval, change stock, zoom-in and zoom-out, etc.
10. Go to [QuestDB Web Console](http://localhost:19000/) again and execute following query:
    ```sql
    SELECT
      timestamp,
      symbol,
      avg(price),
      min(price),
      max(price)
    FROM stock
      where symbol = 'IBM'
    SAMPLE by 1m;
    ```
    It returns the average, minimum and maximum stock price for IBM in each minute. You can change the `1m` to `1s` to get data aggregated by second. The `SAMPLE by` shows a bit of QuestDB syntax sugar to make time-related queries more readable. 
11. Run `docker compose down` when you're done. The project generates a lot of data and you could run out of disk space. 

## Project Internals
If you like what you see and want to learn more about the internals of the project, read on. It's time do demystify the black box. We will discuss these components:
1. Postgres and its schema
2. Java stock price updater
3. Debezium Postgres connector
4. Kafka QuestDB connector
5. QuestDB
6. Grafana

### Postgres
The docker compose starts the [official Postgres container image](https://hub.docker.com/_/postgres). The only Debezium-specific bit is that Postgres must run with `wal_level=logical`, so that Debezium can read changes from its write-ahead log. This is set via the container command in the [docker-compose file](docker-compose.yml):
```yaml
  postgres:
    image: postgres:18
    command: postgres -c wal_level=logical
```

### Java stock price updater
It's a Spring Boot application which during startup creates a table in Postgres and populates it with initial data.  
You can see the SQL executed in the [schema.sql](src/main/resources/schema.sql) file. The table has always one row per each stock symbol.

Once the application is started, it starts updating stock prices in regular intervals. The `price` and `last_update` columns are updated every time a new price is received for the stock symbol. It mimics a real-world scenario where you would have a Postgres table with the latest prices for each stock symbol. Such table would be typically used by a transactional system to get the latest prices for each stock symbol. It our case the transactional system is simulated by a [simple Java application](src/main/java/io/questdb/kafka/samples/StockService.java) which is randomly updating prices for each stock symbol in the Postgres table. The application generates 1000s of updates each second.

The application is built and packaged as a container image when executing `docker compose build`. Inside the [docker-compose file](docker-compose.yml) you can see the container called `producer`. That's our Java application.
```yaml
  producer:
    image: kafka-questdb-connector-samples-stocks-generator
    build:
      dockerfile: Dockerfile-App
      context: .
    depends_on:
      postgres:
        condition: service_healthy
```
The [Dockerfile](Dockerfile-App) is rather trivial:
```Dockerfile
FROM maven:3.9-eclipse-temurin-21 AS builder
COPY ./pom.xml /opt/stocks/pom.xml
COPY ./src ./opt/stocks/src
WORKDIR /opt/stocks
RUN mvn clean install -DskipTests

FROM eclipse-temurin:21-jre
COPY --from=builder /opt/stocks/target/kafka-samples-stocks-*.jar /stocks.jar
CMD ["java", "-jar", "/stocks.jar"]
```
It uses Maven to build the application and then copies the resulting JAR file to the container image. The container image is based on Eclipse Temurin JRE 21. The application is started with `java -jar /stocks.jar` command.

### Debezium Postgres connector
Debezium is an open source project which provides connectors for various databases. It is used to capture changes from a database and feed them to a Kafka topic. In other words: Whenever there is a change in a database table, Debezium will read the change and feed it to a Kafka topic. This way it translates operations such as INSERT or UPDATE into events which can be consumed by other systems. Debezium supports a wide range of databases. In this sample we use the Postgres connector.

The Debezium Postgres connector is implemented as a Kafka Connect source connector. Inside the [docker-compose file](docker-compose.yml) it's called `connect` and its container image is also built during `docker compose build`. The [Dockerfile](Dockerfile-Connect) starts from the [official Apache Kafka image](https://hub.docker.com/r/apache/kafka), which already contains the Kafka Connect runtime. It downloads the Debezium Postgres connector and the QuestDB connector into the `/opt/kafka/plugins` directory and starts Kafka Connect in distributed mode:
```Dockerfile
# Kafka Connect worker built from the official Apache Kafka image.
# The image ships the Connect runtime; we only add the two connector plugins.
FROM apache/kafka:4.3.1

# The QuestDB client bundles a small native library built against glibc.
# The Kafka image is Alpine (musl) based, so add the glibc compatibility layer.
USER root
RUN apk add --no-cache gcompat libstdc++
USER appuser

ARG DEBEZIUM_VERSION=3.6.2.Final
WORKDIR /opt/kafka/plugins

# Debezium Postgres source connector (Postgres -> Kafka)
RUN wget -qO- https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/${DEBEZIUM_VERSION}/debezium-connector-postgres-${DEBEZIUM_VERSION}-plugin.tar.gz | tar xz

# QuestDB sink connector (Kafka -> QuestDB), latest release
RUN wget -q $(wget -qO- https://api.github.com/repos/questdb/kafka-questdb-connector/releases/latest | grep -o 'https://[^"]*-bin.zip') \
    && unzip -q kafka-questdb-connector-*-bin.zip \
    && rm kafka-questdb-connector-*-bin.zip

COPY connect-distributed.properties /opt/kafka/config/connect-distributed.properties
CMD ["/opt/kafka/bin/connect-distributed.sh", "/opt/kafka/config/connect-distributed.properties"]
```
The Kafka Connect worker itself is configured in [connect-distributed.properties](connect-distributed.properties). It tells the worker where the Kafka broker is, where to look for connector plugins and which Kafka topics to use for its own bookkeeping. 

What's important: When this container start it just connects to Kafka broker, but it does not start any connectors. We need to start the connectors using `curl` command. This is how we started the Debezium connector:
```shell
curl -X POST -H "Content-Type: application/json" -d @debezium-source.json localhost:8083/connectors
```
It uses Kafka Connect REST interface to start a new connector with a given configuration. Let's have a closer look at the configuration in [debezium-source.json](debezium-source.json):
```json
{
  "name": "debezium_source",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": 1,
    "database.hostname": "postgres",
    "database.port": 5432,
    "database.user": "postgres",
    "database.password": "postgres",
    "database.dbname": "postgres",
    "plugin.name": "pgoutput",
    "topic.prefix": "dbserver1"
  }
}
```
Most of the fields are self-explanatory. `plugin.name` selects the Postgres logical decoding plugin. `pgoutput` is built into Postgres, so no extra extension has to be installed. The other non-obvious one is `topic.prefix`. It's used by Debezium to generate Kafka topic names. The topic name is generated as `topic.prefix`.`schema`.`table`. In our case it's `dbserver1.public.stock`. It's important that it's unique for each database server. If you have multiple Postgres databases, you need to use different `topic.prefix` for each of them.

### Kafka QuestDB connector
The Kafka QuestDB connector re-uses the same Kafka Connect runtime as the Debezium connector. It's also started using `curl` command. This is how we started the QuestDB connector:
```shell
curl -X POST -H "Content-Type: application/json" -d @questdb-sink.json localhost:8083/connectors
```
This is the configuration in [questdb-sink.json](questdb-sink.json):
```json
{
  "name": "questdb-connect",
  "config": {
    "connector.class": "io.questdb.kafka.QuestDBSinkConnector",
    "tasks.max": "1",
    "topics": "dbserver1.public.stock",
    "table": "stock",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "client.conf.string": "ws::addr=questdb:9000;",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "include.key": "false",
    "symbols": "symbol",
    "timestamp.field.name": "last_update"
  }
}
```
Most of it is the same as in the [faker](../faker) sample. The differences:
1. `"table": "stock"` the Debezium topic is called `dbserver1.public.stock`, which is not a name we want for a QuestDB table, so the target table is set explicitly.
2. `"value.converter"` is used without `"value.converter.schemas.enable": "false"`, because Debezium embeds a schema in every JSON message.
3. `"timestamp.field.name": "last_update"` the Postgres column that holds the time of the price update becomes the [designated timestamp](https://questdb.io/docs/concept/designated-timestamp/). Debezium sends it as microseconds since the epoch and the connector understands that natively.
4. `"transforms":"unwrap"` and `"transforms.unwrap.type"` this instructs the connector to use Debezium's ExtractNewRecordState. 

Let's focus on the ExtractNewRecordState transform a bit more. Why is it needed at all? For every change in the Postgres table the Debezium emits a JSON message to a Kafka topic. Messages look like this:
```json
{
  "schema": {
     "comment": "this contains Debezium message schema, it's not very relevant for this sample"
  },
  "payload": {
    "before": null,
    "after": {
      "id": 8,
      "symbol": "NFLX",
      "price": 1544.3357414199545,
      "last_update": 1666172978269856
    },
    "source": {
      "version": "3.6.2.Final",
      "connector": "postgresql",
      "name": "dbserver1",
      "ts_ms": 1666172978272,
      "snapshot": "false",
      "db": "postgres",
      "sequence": "[\"87397208\",\"87397208\"]",
      "schema": "public",
      "table": "stock",
      "txId": 402087,
      "lsn": 87397208,
      "xmin": null
    },
    "op": "u",
    "ts_ms": 1666172978637,
    "transaction": null
  }
}
```
You can see the `payload` field contains the actual change. Let's zoom it a bit a focus on this part of the JSON:
```json
[...]
"after": {
  "id": 8,
  "symbol": "NFLX",
  "price": 1544.3357414199545,
  "last_update": 1666172978269856
},
[...]
```
This is the actual change in a table. It's a JSON object which contains the new values for the columns in the Postgres table. Notice has the structure maps to the Postgres table schema described above. 

We cannot feed a full change object to Kafka Connect QuestDB Sink, because the sink would create a column for each field in the change object, including all metadata, for example the source part of the JSON:
```json
"source": {
  "version": "3.6.2.Final",
  "connector": "postgresql",
  "name": "dbserver1",
  "ts_ms": 1666172978272,
  "snapshot": "false",
  "db": "postgres",
  "sequence": "[\"87397208\",\"87397208\"]",
  "schema": "public",
  "table": "stock",
  "txId": 402087,
  "lsn": 87397208,
  "xmin": null
},
```

We do not want to create columns in QuestDB for all this metadata. We only want to create columns for the actual data. This is where the `ExtractNewRecordState` transform comes to the rescue! It extracts only the actual new data from the overall change object and feeds only this small part to the QuestDB sink. The end-result is that each INSERT and UPDATE in Postgres will insert a new row in QuestDB.

### QuestDB
QuestDB is a fast, open-source time-series database. It uses SQL for querying and it adds a bit of syntax sugar on top of SQL to make it easier to work with time-series data. It implements the Postgres wire protocol so many tools can be used to connect to it. 

### Grafana
Grafana is a popular open-source tool for visualizing time-series data. It can be used to visualize data from QuestDB. There is no native QuestDB datasource for Grafana, but there is a Postgres datasource. We can use this datasource to connect to QuestDB. Grafana is provisioned with a dashboard that visualizes the data from QuestDB in a candlestick chart. The char is configured to execute this query:
```sql
SELECT
  $__time(timestamp),
  min(price) as low,
  max(price) as high,
  first(price) as open,
  last(price) as close
FROM
  stock
WHERE
  $__timeFilter(timestamp)
  and symbol = '$Symbol'
SAMPLE BY $Interval ALIGN TO CALENDAR;
```
`$__time` is a Grafana macro that converts the timestamp column to the format expected by Grafana. `$__timeFilter` is another Grafana macro that filters the data based on the time range selected in the Grafana dashboard. `$Symbol` is a variable that can be set in the Grafana dashboard. `$Interval` is another variable that can be set in the Grafana dashboard. It controls the granularity of the data.

Grafana will resolve the macros and execute queries similar to this:
```json
SELECT
  timestamp AS "time",
  min(price) as low,
  max(price) as high,
  first(price) as open,
  last(price) as close
FROM
  stock
WHERE
  timestamp BETWEEN '2022-10-19T12:23:44.951Z' AND '2022-10-19T12:28:44.951Z'
  and symbol = 'SNAP'
SAMPLE BY 5s ALIGN TO CALENDAR;
```
And this is then used by the candlestick chart to visualize the data.

### Summary of internals
At this point you should have a good understanding of the architecture. If the explanation above is unclear then please [open a new issue](https://github.com/questdb/kafka-questdb-connector/issues/new).

## Where to go next
This is the end of the learning path. The [sample index](../readme.md) lists all samples, and the
[connector documentation](https://questdb.com/docs/third-party-tools/kafka/questdb-kafka/) has the full
configuration reference.
