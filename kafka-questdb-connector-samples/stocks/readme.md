# Sample Project: Feeding changes from Postgres to QuestDB
## What does this sample do?
This sample project demonstrates how to feed changes from a Postgres table to QuestDB. It uses the [Debezium Postgres connector](https://debezium.io/documentation/reference/1.9/connectors/postgresql.html) to capture changes from a [Postgres database](https://www.postgresql.org/) and feed them to a [Kafka](https://kafka.apache.org/) topic. The [Kafka QuestDB connector](https://github.com/questdb/kafka-questdb-connector) then reads from the Kafka topic and writes the changes to a [QuestDB](questdb.io/) table. QuestDB is used for analytical queries on data and to feed the data to a Grafana dashboard for visualization.

The project can be seen as a reference architecture for a data pipeline that feeds changes from a Postgres database to QuestDB. Postgres is an excellent [transaction/OLTP](https://en.wikipedia.org/wiki/Online_transaction_processing) database. It excels with simple short-running queries. Hence, the `stock` table contains only the most recent snapshot of the data. It stores no history at all. 

QuestDB is a time-series database which shines with time-series analytics. It is a great fit for storing historical data. The `stock` table inside QuestDB contains the full history of the `stock` table in Postgres. Whenever a stock price in Postgres is updated the change is written to QuestDB as a new row. 

## Prerequisites
- Git
- Working Docker environment, including docker-compose
- Internet access to download dependencies

The project was tested on MacOS with M1, but it should work on other platforms too. Please open a new issue if it's not working for you.

Bear in mind the sample starts multiple containers. It's running fine on my machines with 16GB RAM, but chances are it will struggle on machines with less RAM.

## Running the sample
1. Clone this repository via `git clone https://github.com/questdb/kafka-questdb-connector.git`
2. `cd kafka-questdb-connector/kafka-questdb-connector-samples/stocks/` to enter the directory with this sample.
3. Run `docker-compose build` to build docker images with the sample project. This will take a few minutes.
4. Run `docker-compose up` to start Postgres, Java stock price updater app, Apache Kafka, Kafka Connect with Debezium and QuestDB connectors, QuestDB and Grafana. This will take a few minutes.
5. The previous command will generate a lot of log messages. Eventually logging should cease. This means all containers are running. 
6. At this point we have all infrastructure running, the Java application keeps updating stock prices in Postgres. However, the rest of the pipeline is not yet running. We need to start the Kafka Connect connectors. Kafka Connect has a REST API, so we can use `curl` to start the connectors.
7. In a separate shell, execute following command to start Debezium connector:
    ```shell
    curl -X POST -H "Content-Type: application/json" -d  '{"name":"debezium_source","config":{"tasks.max":1,"database.hostname":"postgres","database.port":5432,"database.user":"postgres","database.password":"postgres","connector.class":"io.debezium.connector.postgresql.PostgresConnector","database.dbname":"postgres","database.server.name":"dbserver1"}} ' localhost:8083/connectors
    ```
   It starts the Debezium connector that will capture changes from Postgres and feed them to Kafka.
8. Execute following command to start QuestDB Kafka Connect sink:
    ```shell
    curl -X POST -H "Content-Type: application/json" -d '{"name":"questdb-connect","config":{"topics":"dbserver1.public.stock","table":"stock", "connector.class":"io.questdb.kafka.QuestDBSinkConnector","tasks.max":"1","key.converter":"org.apache.kafka.connect.storage.StringConverter","value.converter":"org.apache.kafka.connect.json.JsonConverter","host":"questdb", "transforms":"unwrap", "transforms.unwrap.type":"io.debezium.transforms.ExtractNewRecordState", "include.key": "false", "symbols": "symbol", "timestamp.field.name": "last_update"}}' localhost:8083/connectors
    ```
   It starts the QuestDB Kafka Connect sink that will read changes from Kafka and write them to QuestDB.
9. Go to [QuestDB Web Console](http://localhost:19000/) and execute following query:
    ```sql
    select * from stock;
    ```
   It should return some rows. If it does not return any rows or returns a _table not found_ error then wait a few seconds and try again.
10. Go to [Grafana Dashboard](http://localhost:3000/d/stocks/stocks?orgId=1&refresh=5s&viewPanel=2). It should show some data. If it does not show any data, wait a few seconds, refresh try again.
11. Play with the Grafana dashboard a bit. You can change the aggregation interval, change stock, zoom-in and zoom-out, etc.
12. Go to [QuestDB Web Console](http://localhost:19000/) again and execute following query:
    ```sql
    SELECT
      timestamp,
      symbol,
      avg(price),
      min(price),
      max(price)
    FROM stock
      where symbol = 'IBM'
    SAMPLE by 1m align to calendar;
    ```
    It returns the average, minimum and maximum stock price for IBM in each minute. You can change the `1m` to `1s` to get data aggregated by second. The `SAMPLE by` shows a bit of QuestDB syntax sugar to make time-related queries more readable. 
13. Don't forget to stop the containers when you're done. The project generates a lot of data and you could run out of disk space. 

## Project Internals
If you like what you see and want to learn more about the internals of the project, read on. It's time do demystify the black box. We will discuss these components:
1. Postgres and its schema
2. Java stock price updater
3. Debezium Postgres connector
4. Kafka QuestDB connector
5. QuestDB
6. Grafana

### Postgres
The docker-compose start Postgres image. It's using a container image provided by the Debezium project as they maintain a Postgres which is preconfigured for Debezium. 

### Java stock price updater
It's a Spring Boot application which during startup creates a table in Postgres and populates it with initial data.  
You can see the SQL executed in the [schema.sql](src/main/resources/schema.sql) file. The table has always one row per each stock symbol.

Once the application is started, it starts updating stock prices in regular intervals. The `price` and `last_update` columns are updated every time a new price is received for the stock symbol. It mimics a real-world scenario where you would have a Postgres table with the latest prices for each stock symbol. Such table would be typically used by a transactional system to get the latest prices for each stock symbol. It our case the transactional system is simulated by a [simple Java application](src/main/java/io/questdb/kafka/samples/StockService.java) which is randomly updating prices for each stock symbol in the Postgres table. The application generates 1000s of updates each second.

The application is build and packaged as container image when executing `docker-compose build`. Inside the [docker-compose file](docker-compose.yml) you can see the container called `producer`. That's our Java application.
```Dockerfile
  producer:
    image: kafka-questdb-connector-samples-stocks-generator
    build:
      dockerfile: ./Dockerfile
      context: .
    depends_on:
      postgres:
        condition: service_healthy
    links:
      - postgres:postgres
```

### Debezium Postgres connector
Debezium is an open source project which provides connectors for various databases. It is used to capture changes from a database and feed them to a Kafka topic. In other words: Whenever there is a change in a database table, Debezium will read the change and feed it to a Kafka topic. This way it translates operations such as INSERT or UPDATE into events which can be consumed by other systems. Debezium supports a wide range of databases. In this sample we use the Postgres connector.

The Debezium Postgres connector is implemented as a Kafka Connect source connector. Inside the [docker-compose file](docker-compose.yml) it's called `connect` and its container image is also built during `docker-compose build`. The [Dockerfile](../../Dockerfile-Samples) uses Debezium image. The Debezium image contains Kafka Connect runtime and Debezium connectors. Our Dockerfile amends it with Kafka Connect QuestDB Sink. 

What's important: When this container start it just connects to Kafka broker, but it does not start any connectors. We need to start the connectors using `curl` command. This is how we started the Debezium connector:
```shell
curl -X POST -H "Content-Type: application/json" -d  '{"name":"debezium_source","config":{"tasks.max":1,"database.hostname":"postgres","database.port":5432,"database.user":"postgres","database.password":"postgres","connector.class":"io.debezium.connector.postgresql.PostgresConnector","database.dbname":"postgres","database.server.name":"dbserver1"}} ' localhost:8083/connectors
```
It uses Kafka Connect REST interface to start a new connector with a give configuration. Let's have a closer look at the configuration. This is how it looks like when formatted for readability:
```json
{
  "name": "debezium_source",
  "config": {
    "tasks.max": 1,
    "database.hostname": "postgres",
    "database.port": 5432,
    "database.user": "postgres",
    "database.password": "postgres",
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.dbname": "postgres",
    "database.server.name": "dbserver1"
  }
}
```
Most of the fields are self-explanatory. The only non-obvious one is `database.server.name`. It's a unique name of the database server. It's used by Kafka Connect to store offsets. It's important that it's unique for each database server. If you have multiple Postgres databases, you need to use different `database.server.name` for each of them. It's used by Debezium to generate Kafka topic names. The topic name is generated as `database.server.name`.`schema`.`table`. In our case it's `dbserver1.public.stock`.

### Kafka QuestDB connector
The Kafka QuestDB connector re-uses the same Kafka Connect runtime as the Debezium connector. It's also started using `curl` command. This is how we started the QuestDB connector:
```shell
curl -X POST -H "Content-Type: application/json" -d '{"name":"questdb-connect","config":{"topics":"dbserver1.public.stock","table":"stock", "connector.class":"io.questdb.kafka.QuestDBSinkConnector","tasks.max":"1","key.converter":"org.apache.kafka.connect.storage.StringConverter","value.converter":"org.apache.kafka.connect.json.JsonConverter","host":"questdb", "transforms":"unwrap", "transforms.unwrap.type":"io.debezium.transforms.ExtractNewRecordState", "include.key": "false", "symbols": "symbol", "timestamp.field.name": "last_update"}}' localhost:8083/connectors
```
This is the connector JSON configuration nicely formatted:
```json
{
  "name": "questdb-connect",
  "config": {
    "topics": "dbserver1.public.stock",
    "table": "stock",
    "connector.class": "io.questdb.kafka.QuestDBSinkConnector",
    "tasks.max": "1",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "host": "questdb",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "include.key": "false",
    "symbols": "symbol",
    "timestamp.field.name": "last_update"
  }
}
```
Again, most of the fields are obvious. Let's focus on the non-obvious ones.
1. `"symbols": "symbol"` this instruct to connector to use the [QuestDB symbol type](https://questdb.io/docs/concept/symbol/) for a column named "symbols". This column has low cardinality thus it's a good candidate for symbol type.
2. `"timestamp.field.name": "last_update"` this instructs the connector to use the `last_update` column as the [designated timestamp](https://questdb.io/docs/concept/designated-timestamp/) column.
3. `"transforms":"unwrap"` and `"transforms.unwrap.type"` this instructs the connector to use Debezium's ExtractNewRecordState. 

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
      "version": "1.9.6.Final",
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
  "version": "1.9.6.Final",
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