# Sample Project: Feeding changes from Postgres to QuestDB
## What does this sample do?
This sample project demonstrates how to feed changes from a Postgres table to QuestDB. It uses the [Debezium Postgres connector](https://debezium.io/documentation/reference/1.9/connectors/postgresql.html) to capture changes from a [Postgres database](https://www.postgresql.org/) and feed them to a [Kafka](https://kafka.apache.org/) topic. The [Kafka QuestDB connector](https://github.com/questdb/kafka-questdb-connector) then reads from the Kafka topic and writes the changes to a [QuestDB](questdb.io/) table. QuestDB is used for analytical queries on data and to feed the data to a Grafana dashboard for visualization.

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
The Postgres table has the following schema:
```sql
create table if not exists stock (
 id serial primary key,
 symbol varchar(10) unique,
 price float8,
 last_update timestamp
);
```
The table has always one row per each stock symbol. The `price` and `last_update` columns are updated every time a new price is received for the stock symbol. It mimics a real-world scenario where you would have a Postgres table with the latest prices for each stock symbol. Such table would be typically used by a transactional system to get the latest prices for each stock symbol. It our case the transactional system is simulated by a [simple Java application](src/main/java/io/questdb/kafka/samples/StockService.java) which is randomly updating prices for each stock symbol in the Postgres table. The application generates 1000s of updates each second. 

Then we have a pipeline which reads the changes from the Postgres table and feeds them to a QuestDB table. The pipeline is composed of the following components:
- [Debezium Postgres connector](https://debezium.io/documentation/reference/1.9/connectors/postgresql.html) which reads changes from the Postgres table and feeds them to a Kafka topic.
- Kafka QuestDB connector which reads changes from the Kafka topic and feeds them to a [QuestDB](https://questdb.io) table.
- QuestDB SQL console which is used to query the QuestDB table.
- [Grafana](https://grafana.com/) which is used to visualize the data in the QuestDB table.

Debezium is open source project which provides connectors for various databases. It is used to capture changes from a database and feed them to a Kafka topic. In other words: Whenever there is a change in a database table, Debezium will read the change and feed it to a Kafka topic. This way in translates operations such as INSERT or UPDATE into events which can be consumed by other systems. Debezium supports a wide range of databases. In this sample we use the Postgres connector. Debezium is technically implemented as a Kafka Connect source. 

For every change in the Postgres table the Debezium emits a JSON message to a Kafka topic. Messages look like this:
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

We cannot feed a full change object to Kafka Connect QuestDB Sink, because the sink would create a column for each field in the change object, including all metadata, like the source part of the JSON:
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
We do not want to create columns in QuestDB for all this metadata. We only want to create columns for the actual data. Debezium comes to the rescue! It ships with a Kafka Connect transform which can extract the actual data from the change object and feed it to the Kafka Connect sink. The transform is called `ExtractNewRecordState`. 

Postgres -> Kafka:
```shell
curl -X POST -H "Content-Type: application/json" -d  '{"name":"debezium_source","config":{"tasks.max":1,"database.hostname":"postgres","database.port":5432,"database.user":"postgres","database.password":"postgres","connector.class":"io.debezium.connector.postgresql.PostgresConnector","database.dbname":"postgres","database.server.name":"dbserver1"}} ' localhost:8083/connectors
```

Kafka -> QuestDB
```shell
curl -X POST -H "Content-Type: application/json" -d '{"name":"questdb-connect","config":{"topics":"dbserver1.public.stock","table":"stock", "connector.class":"io.questdb.kafka.QuestDBSinkConnector","tasks.max":"1","key.converter":"org.apache.kafka.connect.storage.StringConverter","value.converter":"org.apache.kafka.connect.json.JsonConverter","host":"questdb", "transforms":"unwrap", "transforms.unwrap.type":"io.debezium.transforms.ExtractNewRecordState", "include.key": "false", "symbols": "symbol", "timestamp.field.name": "last_update"}}' localhost:8083/connectors
```

[Grafana Dashboard](http://localhost:3000/d/stocks/stocks?orgId=1&refresh=5s&viewPanel=2)