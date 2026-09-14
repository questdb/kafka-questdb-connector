# Sample Project: Protobuf with Schema Registry
## What does this sample do?
This sample shows how to feed [Protobuf](https://protobuf.dev/) messages from Apache Kafka to QuestDB with the
[QuestDB Kafka connector](https://questdb.com/docs/third-party-tools/kafka/#questdb-kafka-connect-connector).
A small Java application produces random stock trades, serializes them as Protobuf and registers the message schema
in the [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html).
The connector reads the topic, resolves the schema from the registry and writes each trade as a row of a QuestDB table.

This is the pipeline:
```
TradesProducer (Java) --Protobuf--> Kafka topic "trades" --ProtobufConverter--> QuestDB connector --> QuestDB table "trades"
        |                                                          ^
        +--------- registers schema ---> Schema Registry <--- fetches schema
```

## Prerequisites
- Git
- Working Docker environment, including docker-compose
- Internet access to download dependencies

The sample starts 5 containers. It needs a few GB of RAM; 8GB is enough.

## Running the sample
1. Clone this repository via `git clone https://github.com/questdb/kafka-questdb-connector.git`
2. `cd kafka-questdb-connector/kafka-questdb-connector-samples/protobuf-schema-registry/` to enter the directory with this sample.
3. Run `docker compose build` to build the Docker images with the Java producer and Kafka Connect. This takes a few minutes: the producer image compiles the Protobuf schema and downloads its Maven dependencies.
4. Run `docker compose up` to start Kafka, Schema Registry, Kafka Connect, QuestDB and the producer.
5. The previous command generates a lot of log messages. Once the producer logs `Sent 100 trades` the whole pipeline up to Kafka is working.
6. The producer has registered the schema of the `Trade` message in the Schema Registry. You can look at it:
    ```shell
    curl -s localhost:8081/subjects/trades-value/versions/latest
    ```
   The subject name is `<topic>-value`. This is the schema the connector fetches when it reads the topic.
7. At this point Kafka Connect is running but no connector is configured yet. Start the QuestDB connector via the Kafka Connect REST API:
    ```shell
    curl -X POST -H "Content-Type: application/json" -d '{"name":"questdb-connect","config":{"connector.class":"io.questdb.kafka.QuestDBSinkConnector","tasks.max":"1","topics":"trades","key.converter":"org.apache.kafka.connect.storage.StringConverter","value.converter":"io.confluent.connect.protobuf.ProtobufConverter","value.converter.schema.registry.url":"http://schema-registry:8081","client.conf.string":"ws::addr=questdb:9000;","timestamp.field.name":"timestamp","symbols":"symbol,side,exchange","include.key":"false"}}' localhost:8083/connectors
    ```
   Kafka Connect responds with the configuration it accepted. The configuration is explained in detail [below](#configuring-the-connector-for-protobuf).
8. Go to the QuestDB Web Console running at http://localhost:19000/ and execute:
    ```sql
    select * from trades;
    ```
   You should see trades arriving. If the table does not exist yet, wait a few seconds and try again. The table was created by the connector, with column types derived from the Protobuf schema:
    ```sql
    show columns from trades;
    ```
9. Try a time-series query. This returns the volume-weighted average price per symbol for every 10 seconds:
    ```sql
    SELECT timestamp, symbol, sum(price * quantity) / sum(quantity) AS vwap, sum(quantity) AS volume
    FROM trades
    WHERE side = 'BUY'
    SAMPLE BY 10s;
    ```
10. Run `docker compose down` when you are done.

## Configuring the connector for Protobuf
This is the connector configuration submitted in step 7, formatted for readability:
```json
{
  "name": "questdb-connect",
  "config": {
    "connector.class": "io.questdb.kafka.QuestDBSinkConnector",
    "tasks.max": "1",
    "topics": "trades",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "io.confluent.connect.protobuf.ProtobufConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "client.conf.string": "ws::addr=questdb:9000;",
    "timestamp.field.name": "timestamp",
    "symbols": "symbol,side,exchange",
    "include.key": "false"
  }
}
```
Only two settings are specific to Protobuf, the rest is the same as for any other format:

1. `"value.converter": "io.confluent.connect.protobuf.ProtobufConverter"` tells Kafka Connect how to turn the bytes of a
   Kafka message into a structured record. The Protobuf converter reads the schema ID that the Confluent serializer prepends
   to every message, fetches that schema from the Schema Registry and decodes the payload with it. The QuestDB connector
   never sees Protobuf: it receives a generic Kafka Connect record with a schema, exactly as it would with Avro or JSON.
2. `"value.converter.schema.registry.url": "http://schema-registry:8081"` is where the converter finds the Schema Registry.
   Every setting prefixed with `value.converter.` is passed to the converter. If your registry requires authentication,
   for example Confluent Cloud, add `"value.converter.basic.auth.credentials.source": "USER_INFO"` and
   `"value.converter.basic.auth.user.info": "<API key>:<API secret>"`.

The remaining settings:

- `"key.converter": "org.apache.kafka.connect.storage.StringConverter"` - the producer uses the stock symbol, a plain string,
  as the message key. Keys can be Protobuf too; then use the `ProtobufConverter` for the key as well, with `key.converter.schema.registry.url`.
- `"include.key": "false"` - do not write the message key as a column. The symbol is already a field of the message.
- `"timestamp.field.name": "timestamp"` - use the `timestamp` field of the message as the [designated timestamp](https://questdb.com/docs/concept/designated-timestamp/)
  of the table. It is a `google.protobuf.Timestamp`, which the converter turns into a Kafka Connect timestamp, and the
  connector understands that natively. Without this setting the connector would use the time the message was written to Kafka.
- `"symbols": "symbol,side,exchange"` - store these low-cardinality string fields as QuestDB [SYMBOL](https://questdb.com/docs/concept/symbol/) columns
  instead of VARCHAR. Note that `side` is a Protobuf enum; the converter delivers enums as their symbolic name.
- `"client.conf.string": "ws::addr=questdb:9000;"` - how to reach QuestDB. `questdb` is the hostname of the QuestDB
  container in [docker-compose.yml](docker-compose.yml). `ws::` selects the QuestDB WebSocket Protocol (QWP) transport,
  which commits Kafka offsets only after QuestDB acknowledges the rows and keeps writes pipelined over slow links. It needs
  QuestDB 10 or newer; the [faker](../faker) sample explains the trade-offs, including at-least-once delivery. `http::`
  works too and is the choice for older QuestDB versions.

### Where does the ProtobufConverter come from?
The converter is not part of Apache Kafka. It is developed by Confluent and ships with the `confluentinc/cp-kafka-connect`
Docker image used in [Dockerfile-Connect](Dockerfile-Connect), so this sample only adds the QuestDB connector jars.
If you run Kafka Connect from a plain Apache Kafka distribution or another image, install the converter first, either
with `confluent-hub install confluentinc/kafka-connect-protobuf-converter:<version>` or by copying the
[kafka-connect-protobuf-converter](https://www.confluent.io/hub/confluentinc/kafka-connect-protobuf-converter) jars into the plugin path.

## How Protobuf fields map to QuestDB columns
The converter maps Protobuf types to Kafka Connect types and the connector maps those to QuestDB columns. Column names are
the Protobuf field names, so `snake_case` names in the `.proto` file become `snake_case` columns.

| Protobuf                                           | QuestDB column                                              |
|----------------------------------------------------|-------------------------------------------------------------|
| `string`                                           | `VARCHAR`, or `SYMBOL` when listed in `symbols`             |
| `bool`                                             | `BOOLEAN`                                                   |
| `int32`, `int64`, `sint*`, `fixed*`, `uint*`       | `LONG`                                                      |
| `float`, `double`                                  | `DOUBLE`                                                    |
| `enum`                                             | `VARCHAR` with the symbolic name, or `SYMBOL` when listed in `symbols` |
| `google.protobuf.Timestamp`                        | `TIMESTAMP`, millisecond precision                          |
| nested message                                     | one column per nested field, named `outer_inner`            |
| `repeated float`, `repeated double`                | `DOUBLE[]` array                                            |
| `bytes`, `map`                                     | not supported: the record fails, or the field is skipped with `"skip.unsupported.types": "true"` |

A few things worth knowing:

- **Missing values.** In proto3 a scalar field that was never set is indistinguishable from one set to its default:
  `0`, `false` or `""` is what arrives, and that is what gets written. Declare the field `optional` when you need to tell
  the two apart; an unset `optional` field, an unset nested message and an unset `google.protobuf.Timestamp` arrive as
  null, and the connector then skips the column for that row. The designated timestamp is the exception: it must be set.
- **Timestamps** keep millisecond precision only. `google.protobuf.Timestamp` carries nanoseconds, but the converter
  hands the connector a Kafka Connect `Timestamp`, which is a millisecond value. If you need microseconds, send the
  timestamp as an `int64` of epoch micros and set `"timestamp.units": "micros"` on the connector.
- **Schema evolution.** Adding a field to the message adds a column to the table the first time a message with the new
  schema arrives; older rows have null in the new column, or `false` for a `BOOLEAN` column, which has no null.
  Removing a field leaves the column in place, and renaming a
  field is a removal plus an addition, so the data ends up in a new column. The Schema Registry enforces its own
  [compatibility rules](https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html)
  on top of that; with the default `BACKWARD` level, adding and removing fields is allowed.

The [integration test](../../integration-tests/protobuf-schema-registry) of the connector covers these cases,
including arrays, nested messages, enums and schema evolution, if you want to see them exercised end to end.

## Project internals
The sample consists of 3 parts:

1. The [Protobuf schema](src/main/proto/trade.proto) of a trade:
    ```protobuf
    message Trade {
      string symbol = 1;
      Side side = 2;
      double price = 3;
      int64 quantity = 4;
      string exchange = 5;
      google.protobuf.Timestamp timestamp = 6;
    }
    ```
   [pom.xml](pom.xml) uses the `protobuf-maven-plugin` to generate a `Trade` Java class from it during the build.
2. The [TradesProducer](src/main/java/io/questdb/kafka/samples/TradesProducer.java) application. It builds `Trade`
   objects and sends them with a plain `KafkaProducer` whose value serializer is the Confluent `KafkaProtobufSerializer`:
    ```java
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
    props.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://schema-registry:8081");
    ```
   On the first send the serializer registers the schema of `Trade` in the registry under the subject `trades-value`
   and from then on prefixes every message with the ID of that schema. This is the wire format the `ProtobufConverter`
   on the Kafka Connect side expects; a Protobuf message written without the Confluent serializer has no schema ID and
   the converter cannot decode it.
3. The [docker-compose.yml](docker-compose.yml) file, which starts:
   - Kafka - the message broker, a single node in KRaft mode
   - Schema Registry - stores the Protobuf schemas
   - Kafka Connect - built from [Dockerfile-Connect](Dockerfile-Connect), the Confluent image with the latest
     [QuestDB connector release](https://github.com/questdb/kafka-questdb-connector/releases) added
   - QuestDB - the database, its web console is exposed on port 19000
   - the producer - built from [Dockerfile-App](Dockerfile-App)

   The Kafka Connect worker is configured with `JsonConverter` as the default value converter. The QuestDB connector
   overrides it with the `ProtobufConverter` in its own configuration. This is the usual way to run connectors for
   different formats on one Kafka Connect cluster.

## Further reading
- [QuestDB Kafka connector documentation](https://questdb.com/docs/third-party-tools/kafka/#questdb-kafka-connect-connector)
- [Protobuf Schema Serializer and Deserializer](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/serdes-protobuf.html) in the Confluent documentation
- [Avro integration test](../../integration-tests/avro-schema-registry) of the connector, if you use Avro instead of Protobuf

## Bugs and Feedback
For bugs, questions and discussions please use the [Github Issues](https://github.com/questdb/kafka-questdb-connector/issues/new)
