# QuestDB Sink connector for Apache Kafka
The connector reads data from Kafka topics and writes it to [QuestDB](https://questdb.io/) tables.
The connector implements Apache Kafka [Sink Connector API](https://kafka.apache.org/documentation/#connect_development).

## Pre-requisites
* QuestDB 6.5.0 or newer
* Apache Kafka 2.8.0 or newer, running on Java 11 or newer.

## Usage with Kafka Connect
This guide assumes you are already familiar with Apache Kafka and Kafka Connect. If you are not then watch this [excellent video](https://www.youtube.com/watch?v=Jkcp28ki82k) or check our [sample projects](kafka-questdb-connector-samples).
1. [Download](https://github.com/questdb/kafka-questdb-connector/releases/latest) and unpack connector ZIP into Apache Kafka `./libs/` directory.
2. Start Kafka Connect in the distributed mode.
3. Create a connector configuration:
    ```json
    {
      "name": "questdb-sink",
      "config": {
        "connector.class": "io.questdb.kafka.QuestDBSinkConnector",
        "host": "localhost:9009",
        "topics": "Orders",
        "table": "orders_table",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "include.key": "false"
      }
    }
    ```
4. Submit the configuration to Kafka Connect. 
5. From now on JSON entries sent to Kafka topic `Orders` will be written to QuestDB table `orders_table`.

See [sample projects](kafka-questdb-connector-samples) for more details.

## Configuration
The connector supports following Options:

| Name                   | Type    | Example                                                     | Default            | Meaning                                                    |
|------------------------|---------|-------------------------------------------------------------|--------------------|------------------------------------------------------------|
| topics                 | STRING  | orders                                                      | N/A                | Topics to read from                                        |
| key.converter          | STRING  | <sub>org.apache.kafka.connect.storage.StringConverter</sub> | N/A                | Converter for keys stored in Kafka                         |
| value.converter        | STRING  | <sub>org.apache.kafka.connect.json.JsonConverter</sub>      | N/A                | Converter for values stored in Kafka                       |
| host                   | STRING  | localhost:9009                                              | N/A                | Host and port where QuestDB server is running              |
| table                  | STRING  | my_table                                                    | Same as Topic name | Target table in QuestDB                                    |
| key.prefix             | STRING  | from_key                                                    | key                | Prefix for key fields                                      | 
| value.prefix           | STRING  | from_value                                                  | N/A                | Prefix for value fields                                    |
| skip.unsupported.types | BOOLEAN | false                                                       | false              | Skip unsupported types                                     |
| timestamp.field.name   | STRING  | pickup_time                                                 | N/A                | Designated timestamp field name                            |
| timestamp.units        | STRING  | micros                                                      | auto               | Designated timestamp field units                           |
| include.key            | BOOLEAN | false                                                       | true               | Include message key in target table                        |
| symbols                | STRING  | instrument,stock                                            | N/A                | Comma separated list of columns that should be symbol type |
| username               | STRING  | user1                                                       | admin              | User name for QuestDB. Used only when token is non-empty   |
| token                  | STRING  | <sub>QgHCOyq35D5HocCMrUGJinEsjEscJlCp7FZQETH21Bw</sub>      | N/A                | Token for QuestDB authentication                           |
| tls                    | BOOLEAN | true                                                        | false              | Use TLS for QuestDB connection                             |

## Supported serialization formats
The connector does not do data deserialization on its own. It relies on Kafka Connect converters to deserialize data. It's been tested predominantly with JSON, but it should work with any converter, including Avro. Converters can be configured using `key.converter` and `value.converter` options, see the table above. 

## How it works
The connector reads data from Kafka topics and writes it to QuestDB tables. The connector converts each field in the Kafka message to a column in the QuestDB table. Structs and maps are flatted into columns. 

Example:
Consider the following Kafka message:
```json
{
    "firstname": "John",
    "lastname": "Doe",
    "age": 30,
    "address": {
      "street": "Main Street",
      "city": "New York"
    }
}
```
The connector will create a table with the following columns:

| firstname <sub>string</sub> | lastname <sub>string</sub> | age <sub>long</sub> | address_street <sub>string</sub> | address_city <sub>string</sub> |
|-----------------------------|----------------------------|---------------------|----------------------------------|--------------------------------|
| John                        | Doe                        | 30                  | Main Street                      | New York                       |

## Designated Timestamps
The connector supports designated timestamps. If a message contains a field with a timestamp, the connector can use it as a timestamp for the row. The field name must be configured using `timestamp.field.name` option. The field must either a plain integer or being labelled as a timestamp in a message schema. When it's a plain integer, the connector will autodetect its units. This works for timestamps after 4/26/1970, 5:46:40 PM. The units can be also configured explicitly using `timestamp.units` option. Supported configuration values are `nanos`, `micros`, `millis` and `auto`. 

## QuestDB Symbol Type
QuestDB supports a special type called [Symbol](https://questdb.io/docs/concept/symbol/). This connector never creates a column with a type `SYMBOL`. Instead, it creates a column with a type `STRING`. If you want to use `SYMBOL` type, you can pre-create a table in QuestDB and use it as a target table.

## Target Table Considerations
When a target table does not exist in QuestDB then it will be automatically created when a first row arrives. This is recommended approach for development and testing.

In production, it's recommended to [create tables manually via SQL](https://questdb.io/docs/reference/sql/create-table/). This gives you more control over the table schema, allow per-table partitioning, creating indexes, etc.

## FAQ
<b>Q</b>: Does this connector work with Schema Registry?
<br/>
<b>A</b>: The Connector does not care about serialization strategy used. It relies on Kafka Connect converters to deserialize data. Converters can be configured using `key.converter` and `value.converter` options, see the configuration section.

<b>Q</b>: I'm getting this error: `org.apache.kafka.connect.errors.DataException: JsonConverter with schemas.enable requires "schema" and "payload" fields and may not contain additional fields. If you are trying to deserialize plain JSON data, set schemas.enable=false in your converter configuration.`
<br/>
<b>A</b>: This error means that the connector is trying to deserialize data using a converter that expects a schema. The connector does not use schemas, so you need to configure the converter to not expect a schema. For example, if you are using JSON converter, you need to set `value.converter.schemas.enable=false` or `key.converter.schemas.enable=false` in the connector configuration. 

<b>Q</b>: Does this connector work with Debezium?
<br/>
<b>A</b>: Yes, it's been tested with Debezium as a source. Bear in mind that QuestDB is meant to be used as append-only database hence updates should be translated as new inserts. The connector supports Debezium's `ExtractNewRecordState` transformation to extract the new state of the record. The transform by default drops DELETE events so no need to handle it explicitly.

<b>Q</b>: How I can select which fields to include in the target table?
<br/>
<b>A</b>: Use the ReplaceField transformation to remove unwanted fields. For example, if you want to remove the `address` field from the example above, you can use the following configuration:
```json
{
  "name": "questdb-sink",
  "config": {
    "connector.class": "io.questdb.kafka.QuestDBSinkConnector",
    "host": "localhost:9009",
    "topics": "Orders",
    "table": "orders_table",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "transforms": "unwrap,removeAddress",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.removeAddress.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
    "transforms.removeAddress.blacklist": "address",
    "include.key": "false"
  }
}
```
See [ReplaceField documentation](https://docs.confluent.io/platform/current/connect/transforms/replacefield.html#replacefield) for more details.

<b>Q</b>: I need to run Kafka Connect on Java 8, but the connector says it requires Java 11. What should I do?
<br/>
<b>A</b>: The Kafka Connect-specific part of the connectors works with Java 8. The requirement for Java 11 is coming from QuestDB client itself. The zip archive contains 2 JARs: `questdb-kafka-connector-<version>.jar` and `questdb-<version>.jar`. You can use replace the latter with `questdb-<version>-jdk8.jar` from the [Maven central](https://mvnrepository.com/artifact/org.questdb/questdb/6.5.3-jdk8). Bear in mind this setup is not officially supported and you may encounter issues. If you do, please report them to us.

<b>Q</b>: QuestDB is a time-series database, how does it fit into Change Data Capture via Debezium?
<br/>
<b>A</b>: QuestDB works with Debezium just great! This is the recommended pattern: Transactional applications use a relational database to store the current state of the data. QuestDB is used to store the history of changes. Example: Imagine you have a Postgres table with the most recent stock prices. Whenever a stock price changes an application updates the Postgres table. Debezium capture each UPDATE/INSERT and pushes it as an event to Kafka. Kafka Connect QuestDB connector reads the events and inserts them into QuestDB. This way Postgres will have the most recent stock prices and QuestDB will have the history of changes. You can use QuestDB to build a dashboard with the most recent stock prices and a chart with the history of changes. 

## License
This project is licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.