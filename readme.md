# QuestDB Sink connector for Apache Kafka
The connector reads data from Kafka topics and writes it to [QuestDB](https://questdb.io/) tables.
The connector implements Apache Kafka [Sink Connector API](https://kafka.apache.org/documentation/#connect_development).

## Usage with Kafka Connect
This guide assumes you are already familiar with Apache Kafka and Kafka Connect. If you are not then watch this [excellent video](https://www.youtube.com/watch?v=Jkcp28ki82k) or check our [sample projects](kafka-questdb-connector-samples).
- Unpack connector ZIP into Kafka Connect `./plugin/` directory.
- Start Kafka Connect
- Create a connector configuration file:
```json
{
  "name": "questdb-sink",
  "config": {
    "connector.class": "io.questdb.kafka.QuestDBSinkConnector",
    "host": "localhost:9009",
    "topics": "Orders",
    "table": "orders_table"
  }
}
```

## Configuration
The connector supports following Options:

| Name                   | Type    | Example                                                     | Default            | Meaning                                       |
|------------------------|---------|-------------------------------------------------------------|--------------------|-----------------------------------------------|
| topics                 | STRING  | orders                                                      | N/A                | Topics to read from                           |
| key.converter          | STRING  | <sub>org.apache.kafka.connect.storage.StringConverter</sub> | N/A                | Converter for keys stored in Kafka            |
| value.converter        | STRING  | <sub>org.apache.kafka.connect.json.JsonConverter</sub>      | N/A                | Converter for values stored in Kafka          |
| host                   | STRING  | localhost:9009                                              | N/A                | Host and port where QuestDB server is running |
| table                  | STRING  | my_table                                                    | Same as Topic name | Target table in QuestDB                       |
| key.prefix             | STRING  | from_key                                                    | key                | Prefix for key fields                         | 
| value.prefix           | STRING  | from_value                                                  | N/A                | Prefix for value fields                       |
| skip.unsupported.types | BOOLEAN | false                                                       | false              | Skip unsupported types                        |
| timestamp.field.name   | STRING  | pickup_time                                                 | N/A                | Designated timestamp field name               |
| include.key            | BOOLEAN | false                                                       | true               | Include message key in target table           |

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
The connector supports designated timestamps. If the message contains a field with a timestamp, the connector can use it as a timestamp for the row. The field name must be configured using `timestamp.field.name` option. The field must either a simple number or a timestamp. When it's a simple number, the connector will interpret it as a Unix timestamp in milliseconds.

## QuestDB Symbol Type
QuestDB supports a special type called [Symbol](https://questdb.io/docs/concept/symbol/). This connector never creates a column with a type `SYMBOL`. Instead, it creates a column with a type `STRING`. If you want to use `SYMBOL` type, you can pre-create a table in QuestDB and use it as a target table.

## Target Table Considerations
When a target table does not exist in QuestDB then it will be automatically created when a first row arrives. This is recommended approach for development and testing.

In production, it's recommended to [create tables manually via SQL](https://questdb.io/docs/reference/sql/create-table/). This gives you more control over the table schema and allows using the symbol type, create indexes, etc.

## FAQ
Q: Does this connector work with Schema Registry?

A: The Connector does not care about serialization strategy used. It relies on Kafka Connect converters to deserialize data. Converters can be configured using `key.converter` and `value.converter` options, see the configuration section.


Q: I'm getting this error: `org.apache.kafka.connect.errors.DataException: JsonConverter with schemas.enable requires "schema" and "payload" fields and may not contain additional fields. If you are trying to deserialize plain JSON data, set schemas.enable=false in your converter configuration.`

A: This error means that the connector is trying to deserialize data using a converter that expects a schema. The connector does not use schemas, so you need to configure the converter to not expect a schema. For example, if you are using JSON converter, you need to set `value.converter.schemas.enable=false` or `key.converter.schemas.enable=false` in the connector configuration. 

Q: Does this connector work with Debezium?

A: Yes, it's been tested with Debezium as a source. Bear in mind that QuestDB is meant to be used as append-only database hence updates should be translated as new inserts.