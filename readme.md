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
    "connector.class": "io.questdb.kafka.connect.QuestDbSinkConnector",
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
|-------------------------------|------------------------------|-----------------------|------------------------------------|----------------------------------|
| John                          | Doe                          | 30                    | Main Street                        | New York                         |

## Designated Timestamps
The connector supports designated timestamps. If the message contains a field with a timestamp, the connector can use it as a timestamp for the row. The field name must be configured using `timestamp.field.name` option. The field must either a simple number or a timestamp. When it's a simple number, the connector will interpret it as a Unix timestamp in milliseconds.