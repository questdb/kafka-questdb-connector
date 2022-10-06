# QuestDB Sink connector for Apache Kafka
Sink data from [Apache Kafka](https://kafka.apache.org/) pipelines to [QuestDB](https://questdb.io/).

The connector implements Apache Kafka [Sink Connector API](https://kafka.apache.org/documentation/#connect_development).

## Usage with Kafka Connect
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

| Name                   | Type    | Example                                          | Default            | Meaning                                       |
|------------------------|---------|--------------------------------------------------|--------------------|-----------------------------------------------|
| topics                 | STRING  | orders                                           | N/A                | Topics to read from                           |
| key.converter          | STRING  | org.apache.kafka.connect.storage.StringConverter | N/A                | Converter for keys stored in Kafka            |
| value.converter        | STRING  | org.apache.kafka.connect.json.JsonConverter      | N/A                | Converter for values stored in Kafka          |
| host                   | STRING  | localhost:9009                                   | N/A                | Host and port where QuestDB server is running |
| table                  | STRING  | my_table                                         | Same as Topic name | Target table in QuestDB                       |
| key.prefix             | STRING  | from_key                                         | key                | Prefix for key fields                         | 
| value.prefix           | STRING  | from_value                                       | N/A                | Prefix for value fields                       |
| skip.unsupported.types | BOOLEAN | false                                            | false              | Skip unsupported types                        |
| timestamp.field.name   | STRING  | pickup_time                                      | N/A                | Designated timestamp field name               |