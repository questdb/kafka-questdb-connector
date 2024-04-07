# Sample Project: Kafka -> QuestDB
## What does the sample project do?
This code sample show the simplest usage of the Kafka QuestDB connector. It uses a simple node.js application to generate random data and send it to a Kafka topic. The connector is configured to read from the topic and write the data to a QuestDB table.

## Prerequisites
- Git
- Working Docker environment, including docker-compose
- Internet access to download dependencies

The project was tested on MacOS with M1, but it should work on other platforms too. Please open a new issue if it's not working for you.

## Usage:
1. Clone this repository via `git clone https://github.com/questdb/kafka-questdb-connector.git`
2. `cd kafka-questdb-connector/kafka-questdb-connector-samples/faker/` to enter the directory with this sample.
3. Run `docker compose build` to build a docker image with the sample project.
4. Run `docker compose up` to start the node.js producer, Apache Kafka and QuestDB containers.
5. The previous command will generate a lot of log messages. Eventually logging should cease. This means both Apache Kafka and QuestDB are running. The last log message should contain the following text: `Session key updated`
6. Execute the following command in shell: 
    ```shell
    $ curl -X POST -H "Content-Type: application/json" -d '{"name":"questdb-connect","config":{"topics":"People","connector.class":"io.questdb.kafka.QuestDBSinkConnector","tasks.max":"1","key.converter":"org.apache.kafka.connect.storage.StringConverter","value.converter":"org.apache.kafka.connect.json.JsonConverter","value.converter.schemas.enable":"false","client.conf.string":"http::addr=questdb;", "timestamp.field.name": "birthday", "transforms":"convert_birthday","transforms.convert_birthday.type":"org.apache.kafka.connect.transforms.TimestampConverter$Value","transforms.convert_birthday.target.type":"Timestamp","transforms.convert_birthday.field":"birthday","transforms.convert_birthday.format": "yyyy-MM-dd'"'"'T'"'"'HH:mm:ss.SSSX"}}' localhost:8083/connectors
    ```
7. The command above will create a new Kafka connector that will read data from the `People` topic and write it to a QuestDB table called `People`. The connector will also convert the `birthday` field to a timestamp.
8. Go to the QuestDB console running at http://localhost:19000 and run `select * from 'People';` and you should see some rows.
9. Congratulations! You have successfully created a Kafka connector that reads data from a Kafka topic and writes it to a QuestDB table!

## How does it work?
The sample project consists of 3 components:
1. [Node.js](index.js) application that generates random JSON data and sends it to a Kafka topic. Each generated JSON looks similar to this:
    ```json
    {"firstname":"John","lastname":"Doe","birthday":"1970-01-12T10:25:12.052Z"}
    ```
2. [Docker-compose](docker-compose.yml) file that starts the node.js application, Apache Kafka, Kafka Connect and QuestDB containers.
3. Kafka Connect configuration that defines the connector that reads data from the Kafka topic and writes it to a QuestDB table. The configuration is submitted into a running Kafka Connect cluster via the REST API.

The Kafka Connect configuration looks complex, but it's quite simple. Let's have a closer look. This is how the `curl` command looks like:
```shell
$ curl -X POST -H "Content-Type: application/json" -d '{"name":"questdb-connect","config":{"topics":"People","connector.class":"io.questdb.kafka.QuestDBSinkConnector","tasks.max":"1","key.converter":"org.apache.kafka.connect.storage.StringConverter","value.converter":"org.apache.kafka.connect.json.JsonConverter","value.converter.schemas.enable":"false","client.conf.string":"http::addr=questdb;", "timestamp.field.name": "birthday", "transforms":"convert_birthday","transforms.convert_birthday.type":"org.apache.kafka.connect.transforms.TimestampConverter$Value","transforms.convert_birthday.target.type":"Timestamp","transforms.convert_birthday.field":"birthday","transforms.convert_birthday.format": "yyyy-MM-dd'"'"'T'"'"'HH:mm:ss.SSSX"}}' localhost:8083/connectors
```

It uses `curl` to submit a following JSON to Kafka Connect:
```json
{
  "name": "questdb-connect",
  "config": {
    "topics": "People",
    "connector.class": "io.questdb.kafka.QuestDBSinkConnector",
    "tasks.max": "1",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "client.conf.string": "http::addr=questdb;",
    "timestamp.field.name": "birthday",
    "transforms": "convert_birthday",
    "transforms.convert_birthday.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
    "transforms.convert_birthday.target.type": "Timestamp",
    "transforms.convert_birthday.field": "birthday",
    "transforms.convert_birthday.format": "yyyy-MM-dd'T'HH:mm:ss.SSSX"
  }
}
```
Most of the fields are self-explanatory. The `transforms` field is a bit more complex. It defines a [Kafka Connect transformation](https://docs.confluent.io/platform/current/connect/transforms/index.html) that converts the `birthday` string field to a timestamp. The `transforms.convert_birthday.format` field defines the format of the date. The format is a [Java SimpleDateFormat](https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html) pattern. The `transforms.convert_birthday.field` field defines the name of the field that should be converted. The `transforms.convert_birthday.target.type` field defines the type of the field after the conversion. In this case, it's a timestamp.

The other potentially non-obvious configuration elements are:
1. `"value.converter.schemas.enable": "false"` It disables the schema support in the Kafka Connect JSON converter. The sample project doesn't use schemas.
2. `"client.conf.string": "http::addr=questdb;"` It configures QuestDB client to use the HTTP transport and connect to a hostname `questdb`. The hostname is defined in the [docker-compose.yml](docker-compose.yml) file.
3. `"timestamp.field.name": "birthday"` It defines the name of the field that should be used as a timestamp. It uses the field that was converted by the Kafka Connect transformation described above.
4. The ugly value in `"transforms.convert_birthday.format": "yyyy-MM-dd'"'"'T'"'"'HH:mm:ss.SSSX"`. This part looks funny: `'"'"'T'"'"'`. In fact, it's a way to submit an apostrophe via shell which uses apostrophes to define strings. The apostrophe is required to escape the `T` character in the date format. The date format is `yyyy-MM-dd'T'HH:mm:ss.SSSX`. If you know a better way to submit the same JSON then please [open a new issue](https://github.com/questdb/kafka-questdb-connector/issues/new). 