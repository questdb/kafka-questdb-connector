# Sample Project: Streaming trades from Kafka to QuestDB

**Start here.** This is the simplest sample in this repository and the first step of the
[learning path](../readme.md).

## What does the sample do?
A small Node.js application generates a live stream of trades, one JSON message per trade, and sends them to the
Kafka topic `trades`. The QuestDB Kafka connector reads the topic and writes every trade as a row of the QuestDB
table `trades`. The table has the same shape as the `trades` table on [demo.questdb.io](https://demo.questdb.io),
so any query you know from the demo works here too.

A message looks like this:
```json
{"trade_id":1789461925365001,"symbol":"BTC-USDT","side":"buy","price":76853.5,"amount":0.004332,"timestamp":"2026-09-15T08:45:25.365Z"}
```

## Prerequisites
- Git
- Working Docker environment, including Docker Compose
- Internet access to download dependencies

## Running the sample
1. Clone this repository via `git clone https://github.com/questdb/kafka-questdb-connector.git`
2. `cd kafka-questdb-connector/kafka-questdb-connector-samples/faker/` to enter the directory with this sample.
3. Run `docker compose up --build --wait`. It builds the producer and Kafka Connect images, starts Kafka, Kafka
   Connect, QuestDB and the producer in the background, and returns once Kafka Connect is ready to accept
   connectors. The first run takes a few minutes because of the downloads.
4. Submit the connector configuration from [connector.json](connector.json) to Kafka Connect:
    ```shell
    curl -X POST -H "Content-Type: application/json" -d @connector.json localhost:8083/connectors
    ```
   Kafka Connect echoes the configuration it accepted.
5. Go to the QuestDB web console at http://localhost:19000 and run:
    ```sql
    select * from trades;
    ```
   The producer has been streaming since step 3, so the table already holds thousands of rows and grows by about
   50 trades per second. If the table does not exist yet, wait a few seconds and run the query again.
6. Run `docker compose down` when you are done.

That is the whole pipeline. The rest of this page explains what you just ran and then goes deeper into
timestamps and delivery guarantees.

## What just happened
The sample consists of 3 parts:

1. [index.js](index.js), the producer. It walks the price of a few crypto pairs randomly, builds one JSON document
   per trade and sends it to Kafka with the symbol as the message key, so all trades of one symbol stay in order.
   Every trade carries a `trade_id` that is unique even across producer restarts; the
   [delivery guarantees](#delivery-guarantees) section uses it.
2. [docker-compose.yml](docker-compose.yml). It starts four containers: Kafka, Kafka Connect with the QuestDB
   connector installed (see [Dockerfile-Connect](Dockerfile-Connect)), QuestDB, and the producer. Kafka and Kafka
   Connect have healthchecks, which is what `docker compose up --wait` waits for. To follow the producer's log,
   run `docker compose logs -f producer`.
3. [connector.json](connector.json), the connector configuration. Kafka Connect has a REST API and step 4 posted
   the file to it.

### The connector configuration
```json
{
  "name": "questdb-connect",
  "config": {
    "connector.class": "io.questdb.kafka.QuestDBSinkConnector",
    "tasks.max": "1",
    "topics": "trades",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "client.conf.string": "ws::addr=questdb:9000;",
    "include.key": "false",
    "symbols": "symbol,side",
    "doubles": "price,amount",
    "timestamp.field.name": "timestamp",
    "timestamp.string.format": "yyyy-MM-ddTHH:mm:ss.SSSZ"
  }
}
```

- `"topics": "trades"` - the Kafka topic to read. The QuestDB table gets the same name; add a `"table"` option to
  choose a different one.
- `"value.converter"` and `"value.converter.schemas.enable": "false"` - the messages are plain JSON documents
  without an embedded schema.
- `"key.converter"` and `"include.key": "false"` - the message key is a plain string, the symbol. It is not stored
  as a column because `symbol` is already a field of the message.
- `"client.conf.string": "ws::addr=questdb:9000;"` - how to reach QuestDB. `questdb` is the hostname of the
  QuestDB container in [docker-compose.yml](docker-compose.yml). `ws::` selects the QuestDB WebSocket Protocol
  (QWP) transport, see [Why QWP?](#why-qwp) below.
- `"symbols": "symbol,side"` - store these low-cardinality string fields as QuestDB
  [SYMBOL](https://questdb.com/docs/concept/symbol/) columns instead of VARCHAR. Symbols are interned, so they
  take less space and filter faster.
- `"doubles": "price,amount"` - JSON has no numeric types. Without this option a whole number such as `100` would
  arrive as a long, and because the connector creates the table from the first message it sees, a column could end
  up with the wrong type. This pins both columns to DOUBLE.
- `"timestamp.field.name"` and `"timestamp.string.format"` - which field is the
  [designated timestamp](https://questdb.com/docs/concept/designated-timestamp/) of the table and how to parse it.
  The next section has the details.

The connector created the table when the first message arrived. `show columns from trades;` in the web console
lists `symbol` and `side` as SYMBOL, `price` and `amount` as DOUBLE, `trade_id` as LONG and `timestamp` as the
designated TIMESTAMP.

## Choosing the designated timestamp
Every QuestDB table used for time-series queries has one designated timestamp column. The table is ordered by
it, partitioned by it, and `SAMPLE BY`, `LATEST ON` and `ASOF JOIN` all use it. The connector supports several
ways to fill it:

1. **A field of the message**, as in this sample: `"timestamp.field.name": "timestamp"`. The producer sends the
   time of the trade as an ISO-8601 string with millisecond precision, so `"timestamp.string.format"` tells the
   connector how to parse it. Numeric fields work without a format; the connector detects seconds, millis, micros
   or nanos from the magnitude, or you set `"timestamp.units"` explicitly.
2. **The Kafka record timestamp**, with `"timestamp.kafka.native": "true"`. That is the time the message was
   written to Kafka. The [confluent-docker-images](../confluent-docker-images) sample uses it.
3. **Nothing.** Without either option QuestDB stamps each row with its arrival time. Avoid this for anything but
   experiments: a replayed message gets a new timestamp, so it can never be deduplicated.

The [connector documentation](https://questdb.com/docs/third-party-tools/kafka/questdb-kafka/) describes all the
options, including timestamps composed from several fields.

## Delivery guarantees
### Why QWP?
This sample uses `ws::`, the QuestDB WebSocket Protocol (QWP) transport. The older `http::` and `tcp::`
transports keep working, but QWP is the better default when you care about not losing rows:

1. **Kafka offsets are committed only after QuestDB acknowledges the data.** An offset advances only once QuestDB
   has confirmed the rows behind it, so if Kafka Connect or QuestDB dies mid-flight the sink resumes from the last
   acknowledged offset.
2. **It holds throughput far better over a high-latency link.** QWP keeps writes pipelined instead of waiting for
   a response before sending more, so a Kafka Connect worker in a different region or cloud than QuestDB is not
   throttled by the round-trip time the way request/response HTTP is.

QWP needs QuestDB 10 or newer and Kafka Connect 3.6 or newer. This sample satisfies both:
[docker-compose.yml](docker-compose.yml) pins `questdb/questdb:10.0.1` and `confluentinc/cp-kafka-connect:7.8.0`,
which ships Kafka 3.8.

### Delivery is at least once
QWP guarantees that acknowledged data is not lost. It does not guarantee that data is written exactly once: a
reconnect or a rejected batch can replay rows QuestDB already holds, so duplicates are possible.

QuestDB removes duplicates for you if the table has
[DEDUP UPSERT KEYS](https://questdb.com/docs/concept/deduplication/): a row whose keys match an existing row
replaces it instead of being added. The keys must identify one trade, and the designated timestamp must be one of
them. The producer's `trade_id` is unique per trade, so these two columns are the key. Enable deduplication on the
running table in the web console:
```sql
ALTER TABLE trades DEDUP ENABLE UPSERT KEYS(timestamp, trade_id);
```
From now on a replayed trade overwrites itself. To see it in action, kill Kafka Connect while the producer keeps
streaming, so it has no chance to commit its offsets, and start it again:
```shell
docker compose kill connect && docker compose start connect
```
Kafka Connect re-reads the topic from the last committed offset. With QWP an offset is committed only after
QuestDB acknowledged the rows behind it, so everything it re-reads either never reached QuestDB or is already
in the table. Count rows and distinct trades once the connector is back:
```sql
SELECT count() AS rows, count_distinct(trade_id) AS trades FROM trades;
```
The two numbers match. Without deduplication `rows` would be larger by the number of replayed messages.

In a real deployment, create the table upfront with `DEDUP UPSERT KEYS` over a column set that identifies an
event, before the connector starts, whenever duplicates are not acceptable. `ALTER TABLE ... DEDUP ENABLE` only
affects rows written after it ran.

## Explore further
The table has the same columns as the `trades` table on [demo.questdb.io](https://demo.questdb.io), so the
queries from the demo apply. The last trade of every symbol:
```sql
SELECT * FROM trades LATEST ON timestamp PARTITION BY symbol;
```
One-minute candles with volume for one pair:
```sql
SELECT timestamp, symbol,
       first(price) AS open, max(price) AS high, min(price) AS low, last(price) AS close,
       sum(amount) AS volume
FROM trades
WHERE symbol = 'BTC-USDT'
SAMPLE BY 1m;
```

## Next step
[protobuf-schema-registry](../protobuf-schema-registry): the same trade stream, but as typed Protobuf messages
with the Confluent Schema Registry.

## Bugs and Feedback
For bugs, questions and discussions please use the [Github Issues](https://github.com/questdb/kafka-questdb-connector/issues/new)
