package io.questdb.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

@Testcontainers
public final class QuestDBSinkConnectorEmbeddedTest {

    private EmbeddedConnectCluster connect;
    private Converter converter;

    private String topicName;

    @Container
    private static GenericContainer<?> questDBContainer = newQuestDbConnector();

    private static GenericContainer<?> newQuestDbConnector() {
        return newQuestDbConnector(null, null);
    }

    private static GenericContainer<?> newQuestDbConnector(Integer httpPort, Integer ilpPort) {
        FixedHostPortGenericContainer<?> selfGenericContainer = new FixedHostPortGenericContainer<>("questdb/questdb:6.6.1");
        if (httpPort != null) {
            selfGenericContainer.withFixedExposedPort(httpPort, QuestDBUtils.QUESTDB_HTTP_PORT);
        } else {
            selfGenericContainer.addExposedPort(QuestDBUtils.QUESTDB_HTTP_PORT);
        }
        if (ilpPort != null) {
            selfGenericContainer.withFixedExposedPort(ilpPort, QuestDBUtils.QUESTDB_ILP_PORT);
        } else {
            selfGenericContainer.addExposedPort(QuestDBUtils.QUESTDB_ILP_PORT);
        }
        selfGenericContainer.setWaitStrategy(new LogMessageWaitStrategy().withRegEx(".*server-main enjoy.*"));
        return selfGenericContainer.withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("questdb")))
                .withEnv("QDB_CAIRO_COMMIT_LAG", "100")
                .withEnv("JAVA_OPTS", "-Djava.locale.providers=JRE,SPI");
    }

    @BeforeEach
    public void setUp() {
        topicName = ConnectTestUtils.newTopicName();
        JsonConverter jsonConverter = new JsonConverter();
        jsonConverter.configure(singletonMap(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName()));
        converter = jsonConverter;

        connect = new EmbeddedConnectCluster.Builder()
                .name("questdb-connect-cluster")
                .build();

        connect.start();
    }

    @AfterEach
    public void tearDown() {
        connect.stop();
    }

    @Test
    public void testSmoke() {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName);
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);
        Schema schema = SchemaBuilder.struct().name("com.example.Person")
                .field("firstname", Schema.STRING_SCHEMA)
                .field("lastname", Schema.STRING_SCHEMA)
                .field("age", Schema.INT8_SCHEMA)
                .build();

        Struct struct = new Struct(schema)
                .put("firstname", "John")
                .put("lastname", "Doe")
                .put("age", (byte) 42);

        connect.kafka().produce(topicName, "key", new String(converter.fromConnectData(topicName, schema, struct)));

        QuestDBUtils.assertSqlEventually(questDBContainer, "\"firstname\",\"lastname\",\"age\"\r\n"
                        + "\"John\",\"Doe\",42\r\n",
                "select firstname,lastname,age from " + topicName);
    }

    @Test
    public void testDeadLetterQueue_wrongJson() {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName);
        props.put("value.converter.schemas.enable", "false");
        props.put("errors.deadletterqueue.topic.name", "dlq");
        props.put("errors.deadletterqueue.topic.replication.factor", "1");
        props.put("errors.tolerance", "all");
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);
        connect.kafka().produce(topicName, "key", "{\"not valid json}");
        connect.kafka().produce(topicName, "key", "{\"firstname\":\"John\",\"lastname\":\"Doe\",\"age\":42}");

        QuestDBUtils.assertSqlEventually(questDBContainer, "\"firstname\",\"lastname\",\"age\"\r\n"
                        + "\"John\",\"Doe\",42\r\n",
                "select firstname,lastname,age from " + topicName);

        ConsumerRecords<byte[], byte[]> fetchedRecords = connect.kafka().consume(1, 5000, "dlq");
        Assertions.assertEquals(1, fetchedRecords.count());

        ConsumerRecord<byte[], byte[]> dqlRecord = fetchedRecords.iterator().next();
        Assertions.assertEquals("{\"not valid json}", new String(dqlRecord.value()));
    }

    @Test
    public void testSymbol() {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName);
        props.put(QuestDBSinkConnectorConfig.SYMBOL_COLUMNS_CONFIG, "firstname,lastname");
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);
        Schema schema = SchemaBuilder.struct().name("com.example.Person")
                .field("firstname", Schema.STRING_SCHEMA)
                .field("lastname", Schema.STRING_SCHEMA)
                .field("age", Schema.INT8_SCHEMA)
                .build();

        Struct struct = new Struct(schema)
                .put("firstname", "John")
                .put("lastname", "Doe")
                .put("age", (byte) 42);

        connect.kafka().produce(topicName, "key", new String(converter.fromConnectData(topicName, schema, struct)));

        QuestDBUtils.assertSqlEventually(questDBContainer, "\"firstname\",\"lastname\",\"age\"\r\n"
                        + "\"John\",\"Doe\",42\r\n",
                "select firstname,lastname,age from " + topicName);
    }

    @Test
    public void testRetrying_badDataStopsTheConnectorEventually() throws Exception {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName);
        props.put("value.converter.schemas.enable", "false");
        props.put(QuestDBSinkConnectorConfig.RETRY_BACKOFF_MS, "1000");
        props.put(QuestDBSinkConnectorConfig.MAX_RETRIES, "5");
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);

        // creates a record with 'age' as long
        connect.kafka().produce(topicName, "key", "{\"firstname\":\"John\",\"lastname\":\"Doe\",\"age\":42}");

        QuestDBUtils.assertSqlEventually(questDBContainer, "\"firstname\",\"lastname\",\"age\"\r\n"
                        + "\"John\",\"Doe\",42\r\n",
                "select firstname,lastname,age from " + topicName);

        for (int i = 0; i < 50; i++) {
            // injects a record with 'age' as string
            connect.kafka().produce(topicName, "key", "{\"firstname\":\"John\",\"lastname\":\"Doe\",\"age\":\"str\"}");

            try {
                ConnectTestUtils.assertConnectorTaskState(connect, ConnectTestUtils.CONNECTOR_NAME, AbstractStatus.State.FAILED);
                return; // ok, the connector already failed, good, we are done
            } catch (AssertionError e) {
                // not yet, maybe next-time
            }
            Thread.sleep(1000);
        }
        fail("The connector should have failed by now");
    }

    @Test
    public void testRetrying_recoversFromInfrastructureIssues() throws Exception {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName);
        props.put("value.converter.schemas.enable", "false");
        props.put(QuestDBSinkConnectorConfig.RETRY_BACKOFF_MS, "1000");
        props.put(QuestDBSinkConnectorConfig.MAX_RETRIES, "40");
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);

        connect.kafka().produce(topicName, "key1", "{\"firstname\":\"John\",\"lastname\":\"Doe\",\"age\":42}");


        QuestDBUtils.assertSqlEventually(questDBContainer, "\"firstname\",\"lastname\",\"age\"\r\n"
                        + "\"John\",\"Doe\",42\r\n",
                "select firstname,lastname,age from " + topicName);

        // we need to get mapped ports, becasue we are going to kill the container and restart it again
        // and we need the same mapping otherwise the Kafka connect will not be able to re-connect to it
        Integer httpPort = questDBContainer.getMappedPort(QuestDBUtils.QUESTDB_HTTP_PORT);
        Integer ilpPort = questDBContainer.getMappedPort(QuestDBUtils.QUESTDB_ILP_PORT);
        questDBContainer.stop();
        // insert a few records while the QuestDB is down
        for (int i = 0; i < 10; i++) {
            connect.kafka().produce(topicName, "key2", "{\"firstname\":\"John\",\"lastname\":\"Doe\",\"age\":42}");
            Thread.sleep(500);
        }

        // restart QuestDB
        questDBContainer = newQuestDbConnector(httpPort, ilpPort);
        questDBContainer.start();
        for (int i = 0; i < 50; i++) {
            connect.kafka().produce(topicName, "key3", "{\"firstname\":\"John\",\"lastname\":\"Doe\",\"age\":" + i + "}");
            Thread.sleep(100);
        }

        QuestDBUtils.assertSqlEventually(questDBContainer, "\"firstname\",\"lastname\",\"age\"\r\n"
                        + "\"John\",\"Doe\",49\r\n",
                "select firstname,lastname,age from " + topicName + " where age = 49");
    }

    @Test
    public void testEmptyCollection_wontFailTheConnector() {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName);
        // filter out all message
        props.put("transforms", "drop");
        props.put("transforms.drop.type", "org.apache.kafka.connect.transforms.Filter");


        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);
        Schema schema = SchemaBuilder.struct().name("com.example.Person")
                .field("firstname", Schema.STRING_SCHEMA)
                .field("lastname", Schema.STRING_SCHEMA)
                .field("age", Schema.INT8_SCHEMA)
                .build();

        Struct struct = new Struct(schema)
                .put("firstname", "John")
                .put("lastname", "Doe")
                .put("age", (byte) 42);

        connect.kafka().produce(topicName, "key", new String(converter.fromConnectData(topicName, schema, struct)));

        int durationMs = 10_000;
        long deadline = System.currentTimeMillis() + durationMs;
        while (System.currentTimeMillis() < deadline) {
            ConnectTestUtils.assertConnectorTaskState(connect, ConnectTestUtils.CONNECTOR_NAME, AbstractStatus.State.RUNNING);
        }
    }

    @Test
    public void testSymbol_withAllOtherILPTypes() {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName);
        props.put(QuestDBSinkConnectorConfig.SYMBOL_COLUMNS_CONFIG, "firstname");
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);
        Schema schema = SchemaBuilder.struct().name("com.example.Person")
                .field("firstname", Schema.STRING_SCHEMA)
                .field("lastname", Schema.STRING_SCHEMA)
                .field("age", Schema.INT8_SCHEMA)
                .field("vegan", Schema.BOOLEAN_SCHEMA)
                .field("height", Schema.FLOAT64_SCHEMA)
                .field("birth", Timestamp.SCHEMA)
                .build();

        java.util.Date birth = new Calendar.Builder()
                .setTimeZone(TimeZone.getTimeZone("UTC"))
                .setDate(2022, 9, 23) // note: month is 0-based
                .setTimeOfDay(13, 53, 59, 123)
                .build().getTime();
        Struct p1 = new Struct(schema)
                .put("firstname", "John")
                .put("lastname", "Doe")
                .put("age", (byte) 42)
                .put("vegan", true)
                .put("height", 1.80)
                .put("birth", birth);

        birth = new Calendar.Builder()
                .setTimeZone(TimeZone.getTimeZone("UTC"))
                .setDate(2021, 9, 23) // note: month is 0-based
                .setTimeOfDay(13, 53, 59, 123)
                .build().getTime();
        Struct p2 = new Struct(schema)
                .put("firstname", "Jane")
                .put("lastname", "Doe")
                .put("age", (byte) 41)
                .put("vegan", false)
                .put("height", 1.60)
                .put("birth", birth);

        connect.kafka().produce(topicName, "p1", new String(converter.fromConnectData(topicName, schema, p1)));
        connect.kafka().produce(topicName, "p2", new String(converter.fromConnectData(topicName, schema, p2)));

        QuestDBUtils.assertSqlEventually(questDBContainer, "\"firstname\",\"lastname\",\"age\",\"vegan\",\"height\",\"birth\"\r\n"
                        + "\"John\",\"Doe\",42,true,1.8,\"2022-10-23T13:53:59.123000Z\"\r\n"
                        + "\"Jane\",\"Doe\",41,false,1.6,\"2021-10-23T13:53:59.123000Z\"\r\n",
                "select firstname,lastname,age,vegan,height,birth from " + topicName);
    }

    @Test
    public void testUpfrontTable_withSymbols() {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName);
        props.put(QuestDBSinkConnectorConfig.SYMBOL_COLUMNS_CONFIG, "firstname,lastname");
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);
        Schema schema = SchemaBuilder.struct().name("com.example.Person")
                .field("firstname", Schema.STRING_SCHEMA)
                .field("lastname", Schema.STRING_SCHEMA)
                .field("age", Schema.INT8_SCHEMA)
                .build();

        Struct struct = new Struct(schema)
                .put("firstname", "John")
                .put("lastname", "Doe")
                .put("age", (byte) 42);

        QuestDBUtils.assertSql(questDBContainer,
                "{\"ddl\":\"OK\"}\n",
                "create table " + topicName + " (firstname symbol, lastname symbol, age int)",
                QuestDBUtils.Endpoint.EXEC);

        connect.kafka().produce(topicName, "key", new String(converter.fromConnectData(topicName, schema, struct)));

        QuestDBUtils.assertSqlEventually(questDBContainer, "\"firstname\",\"lastname\",\"age\",\"key\"\r\n"
                        + "\"John\",\"Doe\",42,\"key\"\r\n",
                "select * from " + topicName);
    }

    @Test
    public void testTimestampUnitResolution_auto() {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName);
        props.put("value.converter.schemas.enable", "false");
        props.put(QuestDBSinkConnectorConfig.DESIGNATED_TIMESTAMP_COLUMN_NAME_CONFIG, "birth");
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);

        java.util.Date birth = new Calendar.Builder()
                .setTimeZone(TimeZone.getTimeZone("UTC"))
                .setDate(2022, 9, 23) // note: month is 0-based
                .setTimeOfDay(13, 53, 59, 123)
                .build().getTime();

        long birthInMillis = birth.getTime();
        long birthInMicros = birthInMillis * 1000;
        long birthInNanos = birthInMicros * 1000;


        connect.kafka().produce(topicName, "foo", "{\"firstname\":\"John\",\"lastname\":\"Doe\",\"birth\":" + birthInMillis + "}");
        connect.kafka().produce(topicName, "bar", "{\"firstname\":\"Jane\",\"lastname\":\"Doe\",\"birth\":" + birthInMicros + "}");
        connect.kafka().produce(topicName, "baz", "{\"firstname\":\"Jack\",\"lastname\":\"Doe\",\"birth\":" + birthInNanos + "}");

        QuestDBUtils.assertSqlEventually(questDBContainer, "\"firstname\",\"lastname\",\"timestamp\"\r\n"
                        + "\"John\",\"Doe\",\"2022-10-23T13:53:59.123000Z\"\r\n"
                        + "\"Jane\",\"Doe\",\"2022-10-23T13:53:59.123000Z\"\r\n"
                        + "\"Jack\",\"Doe\",\"2022-10-23T13:53:59.123000Z\"\r\n",
                "select firstname,lastname,timestamp from " + topicName);
    }

    @Test
    public void testTimestampUnitResolution_millis() {
        testTimestampUnitResolution0("millis");
    }

    @Test
    public void testTimestampUnitResolution_micros() {
        testTimestampUnitResolution0("micros");
    }

    @Test
    public void testTimestampUnitResolution_nanos() {
        testTimestampUnitResolution0("nanos");
    }

    private void testTimestampUnitResolution0(String mode) {
        TimeUnit unit;
        switch (mode) {
            case "nanos":
                unit = TimeUnit.NANOSECONDS;
                break;
            case "micros":
                unit = TimeUnit.MICROSECONDS;
                break;
            case "millis":
                unit = TimeUnit.MILLISECONDS;
                break;
            default:
                throw new IllegalArgumentException("Unknown mode: " + mode);
        }
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName);
        props.put("value.converter.schemas.enable", "false");
        props.put(QuestDBSinkConnectorConfig.DESIGNATED_TIMESTAMP_COLUMN_NAME_CONFIG, "birth");
        props.put(QuestDBSinkConnectorConfig.TIMESTAMP_UNITS_CONFIG, mode);
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);

        long birthMillis = new Calendar.Builder()
                .setTimeZone(TimeZone.getTimeZone("UTC"))
                .setDate(2206, 10, 20) // note: month is 0-based
                .setTimeOfDay(17, 46, 39, 999)
                .build().getTime().getTime();

        long birthTarget = unit.convert(birthMillis, MILLISECONDS);

        connect.kafka().produce(topicName, "foo", "{\"firstname\":\"John\",\"lastname\":\"Doe\",\"birth\":0}");
        connect.kafka().produce(topicName, "bar", "{\"firstname\":\"Jane\",\"lastname\":\"Doe\",\"birth\":" + birthTarget + "}");

        QuestDBUtils.assertSqlEventually(questDBContainer, "\"firstname\",\"lastname\",\"timestamp\"\r\n"
                        + "\"John\",\"Doe\",\"1970-01-01T00:00:00.000000Z\"\r\n"
                        + "\"Jane\",\"Doe\",\"2206-11-20T17:46:39.999000Z\"\r\n",
                "select firstname,lastname,timestamp from " + topicName);
    }

    @Test
    public void testKafkaNativeTimestampsAndExplicitDesignatedFieldTimestampMutuallyExclusive() {
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName);
        props.put("value.converter.schemas.enable", "false");
        props.put(QuestDBSinkConnectorConfig.DESIGNATED_TIMESTAMP_COLUMN_NAME_CONFIG, "born");
        props.put(QuestDBSinkConnectorConfig.DESIGNATED_TIMESTAMP_KAFKA_NATIVE_CONFIG, "true");
        try {
            connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
            fail("Expected ConnectException");
        } catch (ConnectException e) {
            assertThat(e.getMessage(), containsString("timestamp.field.name with timestamp.kafka.native"));
        }
    }

    @Test
    public void testKafkaNativeTimestamp() {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName);
        props.put("value.converter.schemas.enable", "false");
        props.put(QuestDBSinkConnectorConfig.DESIGNATED_TIMESTAMP_KAFKA_NATIVE_CONFIG, "true");

        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);

        QuestDBUtils.assertSql(questDBContainer,
                "{\"ddl\":\"OK\"}\n",
                "create table " + topicName + " (firstname string, lastname string, born timestamp) timestamp(born)",
                QuestDBUtils.Endpoint.EXEC);

        java.util.Date birth = new Calendar.Builder()
                .setTimeZone(TimeZone.getTimeZone("UTC"))
                .setDate(2022, 9, 23) // note: month is 0-based, so it's October and not November
                .setTimeOfDay(13, 53, 59, 123)
                .build().getTime();

        Map<String, Object> prodProps = new HashMap<>();
        try (KafkaProducer<byte[], byte[]> producer = connect.kafka().createProducer(prodProps)) {
            String val = "{\"firstname\":\"John\",\"lastname\":\"Doe\"}";
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topicName, null, birth.getTime(), null, val.getBytes());
            producer.send(record);
        }

        QuestDBUtils.assertSqlEventually(questDBContainer, "\"firstname\",\"lastname\",\"born\"\r\n" +
                        "\"John\",\"Doe\",\"2022-10-23T13:53:59.123000Z\"\r\n",
                "select * from " + topicName);
    }

    @Test
    public void testTimestampSMT_parseTimestamp_schemaLess() {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName);
        props.put("value.converter.schemas.enable", "false");
        props.put(QuestDBSinkConnectorConfig.DESIGNATED_TIMESTAMP_COLUMN_NAME_CONFIG, "born");
        props.put(QuestDBSinkConnectorConfig.INCLUDE_KEY_CONFIG, "false");

        String timestampFormat = "yyyy-MM-dd HH:mm:ss.SSS z";
        props.put("transforms", "Timestamp-born,Timestamp-death");
        props.put("transforms.Timestamp-born.type", "org.apache.kafka.connect.transforms.TimestampConverter$Value");
        props.put("transforms.Timestamp-born.field", "born");
        props.put("transforms.Timestamp-born.format", timestampFormat);
        props.put("transforms.Timestamp-born.target.type", "Timestamp");

        props.put("transforms.Timestamp-death.type", "org.apache.kafka.connect.transforms.TimestampConverter$Value");
        props.put("transforms.Timestamp-death.field", "death");
        props.put("transforms.Timestamp-death.target.type", "Timestamp");
        props.put("transforms.Timestamp-death.format", timestampFormat);

        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);

        QuestDBUtils.assertSql(questDBContainer,
                "{\"ddl\":\"OK\"}\n",
                "create table " + topicName + " (firstname string, lastname string, death timestamp, born timestamp) timestamp(born)",
                QuestDBUtils.Endpoint.EXEC);

        String birthTimestamp = "1985-08-02 16:41:55.402 UTC";
        String deadTimestamp = "2023-08-02 16:41:55.402 UTC";
        connect.kafka().produce(topicName, "foo",
                "{\"firstname\":\"John\""
                        + ",\"lastname\":\"Doe\""
                        + ",\"death\":\"" + deadTimestamp + "\""
                        + ",\"born\":\"" + birthTimestamp + "\"}"
        );

        QuestDBUtils.assertSqlEventually(questDBContainer, "\"firstname\",\"lastname\",\"death\",\"born\"\r\n" +
                        "\"John\",\"Doe\",\"2023-08-02T16:41:55.402000Z\",\"1985-08-02T16:41:55.402000Z\"\r\n",
                "select * from " + topicName);
    }

    @Test
    public void testTimestampSMT_parseTimestamp_withSchema() {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName);
        props.put(QuestDBSinkConnectorConfig.DESIGNATED_TIMESTAMP_COLUMN_NAME_CONFIG, "born");
        props.put(QuestDBSinkConnectorConfig.INCLUDE_KEY_CONFIG, "false");

        String timestampFormat = "yyyy-MM-dd HH:mm:ss.SSS z";
        props.put("transforms", "Timestamp-born,Timestamp-death");
        props.put("transforms.Timestamp-born.type", "org.apache.kafka.connect.transforms.TimestampConverter$Value");
        props.put("transforms.Timestamp-born.field", "born");
        props.put("transforms.Timestamp-born.format", timestampFormat);
        props.put("transforms.Timestamp-born.target.type", "Timestamp");
        props.put("transforms.Timestamp-death.type", "org.apache.kafka.connect.transforms.TimestampConverter$Value");
        props.put("transforms.Timestamp-death.field", "death");
        props.put("transforms.Timestamp-death.target.type", "Timestamp");
        props.put("transforms.Timestamp-death.format", timestampFormat);

        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);

        Schema schema = SchemaBuilder.struct().name("com.example.Person")
                .field("firstname", Schema.STRING_SCHEMA)
                .field("lastname", Schema.STRING_SCHEMA)
                .field("born", Schema.STRING_SCHEMA)
                .field("death", Schema.STRING_SCHEMA)
                .build();

        Struct struct = new Struct(schema)
                .put("firstname", "John")
                .put("lastname", "Doe")
                .put("born", "1985-08-02 16:41:55.402 UTC")
                .put("death", "2023-08-02 16:41:55.402 UTC");


        connect.kafka().produce(topicName, "key", new String(converter.fromConnectData(topicName, schema, struct)));

        QuestDBUtils.assertSqlEventually(questDBContainer, "\"firstname\",\"lastname\",\"death\",\"timestamp\"\r\n" +
                        "\"John\",\"Doe\",\"2023-08-02T16:41:55.402000Z\",\"1985-08-02T16:41:55.402000Z\"\r\n",
                "select * from " + topicName);
    }

    @Test
    public void testUpfrontTable() {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName);
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);
        Schema schema = SchemaBuilder.struct().name("com.example.Person")
                .field("firstname", Schema.STRING_SCHEMA)
                .field("lastname", Schema.STRING_SCHEMA)
                .field("age", Schema.INT8_SCHEMA)
                .build();

        Struct struct = new Struct(schema)
                .put("firstname", "John")
                .put("lastname", "Doe")
                .put("age", (byte) 42);

        QuestDBUtils.assertSql(questDBContainer,
                "{\"ddl\":\"OK\"}\n",
                "create table " + topicName + " (firstname string, lastname string, age int)",
                QuestDBUtils.Endpoint.EXEC);

        connect.kafka().produce(topicName, "key", new String(converter.fromConnectData(topicName, schema, struct)));

        QuestDBUtils.assertSqlEventually(questDBContainer, "\"firstname\",\"lastname\",\"age\",\"key\"\r\n"
                        + "\"John\",\"Doe\",42,\"key\"\r\n",
                "select * from " + topicName);
    }

    @Test
    public void testDesignatedTimestamp_noSchema_unixEpochMillis() {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName);
        props.put("value.converter.schemas.enable", "false");
        props.put(QuestDBSinkConnectorConfig.DESIGNATED_TIMESTAMP_COLUMN_NAME_CONFIG, "birth");
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);

        connect.kafka().produce(topicName, "foo", "{\"firstname\":\"John\",\"lastname\":\"Doe\",\"birth\":433774466123}");

        QuestDBUtils.assertSqlEventually(questDBContainer, "\"key\",\"firstname\",\"lastname\",\"timestamp\"\r\n"
                        + "\"foo\",\"John\",\"Doe\",\"1983-09-30T12:54:26.123000Z\"\r\n",
                "select * from " + topicName);
    }

    @Test
    public void testDesignatedTimestamp_noSchema_dateTransform_fromStringToTimestamp() {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName);
        props.put("value.converter.schemas.enable", "false");
        props.put("transforms", "convert_birth");
        props.put("transforms.convert_birth.type", "org.apache.kafka.connect.transforms.TimestampConverter$Value");
        props.put("transforms.convert_birth.target.type", "Timestamp");
        props.put("transforms.convert_birth.field", "birth");
        props.put("transforms.convert_birth.format", "yyyy-MM-dd'T'HH:mm:ss.SSSX");
        props.put(QuestDBSinkConnectorConfig.DESIGNATED_TIMESTAMP_COLUMN_NAME_CONFIG, "birth");
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);

        connect.kafka().produce(topicName, "foo", "{\"firstname\":\"John\",\"lastname\":\"Doe\",\"birth\":\"1989-09-23T10:25:33.107Z\"}");

        QuestDBUtils.assertSqlEventually(questDBContainer, "\"key\",\"firstname\",\"lastname\",\"timestamp\"\r\n"
                        + "\"foo\",\"John\",\"Doe\",\"1989-09-23T10:25:33.107000Z\"\r\n",
                "select * from " + topicName);
    }

    @Test
    public void testDesignatedTimestamp_withSchema() {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName);
        props.put(QuestDBSinkConnectorConfig.DESIGNATED_TIMESTAMP_COLUMN_NAME_CONFIG, "birth");
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);
        Schema schema = SchemaBuilder.struct().name("com.example.Person")
                .field("firstname", Schema.STRING_SCHEMA)
                .field("lastname", Schema.STRING_SCHEMA)
                .field("birth", Timestamp.SCHEMA)
                .build();

        java.util.Date birth = new Calendar.Builder()
                .setTimeZone(TimeZone.getTimeZone("UTC"))
                .setDate(2022, 9, 23) // note: month is 0-based
                .setTimeOfDay(13, 53, 59, 123)
                .build().getTime();

        Struct struct = new Struct(schema)
                .put("firstname", "John")
                .put("lastname", "Doe")
                .put("birth", birth);


        connect.kafka().produce(topicName, "key", new String(converter.fromConnectData(topicName, schema, struct)));

        QuestDBUtils.assertSqlEventually(questDBContainer, "\"key\",\"firstname\",\"lastname\",\"timestamp\"\r\n"
                        + "\"key\",\"John\",\"Doe\",\"2022-10-23T13:53:59.123000Z\"\r\n",
                "select * from " + topicName);
    }

    @Test
    public void testDoNotIncludeKey() {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName);
        props.put(QuestDBSinkConnectorConfig.DESIGNATED_TIMESTAMP_COLUMN_NAME_CONFIG, "birth");
        props.put(QuestDBSinkConnectorConfig.INCLUDE_KEY_CONFIG, "false");
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);
        Schema schema = SchemaBuilder.struct().name("com.example.Person")
                .field("firstname", Schema.STRING_SCHEMA)
                .field("lastname", Schema.STRING_SCHEMA)
                .field("birth", Timestamp.SCHEMA)
                .build();

        java.util.Date birth = new Calendar.Builder()
                .setTimeZone(TimeZone.getTimeZone("UTC"))
                .setDate(2022, 9, 23) // note: month is 0-based
                .setTimeOfDay(13, 53, 59, 123)
                .build().getTime();

        Struct struct = new Struct(schema)
                .put("firstname", "John")
                .put("lastname", "Doe")
                .put("birth", birth);


        connect.kafka().produce(topicName, "key", new String(converter.fromConnectData(topicName, schema, struct)));

        QuestDBUtils.assertSqlEventually(questDBContainer, "\"firstname\",\"lastname\",\"timestamp\"\r\n"
                        + "\"John\",\"Doe\",\"2022-10-23T13:53:59.123000Z\"\r\n",
                "select * from " + topicName);
    }

    @Test
    public void testJsonNoSchema() {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName);
        props.put("value.converter.schemas.enable", "false");
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);
        connect.kafka().produce(topicName, "key", "{\"firstname\":\"John\",\"lastname\":\"Doe\",\"age\":42}");

        QuestDBUtils.assertSqlEventually(questDBContainer, "\"firstname\",\"lastname\",\"age\"\r\n"
                        + "\"John\",\"Doe\",42\r\n",
                "select firstname,lastname,age from " + topicName);
    }

    @Test
    public void testJsonNoSchema_mixedFlotingAndIntTypes() {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName);
        props.put("value.converter.schemas.enable", "false");
        props.put(QuestDBSinkConnectorConfig.DOUBLE_COLUMNS_CONFIG, "age");
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);
        connect.kafka().produce(topicName, "key", "{\"firstname\":\"John\",\"lastname\":\"Doe\",\"age\":42}");
        connect.kafka().produce(topicName, "key", "{\"firstname\":\"Jane\",\"lastname\":\"Doe\",\"age\":42.5}");

        QuestDBUtils.assertSqlEventually(questDBContainer, "\"firstname\",\"lastname\",\"age\"\r\n"
                        + "\"John\",\"Doe\",42.0\r\n"
                        + "\"Jane\",\"Doe\",42.5\r\n",
                "select firstname,lastname,age from " + topicName);
    }


    @Test
    public void testJsonNoSchema_ArrayNotSupported() {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName);
        props.put("value.converter.schemas.enable", "false");
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);
        connect.kafka().produce(topicName, "key", "{\"firstname\":\"John\",\"lastname\":\"Doe\",\"age\":42,\"array\":[1,2,3]}");

        ConnectTestUtils.assertConnectorTaskFailedEventually(connect);
    }

    @Test
    public void testPrimitiveKey() {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName);
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);
        Schema schema = SchemaBuilder.struct().name("com.example.Person")
                .field("firstname", Schema.STRING_SCHEMA)
                .field("lastname", Schema.STRING_SCHEMA)
                .field("age", Schema.INT8_SCHEMA)
                .build();

        Struct struct = new Struct(schema)
                .put("firstname", "John")
                .put("lastname", "Doe")
                .put("age", (byte) 42);

        connect.kafka().produce(topicName, "key", new String(converter.fromConnectData(topicName, schema, struct)));

        QuestDBUtils.assertSqlEventually(questDBContainer, "\"firstname\",\"lastname\",\"age\",\"key\"\r\n"
                        + "\"John\",\"Doe\",42,\"key\"\r\n",
                "select firstname, lastname, age, key from " + topicName);
    }

    @Test
    public void testParsingStringTimestamp() {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName);
        props.put("value.converter.schemas.enable", "false");
        props.put(QuestDBSinkConnectorConfig.DESIGNATED_TIMESTAMP_COLUMN_NAME_CONFIG, "born");
        props.put(QuestDBSinkConnectorConfig.INCLUDE_KEY_CONFIG, "false");
        props.put(QuestDBSinkConnectorConfig.TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss.SSSUUU z");
        props.put(QuestDBSinkConnectorConfig.TIMESTAMP_STRING_FIELDS, "born,death");

        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);

        QuestDBUtils.assertSql(questDBContainer,
                "{\"ddl\":\"OK\"}\n",
                "create table " + topicName + " (firstname string, lastname string, death timestamp, born timestamp) timestamp(born)",
                QuestDBUtils.Endpoint.EXEC);

        String birthTimestamp = "1985-08-02 16:41:55.402095 UTC";
        String deadTimestamp = "2023-08-02 16:41:55.402095 UTC";
        connect.kafka().produce(topicName, "foo",
                "{\"firstname\":\"John\""
                        + ",\"lastname\":\"Doe\""
                        + ",\"death\":\"" + deadTimestamp + "\""
                        + ",\"born\":\"" + birthTimestamp + "\"}"
        );

        QuestDBUtils.assertSqlEventually(questDBContainer, "\"firstname\",\"lastname\",\"death\",\"born\"\r\n" +
                        "\"John\",\"Doe\",\"2023-08-02T16:41:55.402095Z\",\"1985-08-02T16:41:55.402095Z\"\r\n",
                "select * from " + topicName);
    }

    @Test
    public void testCustomPrefixWithPrimitiveKeyAndValues() {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName);
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(QuestDBSinkConnectorConfig.KEY_PREFIX_CONFIG, "col_key");
        props.put(QuestDBSinkConnectorConfig.VALUE_PREFIX_CONFIG, "col_value");

        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);

        connect.kafka().produce(topicName, "foo", "bar");

        QuestDBUtils.assertSqlEventually(questDBContainer, "\"col_key\",\"col_value\"\r\n"
                        + "\"foo\",\"bar\"\r\n",
                "select col_key, col_value from " + topicName);
    }

    @Test
    public void testSkipUnsupportedType_Bytes() {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName);
        props.put(QuestDBSinkConnectorConfig.SKIP_UNSUPPORTED_TYPES_CONFIG, "true");
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);
        Schema schema = SchemaBuilder.struct().name("com.example.Person")
                .field("firstname", Schema.STRING_SCHEMA)
                .field("lastname", Schema.STRING_SCHEMA)
                .field("age", Schema.BYTES_SCHEMA)
                .build();

        Struct struct = new Struct(schema)
                .put("firstname", "John")
                .put("lastname", "Doe")
                .put("age", new byte[]{1, 2, 3});

        connect.kafka().produce(topicName, "key", new String(converter.fromConnectData(topicName, schema, struct)));

        QuestDBUtils.assertSqlEventually(questDBContainer, "\"key\",\"firstname\",\"lastname\"\r\n"
                        + "\"key\",\"John\",\"Doe\"\r\n",
                "select key, firstname, lastname from " + topicName);
    }

    @Test
    public void testDefaultPrefixWithPrimitiveKeyAndValues() {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName);
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());

        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);

        connect.kafka().produce(topicName, "foo", "bar");

        QuestDBUtils.assertSqlEventually(questDBContainer, "\"key\",\"value\"\r\n"
                        + "\"foo\",\"bar\"\r\n",
                "select key, value from " + topicName);
    }

    @Test
    public void testStructKey() {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName);
        //overrider the convertor from String to Json
        props.put(KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);
        Schema schema = SchemaBuilder.struct().name("com.example.Person")
                .field("firstname", Schema.STRING_SCHEMA)
                .field("lastname", Schema.STRING_SCHEMA)
                .build();

        Struct struct = new Struct(schema)
                .put("firstname", "John")
                .put("lastname", "Doe");

        String json = new String(converter.fromConnectData(topicName, schema, struct));
        connect.kafka().produce(topicName, json, json);

        QuestDBUtils.assertSqlEventually(questDBContainer, "\"firstname\",\"lastname\",\"key_firstname\",\"key_lastname\"\r\n"
                        + "\"John\",\"Doe\",\"John\",\"Doe\"\r\n",
                "select firstname, lastname, key_firstname, key_lastname from " + topicName);
    }

    @Test
    public void testStructKeyWithNoPrefix() {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName);
        //overrider the convertor from String to Json
        props.put(KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(QuestDBSinkConnectorConfig.KEY_PREFIX_CONFIG, "");
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);
        Schema schema = SchemaBuilder.struct().name("com.example.Person")
                .field("firstname", Schema.STRING_SCHEMA)
                .field("lastname", Schema.STRING_SCHEMA)
                .build();

        Struct struct = new Struct(schema)
                .put("firstname", "John")
                .put("lastname", "Doe");

        String json = new String(converter.fromConnectData(topicName, schema, struct));
        connect.kafka().produce(topicName, json, "foo");

        QuestDBUtils.assertSqlEventually(questDBContainer, "\"firstname\",\"lastname\",\"value\"\r\n"
                        + "\"John\",\"Doe\",\"foo\"\r\n",
                "select firstname, lastname, value from " + topicName);
    }

    @Test
    public void testStructKeyAndPrimitiveValue() {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName);
        //overrider the convertor from String to Json
        props.put(KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);
        Schema schema = SchemaBuilder.struct().name("com.example.Person")
                .field("firstname", Schema.STRING_SCHEMA)
                .field("lastname", Schema.STRING_SCHEMA)
                .build();

        Struct struct = new Struct(schema)
                .put("firstname", "John")
                .put("lastname", "Doe");

        String json = new String(converter.fromConnectData(topicName, schema, struct));
        connect.kafka().produce(topicName, json, "foo");

        QuestDBUtils.assertSqlEventually(questDBContainer, "\"key_firstname\",\"key_lastname\",\"value\"\r\n"
                        + "\"John\",\"Doe\",\"foo\"\r\n",
                "select key_firstname, key_lastname, value from " + topicName);
    }


    @Test
    public void testExplicitTableName() {
        String tableName = ConnectTestUtils.newTableName();
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName);
        props.put(QuestDBSinkConnectorConfig.TABLE_CONFIG, tableName);
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);
        Schema schema = SchemaBuilder.struct().name("com.example.Person")
                .field("firstname", Schema.STRING_SCHEMA)
                .field("lastname", Schema.STRING_SCHEMA)
                .field("age", Schema.INT8_SCHEMA)
                .build();

        Struct struct = new Struct(schema)
                .put("firstname", "John")
                .put("lastname", "Doe")
                .put("age", (byte) 42);

        connect.kafka().produce(topicName, "key", new String(converter.fromConnectData(topicName, schema, struct)));

        QuestDBUtils.assertSqlEventually(questDBContainer, "\"firstname\",\"lastname\",\"age\"\r\n"
                        + "\"John\",\"Doe\",42\r\n",
                "select firstname,lastname,age from " + tableName);
    }

    @Test
    public void testLogicalTypes() {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName);
        props.put(QuestDBSinkConnectorConfig.TABLE_CONFIG, topicName);
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);
        Schema schema = SchemaBuilder.struct().name("com.example.Person")
                .field("firstname", Schema.STRING_SCHEMA)
                .field("lastname", Schema.STRING_SCHEMA)
                .field("age", Schema.INT8_SCHEMA)
                .field("col_timestamp", Timestamp.SCHEMA)
                .field("col_date", Date.SCHEMA)
                .field("col_time", Time.SCHEMA)
                .build();

        // both time and date
        java.util.Date timestamp = new Calendar.Builder()
                .setTimeZone(TimeZone.getTimeZone("UTC"))
                .setDate(2022, 9, 23)
                .setTimeOfDay(13, 53, 59, 123)
                .build().getTime();

        // date has no time
        java.util.Date date = new Calendar.Builder()
                .setTimeZone(TimeZone.getTimeZone("UTC"))
                .setDate(2022, 9, 23)
                .build().getTime();

        // 13:53:59.123 UTC, no date
        // QuestDB does not support time type, so we send it as long - number of millis since midnight
        java.util.Date time = new java.util.Date(50039123);

        Struct struct = new Struct(schema)
                .put("firstname", "John")
                .put("lastname", "Doe")
                .put("age", (byte) 42)
                .put("col_timestamp", timestamp)
                .put("col_date", date)
                .put("col_time", time);


        connect.kafka().produce(topicName, "key", new String(converter.fromConnectData(topicName, schema, struct)));

        QuestDBUtils.assertSqlEventually(questDBContainer, "\"firstname\",\"lastname\",\"age\",\"col_timestamp\",\"col_date\",\"col_time\"\r\n"
                        + "\"John\",\"Doe\",42,\"2022-10-23T13:53:59.123000Z\",\"2022-10-23T00:00:00.000000Z\",50039123\r\n",
                "select firstname,lastname,age, col_timestamp, col_date, col_time from " + topicName);
    }

    @Test
    public void testDecimalTypeNotSupported() {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName);
        props.put(QuestDBSinkConnectorConfig.TABLE_CONFIG, topicName);
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);
        Schema schema = SchemaBuilder.struct().name("com.example.Person")
                .field("firstname", Schema.STRING_SCHEMA)
                .field("lastname", Schema.STRING_SCHEMA)
                .field("age", Schema.INT8_SCHEMA)
                .field("col_decimal", Decimal.schema(2))
                .build();

        Struct struct = new Struct(schema)
                .put("firstname", "John")
                .put("lastname", "Doe")
                .put("age", (byte) 42)
                .put("col_decimal", new BigDecimal("123.45"));

        connect.kafka().produce(topicName, "key", new String(converter.fromConnectData(topicName, schema, struct)));

        ConnectTestUtils.assertConnectorTaskFailedEventually(connect);
    }

    @Test
    public void testNestedStructInValue() {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName);
        props.put(QuestDBSinkConnectorConfig.TABLE_CONFIG, topicName);
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);

        Schema nameSchema = SchemaBuilder.struct().name("com.example.Name")
                .field("firstname", Schema.STRING_SCHEMA)
                .field("lastname", Schema.STRING_SCHEMA)
                .build();

        Schema personSchema = SchemaBuilder.struct().name("com.example.Person")
                .field("name", nameSchema)
                .build();

        Struct person = new Struct(personSchema)
                .put("name", new Struct(nameSchema)
                        .put("firstname", "John")
                        .put("lastname", "Doe")
                );

        String value = new String(converter.fromConnectData(topicName, personSchema, person));
        connect.kafka().produce(topicName, "key", value);

        QuestDBUtils.assertSqlEventually(questDBContainer, "\"name_firstname\",\"name_lastname\"\r\n"
                        + "\"John\",\"Doe\"\r\n",
                "select name_firstname, name_lastname from " + topicName);
    }

    @Test
    public void testMultiLevelNestedStructInValue() {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName);
        props.put(QuestDBSinkConnectorConfig.TABLE_CONFIG, topicName);
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);

        Schema nameSchema = SchemaBuilder.struct().name("com.example.Name")
                .field("firstname", Schema.STRING_SCHEMA)
                .field("lastname", Schema.STRING_SCHEMA)
                .build();

        Schema personSchema = SchemaBuilder.struct().name("com.example.Person")
                .field("name", nameSchema)
                .build();

        Schema coupleSchema = SchemaBuilder.struct().name("com.example.Couple")
                .field("partner1", personSchema)
                .field("partner2", personSchema)
                .build();

        Struct couple = new Struct(coupleSchema)
                .put("partner1", new Struct(personSchema)
                        .put("name", new Struct(nameSchema)
                                .put("firstname", "John")
                                .put("lastname", "Doe")
                        ))
                .put("partner2", new Struct(personSchema)
                        .put("name", new Struct(nameSchema)
                                .put("firstname", "Jane")
                                .put("lastname", "Doe")
                        ));

        String value = new String(converter.fromConnectData(topicName, coupleSchema, couple));
        connect.kafka().produce(topicName, "key", value);

        QuestDBUtils.assertSqlEventually(questDBContainer, "\"partner1_name_firstname\",\"partner1_name_lastname\",\"partner2_name_firstname\",\"partner2_name_lastname\"\r\n"
                        + "\"John\",\"Doe\",\"Jane\",\"Doe\"\r\n",
                "select partner1_name_firstname, partner1_name_lastname, partner2_name_firstname, partner2_name_lastname from " + topicName);
    }
}
