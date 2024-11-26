package io.questdb.kafka;

import io.questdb.std.Files;
import io.questdb.std.Os;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

@Testcontainers
public final class QuestDBSinkConnectorEmbeddedTest {
    private static int httpPort = -1;
    private static int ilpPort = -1;
    private static final String OFFICIAL_QUESTDB_DOCKER = "questdb/questdb:8.2.0";
    private static final boolean DUMP_QUESTDB_CONTAINER_LOGS = true;

    private EmbeddedConnectCluster connect;
    private Converter converter;
    private String topicName;

    @TempDir(cleanup = CleanupMode.NEVER)
    static Path dbRoot;

    @BeforeAll
    public static void createContainer() {
        questDBContainer = newQuestDbContainer();
    }

    @AfterAll
    public static void stopContainer() {
        questDBContainer.stop();
        Files.rmdir(io.questdb.std.str.Path.getThreadLocal(dbRoot.toAbsolutePath().toString()), true);
    }

    private static String questDBDirectory() {
        Path questdb = dbRoot.resolve("questdb").toAbsolutePath();
        try {
            java.nio.file.Files.createDirectories(questdb);
        } catch (IOException e) {
            throw new AssertionError("Could not create directory: " + questdb, e);
        }
        return questdb.toAbsolutePath().toString();
    }

    private static GenericContainer<?> questDBContainer;

    private static GenericContainer<?> newQuestDbContainer() {
        FixedHostPortGenericContainer<?> selfGenericContainer = new FixedHostPortGenericContainer<>(OFFICIAL_QUESTDB_DOCKER);
        if (httpPort != -1) {
            selfGenericContainer = selfGenericContainer.withFixedExposedPort(httpPort, QuestDBUtils.QUESTDB_HTTP_PORT);
        } else {
            selfGenericContainer.addExposedPort(QuestDBUtils.QUESTDB_HTTP_PORT);
        }
        if (ilpPort != -1) {
            selfGenericContainer = selfGenericContainer.withFixedExposedPort(ilpPort, QuestDBUtils.QUESTDB_ILP_PORT);
        } else {
            selfGenericContainer.addExposedPort(QuestDBUtils.QUESTDB_ILP_PORT);
        }
        selfGenericContainer = selfGenericContainer.withFileSystemBind(questDBDirectory(), "/var/lib/questdb");
        if (DUMP_QUESTDB_CONTAINER_LOGS) {
            selfGenericContainer = selfGenericContainer.withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("questdb")));
        }
        selfGenericContainer.setWaitStrategy(new LogMessageWaitStrategy().withRegEx(".*server-main.*"));
        selfGenericContainer.start();

        httpPort = selfGenericContainer.getMappedPort(QuestDBUtils.QUESTDB_HTTP_PORT);
        ilpPort = selfGenericContainer.getMappedPort(QuestDBUtils.QUESTDB_ILP_PORT);

        return selfGenericContainer;
    }

    @BeforeEach
    public void setUp() {
        topicName = ConnectTestUtils.newTopicName();
        JsonConverter jsonConverter = new JsonConverter();
        jsonConverter.configure(singletonMap(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName()));
        converter = jsonConverter;


        Map<String, String> props = new HashMap<>();
        props.put("connector.client.config.override.policy", "All");
        connect = new EmbeddedConnectCluster.Builder()
                .name("questdb-connect-cluster")
                .workerProps(props)
                .numWorkers(4)
                .build();

        connect.start();
    }

    @AfterEach
    public void tearDown() {
        connect.stop();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testSmoke(boolean useHttp) {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, useHttp);
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

        QuestDBUtils.assertSqlEventually( "\"firstname\",\"lastname\",\"age\"\r\n"
                        + "\"John\",\"Doe\",42\r\n",
                "select firstname,lastname,age from " + topicName,
                httpPort);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testTableTemplateWithKey_withSchema(boolean useHttp) {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, useHttp);
        props.put(QuestDBSinkConnectorConfig.TABLE_CONFIG, "${topic}.${key}");
        props.put(QuestDBSinkConnectorConfig.INCLUDE_KEY_CONFIG, "false");
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);
        Schema schema = SchemaBuilder.struct().name("com.example.Person")
                .field("firstname", Schema.STRING_SCHEMA)
                .field("lastname", Schema.STRING_SCHEMA)
                .field("age", Schema.INT8_SCHEMA)
                .build();

        Struct john = new Struct(schema)
                .put("firstname", "John")
                .put("lastname", "Doe")
                .put("age", (byte) 42);

        Struct jane = new Struct(schema)
                .put("firstname", "Jane")
                .put("lastname", "Doe")
                .put("age", (byte) 41);

        connect.kafka().produce(topicName, "john", new String(converter.fromConnectData(topicName, schema, john)));
        connect.kafka().produce(topicName, "jane", new String(converter.fromConnectData(topicName, schema, jane)));

        QuestDBUtils.assertSqlEventually( "\"firstname\",\"lastname\",\"age\"\r\n"
                        + "\"John\",\"Doe\",42\r\n",
                "select firstname,lastname,age from " + topicName + "." + "john",
                httpPort);
        QuestDBUtils.assertSqlEventually( "\"firstname\",\"lastname\",\"age\"\r\n"
                        + "\"Jane\",\"Doe\",41\r\n",
                "select firstname,lastname,age from " + topicName + "." + "jane",
                httpPort);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testTableTemplateWithKey_schemaless(boolean useHttp) {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, useHttp);
        props.put(QuestDBSinkConnectorConfig.TABLE_CONFIG, "literal_${topic}_literal_${key}_literal");
        props.put(QuestDBSinkConnectorConfig.INCLUDE_KEY_CONFIG, "false");
        props.put("value.converter.schemas.enable", "false");
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);

        connect.kafka().produce(topicName, "john", "{\"firstname\":\"John\",\"lastname\":\"Doe\",\"age\":42}");
        connect.kafka().produce(topicName, "jane", "{\"firstname\":\"Jane\",\"lastname\":\"Doe\",\"age\":41}");

        QuestDBUtils.assertSqlEventually( "\"firstname\",\"lastname\",\"age\"\r\n"
                        + "\"John\",\"Doe\",42\r\n",
                "select firstname,lastname,age from literal_" + topicName + "_literal_" + "john_literal",
                httpPort);
        QuestDBUtils.assertSqlEventually( "\"firstname\",\"lastname\",\"age\"\r\n"
                        + "\"Jane\",\"Doe\",41\r\n",
                "select firstname,lastname,age from literal_" + topicName + "_literal_" + "jane_literal",
                httpPort);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDeadLetterQueue_wrongJson(boolean useHttp) {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, useHttp);
        props.put("value.converter.schemas.enable", "false");
        props.put("errors.deadletterqueue.topic.name", "dlq");
        props.put("errors.deadletterqueue.topic.replication.factor", "1");
        props.put("errors.tolerance", "all");
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);
        connect.kafka().produce(topicName, "key", "{\"not valid json}");
        connect.kafka().produce(topicName, "key", "{\"firstname\":\"John\",\"lastname\":\"Doe\",\"age\":42}");

        QuestDBUtils.assertSqlEventually("\"firstname\",\"lastname\",\"age\"\r\n"
                        + "\"John\",\"Doe\",42\r\n",
                "select firstname,lastname,age from " + topicName,
                httpPort);

        ConsumerRecords<byte[], byte[]> fetchedRecords = connect.kafka().consume(1, 5000, "dlq");
        Assertions.assertEquals(1, fetchedRecords.count());

        ConsumerRecord<byte[], byte[]> dqlRecord = fetchedRecords.iterator().next();
        Assertions.assertEquals("{\"not valid json}", new String(dqlRecord.value()));
    }

    @Test
    public void testDeadLetterQueue_unsupportedType() {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, true);
        props.put("errors.deadletterqueue.topic.name", "dlq");
        props.put("errors.deadletterqueue.topic.replication.factor", "1");
        props.put("errors.tolerance", "all");
        props.put("value.converter.schemas.enable", "false");
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);

        // contains array - not supported
        String badObjectString = "{\"firstname\":\"John\",\"lastname\":\"Doe\",\"age\":[1, 2, 3]}";

        connect.kafka().produce(topicName, "key", "{\"firstname\":\"John\",\"lastname\":\"Doe\",\"age\":42}");
        connect.kafka().produce(topicName, "key", badObjectString);
        connect.kafka().produce(topicName, "key", "{\"firstname\":\"Jane\",\"lastname\":\"Doe\",\"age\":41}");

        QuestDBUtils.assertSqlEventually( "\"firstname\",\"lastname\",\"age\"\r\n"
                        + "\"John\",\"Doe\",42\r\n"
                        + "\"Jane\",\"Doe\",41\r\n",
                "select firstname,lastname,age from " + topicName,
                httpPort);

        ConsumerRecords<byte[], byte[]> fetchedRecords = connect.kafka().consume(1, 120_000, "dlq");
        Assertions.assertEquals(1, fetchedRecords.count());
        Iterator<ConsumerRecord<byte[], byte[]>> iterator = fetchedRecords.iterator();
        Assertions.assertEquals(badObjectString, new String(iterator.next().value()));
    }

    @Test
    public void testDeadLetterQueue_emptyTable() {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, true);
        props.put(QuestDBSinkConnectorConfig.TABLE_CONFIG, "${key}");
        props.put(QuestDBSinkConnectorConfig.INCLUDE_KEY_CONFIG, "false");
        props.put("value.converter.schemas.enable", "false");
        props.put("errors.deadletterqueue.topic.name", "dlq");
        props.put("errors.deadletterqueue.topic.replication.factor", "1");
        props.put("errors.tolerance", "all");
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);

        connect.kafka().produce(topicName, "tab", "{\"firstname\":\"John\",\"lastname\":\"Doe\",\"age\":42}");
        String emptyRecordValue = "{\"firstname\":\"empty\",\"lastname\":\"\",\"age\":-41}";
        connect.kafka().produce(topicName, "", emptyRecordValue);
        connect.kafka().produce(topicName, "tab", "{\"firstname\":\"Jane\",\"lastname\":\"Doe\",\"age\":41}");

        QuestDBUtils.assertSqlEventually( "\"firstname\",\"lastname\",\"age\"\r\n"
                        + "\"John\",\"Doe\",42\r\n"
                        + "\"Jane\",\"Doe\",41\r\n",
                "select firstname,lastname,age from tab",
                httpPort);

        ConsumerRecords<byte[], byte[]> fetchedRecords = connect.kafka().consume(1, 120_000, "dlq");
        Assertions.assertEquals(1, fetchedRecords.count());
        Iterator<ConsumerRecord<byte[], byte[]>> iterator = fetchedRecords.iterator();
        Assertions.assertEquals(emptyRecordValue, new String(iterator.next().value()));
    }

    @Test
    public void testDeadLetterQueue_badColumnType() {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, true);
        props.put("value.converter.schemas.enable", "false");
        props.put("errors.deadletterqueue.topic.name", "dlq");
        props.put("errors.deadletterqueue.topic.replication.factor", "1");
        props.put("errors.tolerance", "all");
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);

        QuestDBUtils.assertSql(
                "{\"ddl\":\"OK\"}",
                "create table " + topicName + " (firstname string, lastname string, age int, id uuid, ts timestamp) timestamp(ts) partition by day wal",
                httpPort,
                QuestDBUtils.Endpoint.EXEC);

        String goodRecordA = "{\"firstname\":\"John\",\"lastname\":\"Doe\",\"age\":42,\"id\":\"ad956a45-a55b-441e-b80d-023a2bf5d041\"}";
        String goodRecordB = "{\"firstname\":\"John\",\"lastname\":\"Doe\",\"age\":42,\"id\":\"ad956a45-a55b-441e-b80d-023a2bf5d042\"}";
        String goodRecordC = "{\"firstname\":\"John\",\"lastname\":\"Doe\",\"age\":42,\"id\":\"ad956a45-a55b-441e-b80d-023a2bf5d043\"}";
        String badRecordA = "{\"firstname\":\"John\",\"lastname\":\"Doe\",\"age\":42,\"id\":\"Invalid UUID\"}";
        String badRecordB = "{\"firstname\":\"John\",\"lastname\":\"Doe\",\"age\":\"not a number\",\"id\":\"ad956a45-a55b-441e-b80d-023a2bf5d041\"}";

        // interleave good and bad records
        connect.kafka().produce(topicName, "key", goodRecordA);
        connect.kafka().produce(topicName, "key", badRecordA);
        connect.kafka().produce(topicName, "key", goodRecordB);
        connect.kafka().produce(topicName, "key", badRecordB);
        connect.kafka().produce(topicName, "key", goodRecordC);

        ConsumerRecords<byte[], byte[]> fetchedRecords = connect.kafka().consume(2, 120_000, "dlq");
        Assertions.assertEquals(2, fetchedRecords.count());
        Iterator<ConsumerRecord<byte[], byte[]>> iterator = fetchedRecords.iterator();
        Assertions.assertEquals(badRecordA, new String(iterator.next().value()));
        Assertions.assertEquals(badRecordB, new String(iterator.next().value()));

        QuestDBUtils.assertSqlEventually("\"firstname\",\"lastname\",\"age\",\"id\"\r\n"
                        + "\"John\",\"Doe\",42,ad956a45-a55b-441e-b80d-023a2bf5d041\r\n"
                        + "\"John\",\"Doe\",42,ad956a45-a55b-441e-b80d-023a2bf5d042\r\n"
                        + "\"John\",\"Doe\",42,ad956a45-a55b-441e-b80d-023a2bf5d043\r\n",
                "select firstname,lastname,age, id from " + topicName,
                httpPort);

    }

    @Test
    public void testbadColumnType_noDLQ() {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, true);
        props.put("value.converter.schemas.enable", "false");
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);

        QuestDBUtils.assertSql(
                "{\"ddl\":\"OK\"}",
                "create table " + topicName + " (firstname string, lastname string, age int, id uuid, ts timestamp) timestamp(ts) partition by day wal",
                httpPort,
                QuestDBUtils.Endpoint.EXEC);

        String goodRecordA = "{\"firstname\":\"John\",\"lastname\":\"Doe\",\"age\":42,\"id\":\"ad956a45-a55b-441e-b80d-023a2bf5d041\"}";
        String goodRecordB = "{\"firstname\":\"John\",\"lastname\":\"Doe\",\"age\":42,\"id\":\"ad956a45-a55b-441e-b80d-023a2bf5d042\"}";
        String goodRecordC = "{\"firstname\":\"John\",\"lastname\":\"Doe\",\"age\":42,\"id\":\"ad956a45-a55b-441e-b80d-023a2bf5d043\"}";
        String badRecordA = "{\"firstname\":\"John\",\"lastname\":\"Doe\",\"age\":42,\"id\":\"Invalid UUID\"}";
        String badRecordB = "{\"firstname\":\"John\",\"lastname\":\"Doe\",\"age\":\"not a number\",\"id\":\"ad956a45-a55b-441e-b80d-023a2bf5d041\"}";

        // interleave good and bad records
        connect.kafka().produce(topicName, "key", goodRecordA);
        connect.kafka().produce(topicName, "key", badRecordA);
        connect.kafka().produce(topicName, "key", goodRecordB);
        connect.kafka().produce(topicName, "key", badRecordB);
        connect.kafka().produce(topicName, "key", goodRecordC);

        ConnectTestUtils.assertConnectorTaskStateEventually(connect, AbstractStatus.State.FAILED);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testSymbol(boolean useHttp) {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, useHttp);
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

        QuestDBUtils.assertSqlEventually("\"firstname\",\"lastname\",\"age\"\r\n"
                        + "\"John\",\"Doe\",42\r\n",
                "select firstname,lastname,age from " + topicName,
                httpPort);
    }

    @Test
    public void testRetrying_badDataStopsTheConnectorEventually_tcp() throws Exception {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, false);
        props.put("value.converter.schemas.enable", "false");
        props.put(QuestDBSinkConnectorConfig.RETRY_BACKOFF_MS, "1000");
        props.put(QuestDBSinkConnectorConfig.MAX_RETRIES, "5");
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);

        // creates a record with 'age' as long
        connect.kafka().produce(topicName, "key", "{\"firstname\":\"John\",\"lastname\":\"Doe\",\"age\":42}");

        QuestDBUtils.assertSqlEventually("\"firstname\",\"lastname\",\"age\"\r\n"
                        + "\"John\",\"Doe\",42\r\n",
                "select firstname,lastname,age from " + topicName,
                httpPort);

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
    public void testRetrying_badDataStopsTheConnectorEventually_http() {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, true);
        props.put("value.converter.schemas.enable", "false");
        props.put(QuestDBSinkConnectorConfig.RETRY_BACKOFF_MS, "1000");
        props.put(QuestDBSinkConnectorConfig.MAX_RETRIES, "5");
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);

        // creates a record with 'age' as long
        connect.kafka().produce(topicName, "key", "{\"firstname\":\"John\",\"lastname\":\"Doe\",\"age\":42}");

        QuestDBUtils.assertSqlEventually("\"firstname\",\"lastname\",\"age\"\r\n"
                        + "\"John\",\"Doe\",42\r\n",
                "select firstname,lastname,age from " + topicName,
                httpPort);

        for (int i = 0; i < 150_000; i++) {
            // injects records with 'age' as string
            connect.kafka().produce(topicName, "key", "{\"firstname\":\"John\",\"lastname\":\"Doe\",\"age\":\"str\"}");
        }

        ConnectTestUtils.assertConnectorTaskStateEventually(connect, AbstractStatus.State.FAILED);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testRetrying_recoversFromInfrastructureIssues(boolean useHttp) throws Exception {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, useHttp);
        props.put("value.converter.schemas.enable", "false");
        props.put(QuestDBSinkConnectorConfig.RETRY_BACKOFF_MS, "1000");
        props.put(QuestDBSinkConnectorConfig.MAX_RETRIES, "40");
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);

        connect.kafka().produce(topicName, "key1", "{\"firstname\":\"John\",\"lastname\":\"Doe\",\"age\":42}");


        QuestDBUtils.assertSqlEventually("\"firstname\",\"lastname\",\"age\"\r\n"
                        + "\"John\",\"Doe\",42\r\n",
                "select firstname,lastname,age from " + topicName,
                httpPort);

        questDBContainer.stop();
        // insert a few records while the QuestDB is down
        for (int i = 0; i < 10; i++) {
            connect.kafka().produce(topicName, "key2", "{\"firstname\":\"John\",\"lastname\":\"Doe\",\"age\":42}");
            Thread.sleep(500);
        }

        // restart QuestDB
        questDBContainer = newQuestDbContainer();
        for (int i = 0; i < 50; i++) {
            connect.kafka().produce(topicName, "key3", "{\"firstname\":\"John\",\"lastname\":\"Doe\",\"age\":" + i + "}");
        }

        QuestDBUtils.assertSqlEventually("\"firstname\",\"lastname\",\"age\"\r\n"
                        + "\"John\",\"Doe\",49\r\n",
                "select firstname,lastname,age from " + topicName + " where age = 49",
                20,
                httpPort
        );
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testJsonNestedLongTimestampInSeconds(boolean useHttp) {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, useHttp);
        props.put("value.converter.schemas.enable", "false");
        props.put(QuestDBSinkConnectorConfig.DESIGNATED_TIMESTAMP_COLUMN_NAME_CONFIG, "timeseriesElement_observationDateTime");
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);
        connect.kafka().produce(topicName, "key",
                "{\"timeseriesElement\":{\n" +
                "\"open\":65994.204593157,\n" +
                "\"close\":66213.0396203394,\n" +
                "\"low\":65106.5637640695,\n" +
                "\"high\":66467.4682495325,\n" +
                "\"observationDateTime\":1712185812,\n" +
                "\"volume\":104734.408713828\n" +
                "}}"
        );

        QuestDBUtils.assertSqlEventually(
                "\"key\",\"timeseriesElement_volume\",\"timeseriesElement_high\",\"timeseriesElement_low\",\"timeseriesElement_close\",\"timeseriesElement_open\",\"timestamp\"\r\n" +
                        "\"key\",104734.408713828,66467.4682495325,65106.5637640695,66213.0396203394,65994.204593157,\"2024-04-03T23:10:12.000000Z\"\r\n",
                "select * from " + topicName,
                httpPort);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testEmptyCollection_wontFailTheConnector(boolean useHttp) {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, useHttp);
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

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testSymbol_withAllOtherILPTypes(boolean useHttp) {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, useHttp);
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

        QuestDBUtils.assertSqlEventually( "\"firstname\",\"lastname\",\"age\",\"vegan\",\"height\",\"birth\"\r\n"
                        + "\"John\",\"Doe\",42,true,1.8,\"2022-10-23T13:53:59.123000Z\"\r\n"
                        + "\"Jane\",\"Doe\",41,false,1.6,\"2021-10-23T13:53:59.123000Z\"\r\n",
                "select firstname,lastname,age,vegan,height,birth from " + topicName,
                httpPort);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testUpfrontTable_withSymbols(boolean useHttp) {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, useHttp);
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

        QuestDBUtils.assertSql(
                "{\"ddl\":\"OK\"}",
                "create table " + topicName + " (firstname symbol, lastname symbol, age int, ts timestamp) timestamp(ts) partition by day wal",
                httpPort,
                QuestDBUtils.Endpoint.EXEC);

        connect.kafka().produce(topicName, "key", new String(converter.fromConnectData(topicName, schema, struct)));

        QuestDBUtils.assertSqlEventually("\"firstname\",\"lastname\",\"age\",\"key\"\r\n"
                        + "\"John\",\"Doe\",42,\"key\"\r\n",
                "select firstname, lastname, age, key from " + topicName,
                httpPort);
    }

    @Test
    public void testExactlyOnce_withDedup() throws BrokenBarrierException, InterruptedException {
        // no parametrized since TCP transport does not support exactly-once processing
        connect.kafka().createTopic(topicName, 4);

        Schema schema = SchemaBuilder.struct().name("com.example.Event")
                .field("ts", Schema.INT64_SCHEMA)
                .field("id", Schema.STRING_SCHEMA)
                .field("type", Schema.INT64_SCHEMA)
                .build();

        // async inserts to Kafka
        long recordCount = 1_000_000;
        Map<String, Object> prodProps = new HashMap<>();
        new Thread(() -> {
            try (KafkaProducer<byte[], byte[]> producer = connect.kafka().createProducer(prodProps)) {
                for (long i = 0; i < recordCount; i++) {
                    Instant now = Instant.now();
                    long nanoTs = now.getEpochSecond() * 1_000_000_000 + now.getNano();
                    Struct struct = new Struct(schema)
                            .put("ts", nanoTs)
                            .put("id", UUID.randomUUID().toString())
                            .put("type", (i % 5));

                    byte[] value = new String(converter.fromConnectData(topicName, schema, struct)).getBytes();
                    producer.send(new ProducerRecord<>(topicName, null, null, value));

                    // 1% chance of duplicates - we want them to be also deduped by QuestDB
                    if (ThreadLocalRandom.current().nextInt(100) == 0) {
                        producer.send(new ProducerRecord<>(topicName, null, null, value));
                    }

                }
            }
        }).start();


        // configure questdb dedups
        QuestDBUtils.assertSql(
                "{\"ddl\":\"OK\"}",
                "CREATE TABLE " + topicName + " (ts TIMESTAMP, id UUID, type LONG) timestamp(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, id);",
                httpPort,
                QuestDBUtils.Endpoint.EXEC);

        // start connector
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, true);
        props.put(QuestDBSinkConnectorConfig.INCLUDE_KEY_CONFIG, "false");
        props.put(QuestDBSinkConnectorConfig.DESIGNATED_TIMESTAMP_COLUMN_NAME_CONFIG, "ts");
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);

        // restart QuestDB every 15 seconds
        CyclicBarrier barrier = new CyclicBarrier(2);
        new Thread(() -> {
            while (barrier.getNumberWaiting() == 0) {
                Os.sleep(15_000);
                restartQuestDB();
            }
            try {
                barrier.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            } catch (BrokenBarrierException e) {
                // shouldn't happen
                throw new RuntimeException(e);
            }
        }).start();

        // make sure we have all records in the table
        QuestDBUtils.assertSqlEventually(
                "\"count\"\r\n"
                        + recordCount + "\r\n",
                "select count(*) from " + topicName,
                600,
                httpPort);

        // await the restarter thread so we don't leave dangling threads behind
        barrier.await();
    }

    private static void restartQuestDB() {
        questDBContainer.stop();
        questDBContainer = newQuestDbContainer();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testTimestampUnitResolution_auto(boolean useHttp) {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, useHttp);
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

        QuestDBUtils.assertSqlEventually("\"firstname\",\"lastname\",\"timestamp\"\r\n"
                        + "\"John\",\"Doe\",\"2022-10-23T13:53:59.123000Z\"\r\n"
                        + "\"Jane\",\"Doe\",\"2022-10-23T13:53:59.123000Z\"\r\n"
                        + "\"Jack\",\"Doe\",\"2022-10-23T13:53:59.123000Z\"\r\n",
                "select firstname,lastname,timestamp from " + topicName,
                httpPort);
    }

    @ParameterizedTest
    @CsvSource({
            "seconds, true",
            "seconds, false",
            "millis, true",
            "millis, false",
            "micros, true",
            "micros, false",
            "nanos, true",
            "nanos, false",
    })
    public void testTimestampUnitResolution0(String mode, boolean useHttp) {
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
            case "seconds":
                unit = TimeUnit.SECONDS;
                break;
            default:
                throw new IllegalArgumentException("Unknown mode: " + mode);
        }
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, useHttp);
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


        String upperBound = unit == SECONDS ? "2206-11-20T17:46:39.000000Z" : "2206-11-20T17:46:39.999000Z";
        QuestDBUtils.assertSqlEventually("\"firstname\",\"lastname\",\"timestamp\"\r\n"
                        + "\"John\",\"Doe\",\"1970-01-01T00:00:00.000000Z\"\r\n"
                        + "\"Jane\",\"Doe\",\""+ upperBound + "\"\r\n",
                "select firstname,lastname,timestamp from " + topicName,
                httpPort);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testKafkaNativeTimestampsAndExplicitDesignatedFieldTimestampMutuallyExclusive(boolean useHttp) {
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, useHttp);
        props.put("value.converter.schemas.enable", "false");
        props.put(QuestDBSinkConnectorConfig.DESIGNATED_TIMESTAMP_COLUMN_NAME_CONFIG, "born");
        props.put(QuestDBSinkConnectorConfig.DESIGNATED_TIMESTAMP_KAFKA_NATIVE_CONFIG, "true");
        try {
            connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
            fail("Expected ConnectException");
        } catch (ConnectException e) {
            assertThat(e.getMessage(), containsString("'timestamp.field.name' with 'timestamp.kafka.native'"));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testKafkaNativeTimestamp(boolean useHttp) {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, useHttp);
        props.put("value.converter.schemas.enable", "false");
        props.put(QuestDBSinkConnectorConfig.DESIGNATED_TIMESTAMP_KAFKA_NATIVE_CONFIG, "true");

        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);

        QuestDBUtils.assertSql(
                "{\"ddl\":\"OK\"}",
                "create table " + topicName + " (firstname string, lastname string, born timestamp) timestamp(born) partition by day wal",
                httpPort,
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

        QuestDBUtils.assertSqlEventually("\"firstname\",\"lastname\",\"born\"\r\n" +
                        "\"John\",\"Doe\",\"2022-10-23T13:53:59.123000Z\"\r\n",
                "select * from " + topicName,
                httpPort);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testTimestampSMT_parseTimestamp_schemaLess(boolean useHttp) {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, useHttp);
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

        QuestDBUtils.assertSql(
                "{\"ddl\":\"OK\"}",
                "create table " + topicName + " (firstname string, lastname string, death timestamp, born timestamp) timestamp(born) partition by day wal",
                httpPort,
                QuestDBUtils.Endpoint.EXEC);

        String birthTimestamp = "1985-08-02 16:41:55.402 UTC";
        String deadTimestamp = "2023-08-02 16:41:55.402 UTC";
        connect.kafka().produce(topicName, "foo",
                "{\"firstname\":\"John\""
                        + ",\"lastname\":\"Doe\""
                        + ",\"death\":\"" + deadTimestamp + "\""
                        + ",\"born\":\"" + birthTimestamp + "\"}"
        );

        QuestDBUtils.assertSqlEventually("\"firstname\",\"lastname\",\"death\",\"born\"\r\n" +
                        "\"John\",\"Doe\",\"2023-08-02T16:41:55.402000Z\",\"1985-08-02T16:41:55.402000Z\"\r\n",
                "select * from " + topicName,
                httpPort);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testTimestampSMT_parseTimestamp_withSchema(boolean useHttp) {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, useHttp);
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

        QuestDBUtils.assertSqlEventually("\"firstname\",\"lastname\",\"death\",\"timestamp\"\r\n" +
                        "\"John\",\"Doe\",\"2023-08-02T16:41:55.402000Z\",\"1985-08-02T16:41:55.402000Z\"\r\n",
                "select * from " + topicName,
                httpPort);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testUpfrontTable(boolean useHttp) {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, useHttp);
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

        QuestDBUtils.assertSql(
                "{\"ddl\":\"OK\"}",
                "create table " + topicName + " (firstname string, lastname string, age int, ts timestamp) timestamp(ts) partition by day wal",
                httpPort,
                QuestDBUtils.Endpoint.EXEC);

        connect.kafka().produce(topicName, "key", new String(converter.fromConnectData(topicName, schema, struct)));

        QuestDBUtils.assertSqlEventually("\"firstname\",\"lastname\",\"age\",\"key\"\r\n"
                        + "\"John\",\"Doe\",42,\"key\"\r\n",
                "select firstname, lastname, age, key from " + topicName,
                httpPort);
    }

    @Test
    public void testContentBasedRouting_extractFromValueStruct() {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, true);
        props.put("transforms", "route");
        props.put("transforms.route.type", "io.github.rerorero.kafka.smt.PayloadBasisRouter$Value");
        props.put("transforms.route.replacement", topicName + "-{$.firstname}");
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);
        Schema schema = SchemaBuilder.struct().name("com.example.Person")
                .field("firstname", Schema.STRING_SCHEMA)
                .field("lastname", Schema.STRING_SCHEMA)
                .field("age", Schema.INT8_SCHEMA)
                .build();

        Struct john = new Struct(schema)
                .put("firstname", "John")
                .put("lastname", "Doe")
                .put("age", (byte) 42);

        Struct joe = new Struct(schema)
                .put("firstname", "Joe")
                .put("lastname", "Doe")
                .put("age", (byte) 41);


        connect.kafka().produce(topicName, "john", new String(converter.fromConnectData(topicName, schema, john)));
        connect.kafka().produce(topicName, "joe", new String(converter.fromConnectData(topicName, schema, joe)));

        QuestDBUtils.assertSqlEventually("\"firstname\",\"lastname\",\"age\",\"key\"\r\n"
                        + "\"John\",\"Doe\",42,\"john\"\r\n",
                "select firstname, lastname, age, key from '" + topicName + "-John'",
                httpPort);
        QuestDBUtils.assertSqlEventually("\"firstname\",\"lastname\",\"age\",\"key\"\r\n"
                        + "\"Joe\",\"Doe\",41,\"joe\"\r\n",
                "select firstname, lastname, age, key from '" + topicName + "-Joe'",
                httpPort);
    }

    @Test
    public void testContentBasedRouting_extractFromKey() {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, true);
        props.put(KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        props.put("key.converter.schemas.enable", "false");
        props.put(QuestDBSinkConnectorConfig.INCLUDE_KEY_CONFIG, "false");
        props.put("transforms", "route");
        props.put("transforms.route.type", "io.github.rerorero.kafka.smt.PayloadBasisRouter$Key");
        props.put("transforms.route.replacement", topicName + "-{$.name}");
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);
        Schema schema = SchemaBuilder.struct().name("com.example.Person")
                .field("firstname", Schema.STRING_SCHEMA)
                .field("lastname", Schema.STRING_SCHEMA)
                .field("age", Schema.INT8_SCHEMA)
                .build();

        Struct john = new Struct(schema)
                .put("firstname", "John")
                .put("lastname", "Doe")
                .put("age", (byte) 42);

        Struct joe = new Struct(schema)
                .put("firstname", "Joe")
                .put("lastname", "Doe")
                .put("age", (byte) 41);


        connect.kafka().produce(topicName, "{\"name\": \"john\"}", new String(converter.fromConnectData(topicName, schema, john)));
        connect.kafka().produce(topicName, "{\"name\": \"joe\"}", new String(converter.fromConnectData(topicName, schema, joe)));

        QuestDBUtils.assertSqlEventually("\"firstname\",\"lastname\",\"age\"\r\n"
                        + "\"John\",\"Doe\",42\r\n",
                "select firstname, lastname, age from '" + topicName + "-john'",
                httpPort);
        QuestDBUtils.assertSqlEventually("\"firstname\",\"lastname\",\"age\"\r\n"
                        + "\"Joe\",\"Doe\",41\r\n",
                "select firstname, lastname, age from '" + topicName + "-joe'",
                httpPort);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDesignatedTimestamp_noSchema_unixEpochMillis(boolean useHttp) {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, useHttp);
        props.put("value.converter.schemas.enable", "false");
        props.put(QuestDBSinkConnectorConfig.DESIGNATED_TIMESTAMP_COLUMN_NAME_CONFIG, "birth");
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);

        connect.kafka().produce(topicName, "foo", "{\"firstname\":\"John\",\"lastname\":\"Doe\",\"birth\":433774466123}");

        QuestDBUtils.assertSqlEventually("\"key\",\"firstname\",\"lastname\",\"timestamp\"\r\n"
                        + "\"foo\",\"John\",\"Doe\",\"1983-09-30T12:54:26.123000Z\"\r\n",
                "select * from " + topicName,
                httpPort);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDesignatedTimestamp_noSchema_dateTransform_fromStringToTimestamp(boolean useHttp) {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, useHttp);
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

        QuestDBUtils.assertSqlEventually("\"key\",\"firstname\",\"lastname\",\"timestamp\"\r\n"
                        + "\"foo\",\"John\",\"Doe\",\"1989-09-23T10:25:33.107000Z\"\r\n",
                "select * from " + topicName,
                httpPort);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDesignatedTimestamp_withSchema(boolean useHttp) {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, useHttp);
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

        QuestDBUtils.assertSqlEventually("\"key\",\"firstname\",\"lastname\",\"timestamp\"\r\n"
                        + "\"key\",\"John\",\"Doe\",\"2022-10-23T13:53:59.123000Z\"\r\n",
                "select * from " + topicName,
                httpPort);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDoNotIncludeKey(boolean useHttp) {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, useHttp);
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

        QuestDBUtils.assertSqlEventually("\"firstname\",\"lastname\",\"timestamp\"\r\n"
                        + "\"John\",\"Doe\",\"2022-10-23T13:53:59.123000Z\"\r\n",
                "select * from " + topicName,
                httpPort);
    }

    @Test
    public void testExtractKafkaIngestionTimestampAsField_designated() {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, true);
        props.put(QuestDBSinkConnectorConfig.DESIGNATED_TIMESTAMP_COLUMN_NAME_CONFIG, "birth"); // the field is injected via InsertField SMT
        props.put(QuestDBSinkConnectorConfig.INCLUDE_KEY_CONFIG, "false");
        props.put("transforms", "InsertField");
        props.put("transforms.InsertField.type", "org.apache.kafka.connect.transforms.InsertField$Value");
        props.put("transforms.InsertField.timestamp.field", "birth");
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);

        // note: there is no birth field in the message payload
        Schema schema = SchemaBuilder.struct().name("com.example.Person")
                .field("firstname", Schema.STRING_SCHEMA)
                .field("lastname", Schema.STRING_SCHEMA)
                .build();
        Struct struct = new Struct(schema)
                .put("firstname", "John")
                .put("lastname", "Doe");

        Map<String, Object> prodProps = new HashMap<>();
        try (KafkaProducer<byte[], byte[]> producer = connect.kafka().createProducer(prodProps)) {
            java.util.Date birth = new Calendar.Builder()
                    .setTimeZone(TimeZone.getTimeZone("UTC"))
                    .setDate(2022, 9, 23) // note: month is 0-based
                    .setTimeOfDay(13, 53, 59, 123)
                    .build().getTime();
            long kafkaTimestamp = birth.getTime();
            ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(topicName, null, kafkaTimestamp, "key".getBytes(), new String(converter.fromConnectData(topicName, schema, struct)).getBytes());
            producer.send(producerRecord);
        }

        QuestDBUtils.assertSqlEventually("\"firstname\",\"lastname\",\"timestamp\"\r\n"
                        + "\"John\",\"Doe\",\"2022-10-23T13:53:59.123000Z\"\r\n",
                "select * from " + topicName,
                httpPort);
    }

    @Test
    public void testExtractKafkaIngestionTimestampAsField_nondesignated_schemaless() {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, true);
        props.put(QuestDBSinkConnectorConfig.INCLUDE_KEY_CONFIG, "false");
        props.put("value.converter.schemas.enable", "false");
        props.put("transforms", "InsertField,TimestampConverter");
        props.put("transforms.InsertField.type", "org.apache.kafka.connect.transforms.InsertField$Value");
        props.put("transforms.InsertField.timestamp.field", "birth");
        props.put("transforms.TimestampConverter.type", "org.apache.kafka.connect.transforms.TimestampConverter$Value");
        props.put("transforms.TimestampConverter.field", "birth");
        props.put("transforms.TimestampConverter.target.type", "Timestamp");
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);

        QuestDBUtils.assertSql(
                "{\"ddl\":\"OK\"}",
                "create table " + topicName + " (firstname string, lastname string, birth timestamp, ts timestamp) timestamp(ts) partition by day wal",
                httpPort,
                QuestDBUtils.Endpoint.EXEC);

        // note: there is no birth field in the message payload
        String personJson = "{\"firstname\":\"John\",\"lastname\":\"Doe\"}";

        Map<String, Object> prodProps = new HashMap<>();
        try (KafkaProducer<byte[], byte[]> producer = connect.kafka().createProducer(prodProps)) {
            java.util.Date birth = new Calendar.Builder()
                    .setTimeZone(TimeZone.getTimeZone("UTC"))
                    .setDate(2022, 9, 23) // note: month is 0-based
                    .setTimeOfDay(13, 53, 59, 123)
                    .build().getTime();
            long kafkaTimestamp = birth.getTime();
            ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(topicName, null, kafkaTimestamp, "key".getBytes(), personJson.getBytes());
            producer.send(producerRecord);
        }

        QuestDBUtils.assertSqlEventually("\"firstname\",\"lastname\",\"birth\"\r\n"
                        + "\"John\",\"Doe\",\"2022-10-23T13:53:59.123000Z\"\r\n",
                "select firstname, lastname, birth from " + topicName,
                httpPort);
    }


    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testJsonNoSchema(boolean useHttp) {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, useHttp);
        props.put("value.converter.schemas.enable", "false");
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);
        connect.kafka().produce(topicName, "key", "{\"firstname\":\"John\",\"lastname\":\"Doe\",\"age\":42}");

        QuestDBUtils.assertSqlEventually("\"firstname\",\"lastname\",\"age\"\r\n"
                        + "\"John\",\"Doe\",42\r\n",
                "select firstname,lastname,age from " + topicName,
                httpPort);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testJsonNoSchema_mixedFlotingAndIntTypes(boolean useHttp) {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, useHttp);
        props.put("value.converter.schemas.enable", "false");
        props.put(QuestDBSinkConnectorConfig.DOUBLE_COLUMNS_CONFIG, "age");
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);
        connect.kafka().produce(topicName, "key", "{\"firstname\":\"John\",\"lastname\":\"Doe\",\"age\":42}");
        connect.kafka().produce(topicName, "key", "{\"firstname\":\"Jane\",\"lastname\":\"Doe\",\"age\":42.5}");

        QuestDBUtils.assertSqlEventually("\"firstname\",\"lastname\",\"age\"\r\n"
                        + "\"John\",\"Doe\",42.0\r\n"
                        + "\"Jane\",\"Doe\",42.5\r\n",
                "select firstname,lastname,age from " + topicName,
                httpPort);
    }


    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testJsonNoSchema_ArrayNotSupported(boolean useHttp) {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, useHttp);
        props.put("value.converter.schemas.enable", "false");
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);
        connect.kafka().produce(topicName, "key", "{\"firstname\":\"John\",\"lastname\":\"Doe\",\"age\":42,\"array\":[1,2,3]}");

        ConnectTestUtils.assertConnectorTaskFailedEventually(connect);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testPrimitiveKey(boolean useHttp) {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, useHttp);
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

        QuestDBUtils.assertSqlEventually("\"firstname\",\"lastname\",\"age\",\"key\"\r\n"
                        + "\"John\",\"Doe\",42,\"key\"\r\n",
                "select firstname, lastname, age, key from " + topicName,
                httpPort);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testParsingStringTimestamp_designatedTimestampNotListedExplicitly(boolean useHttp) {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, useHttp);
        props.put("value.converter.schemas.enable", "false");
        props.put(QuestDBSinkConnectorConfig.DESIGNATED_TIMESTAMP_COLUMN_NAME_CONFIG, "born");
        props.put(QuestDBSinkConnectorConfig.INCLUDE_KEY_CONFIG, "false");
        props.put(QuestDBSinkConnectorConfig.TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss.SSSUUU z");

        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);

        QuestDBUtils.assertSql(
                "{\"ddl\":\"OK\"}",
                "create table " + topicName + " (firstname string, lastname string, born timestamp) timestamp(born) partition by day wal",
                httpPort,
                QuestDBUtils.Endpoint.EXEC);

        String birthTimestamp = "1985-08-02 16:41:55.402095 UTC";
        connect.kafka().produce(topicName, "foo",
                "{\"firstname\":\"John\""
                        + ",\"lastname\":\"Doe\""
                        + ",\"born\":\"" + birthTimestamp + "\"}"
        );

        QuestDBUtils.assertSqlEventually("\"firstname\",\"lastname\",\"born\"\r\n" +
                        "\"John\",\"Doe\",\"1985-08-02T16:41:55.402095Z\"\r\n",
                "select * from " + topicName,
                httpPort);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testParsingStringTimestamp(boolean useHttp) {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, useHttp);
        props.put("value.converter.schemas.enable", "false");
        props.put(QuestDBSinkConnectorConfig.DESIGNATED_TIMESTAMP_COLUMN_NAME_CONFIG, "born");
        props.put(QuestDBSinkConnectorConfig.INCLUDE_KEY_CONFIG, "false");
        props.put(QuestDBSinkConnectorConfig.TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss.SSSUUU z");
        props.put(QuestDBSinkConnectorConfig.TIMESTAMP_STRING_FIELDS, "born,death");

        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);

        QuestDBUtils.assertSql(
                "{\"ddl\":\"OK\"}",
                "create table " + topicName + " (firstname string, lastname string, death timestamp, born timestamp) timestamp(born) partition by day wal",
                httpPort,
                QuestDBUtils.Endpoint.EXEC);

        String birthTimestamp = "1985-08-02 16:41:55.402095 UTC";
        String deadTimestamp = "2023-08-02 16:41:55.402095 UTC";
        connect.kafka().produce(topicName, "foo",
                "{\"firstname\":\"John\""
                        + ",\"lastname\":\"Doe\""
                        + ",\"death\":\"" + deadTimestamp + "\""
                        + ",\"born\":\"" + birthTimestamp + "\"}"
        );

        QuestDBUtils.assertSqlEventually("\"firstname\",\"lastname\",\"death\",\"born\"\r\n" +
                        "\"John\",\"Doe\",\"2023-08-02T16:41:55.402095Z\",\"1985-08-02T16:41:55.402095Z\"\r\n",
                "select * from " + topicName,
                httpPort);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testParsingStringTimestamp_defaultPattern(boolean useHttp) {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, useHttp);
        props.put("value.converter.schemas.enable", "false");
        props.put(QuestDBSinkConnectorConfig.DESIGNATED_TIMESTAMP_COLUMN_NAME_CONFIG, "born");
        props.put(QuestDBSinkConnectorConfig.INCLUDE_KEY_CONFIG, "false");
        props.put(QuestDBSinkConnectorConfig.TIMESTAMP_STRING_FIELDS, "born,death");

        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);

        QuestDBUtils.assertSql(
                "{\"ddl\":\"OK\"}",
                "create table " + topicName + " (firstname string, lastname string, death timestamp, born timestamp) timestamp(born) partition by day wal",
                httpPort,
                QuestDBUtils.Endpoint.EXEC);

        String birthTimestamp = "1985-08-02T16:41:55.402095Z";
        String deadTimestamp = "2023-08-02T16:41:55.402095Z";
        connect.kafka().produce(topicName, "foo",
                "{\"firstname\":\"John\""
                        + ",\"lastname\":\"Doe\""
                        + ",\"death\":\"" + deadTimestamp + "\""
                        + ",\"born\":\"" + birthTimestamp + "\"}"
        );

        QuestDBUtils.assertSqlEventually("\"firstname\",\"lastname\",\"death\",\"born\"\r\n" +
                        "\"John\",\"Doe\",\"2023-08-02T16:41:55.402095Z\",\"1985-08-02T16:41:55.402095Z\"\r\n",
                "select * from " + topicName,
                httpPort);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testCustomPrefixWithPrimitiveKeyAndValues(boolean useHttp) {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, useHttp);
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(QuestDBSinkConnectorConfig.KEY_PREFIX_CONFIG, "col_key");
        props.put(QuestDBSinkConnectorConfig.VALUE_PREFIX_CONFIG, "col_value");

        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);

        connect.kafka().produce(topicName, "foo", "bar");

        QuestDBUtils.assertSqlEventually("\"col_key\",\"col_value\"\r\n"
                        + "\"foo\",\"bar\"\r\n",
                "select col_key, col_value from " + topicName,
                httpPort);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testSkipUnsupportedType_Bytes(boolean useHttp) {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, useHttp);
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

        QuestDBUtils.assertSqlEventually("\"key\",\"firstname\",\"lastname\"\r\n"
                        + "\"key\",\"John\",\"Doe\"\r\n",
                "select key, firstname, lastname from " + topicName,
                httpPort);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDefaultPrefixWithPrimitiveKeyAndValues(boolean useHttp) {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, useHttp);
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());

        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);

        connect.kafka().produce(topicName, "foo", "bar");

        QuestDBUtils.assertSqlEventually("\"key\",\"value\"\r\n"
                        + "\"foo\",\"bar\"\r\n",
                "select key, value from " + topicName,
                httpPort);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testStructKey(boolean useHttp) {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, useHttp);
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

        QuestDBUtils.assertSqlEventually("\"firstname\",\"lastname\",\"key_firstname\",\"key_lastname\"\r\n"
                        + "\"John\",\"Doe\",\"John\",\"Doe\"\r\n",
                "select firstname, lastname, key_firstname, key_lastname from " + topicName,
                httpPort);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testStructKeyWithNoPrefix(boolean useHttp) {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, useHttp);
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

        QuestDBUtils.assertSqlEventually("\"firstname\",\"lastname\",\"value\"\r\n"
                        + "\"John\",\"Doe\",\"foo\"\r\n",
                "select firstname, lastname, value from " + topicName,
                httpPort);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testStructKeyAndPrimitiveValue(boolean useHttp) {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, useHttp);
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

        QuestDBUtils.assertSqlEventually("\"key_firstname\",\"key_lastname\",\"value\"\r\n"
                        + "\"John\",\"Doe\",\"foo\"\r\n",
                "select key_firstname, key_lastname, value from " + topicName,
                httpPort);
    }


    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testExplicitTableName(boolean useHttp) {
        String tableName = ConnectTestUtils.newTableName();
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, useHttp);
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

        QuestDBUtils.assertSqlEventually("\"firstname\",\"lastname\",\"age\"\r\n"
                        + "\"John\",\"Doe\",42\r\n",
                "select firstname,lastname,age from " + tableName,
                httpPort);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testLogicalTypes(boolean useHttp) {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, useHttp);
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

        QuestDBUtils.assertSqlEventually("\"firstname\",\"lastname\",\"age\",\"col_timestamp\",\"col_date\",\"col_time\"\r\n"
                        + "\"John\",\"Doe\",42,\"2022-10-23T13:53:59.123000Z\",\"2022-10-23T00:00:00.000000Z\",50039123\r\n",
                "select firstname,lastname,age, col_timestamp, col_date, col_time from " + topicName,
                httpPort);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDecimalTypeNotSupported(boolean useHttp) {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, useHttp);
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

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testNestedStructInValue(boolean useHttp) {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, useHttp);
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

        QuestDBUtils.assertSqlEventually("\"name_firstname\",\"name_lastname\"\r\n"
                        + "\"John\",\"Doe\"\r\n",
                "select name_firstname, name_lastname from " + topicName,
                httpPort);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testMultiLevelNestedStructInValue(boolean useHttp) {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, useHttp);
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

        QuestDBUtils.assertSqlEventually("\"partner1_name_firstname\",\"partner1_name_lastname\",\"partner2_name_firstname\",\"partner2_name_lastname\"\r\n"
                        + "\"John\",\"Doe\",\"Jane\",\"Doe\"\r\n",
                "select partner1_name_firstname, partner1_name_lastname, partner2_name_firstname, partner2_name_lastname from " + topicName,
                httpPort);
    }
}
