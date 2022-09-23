package org.questdb.kafka;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;

import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.fail;
import static org.questdb.kafka.QuestDBUtils.assertSqlEventually;

public final class QuestDBSinkConnectorEmbeddedTest {
    private static final String CONNECTOR_NAME = "questdb-sink-connector";
    private static final long CONNECTOR_START_TIMEOUT_MS = SECONDS.toMillis(60);

    private EmbeddedConnectCluster connect;
    private Converter converter;

    private final GenericContainer questDBContainer = new GenericContainer("questdb/questdb:6.5.2")
            .withExposedPorts(QuestDBUtils.QUESTDB_HTTP_PORT, QuestDBUtils.QUESTDB_ILP_PORT)
            .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("questdb")))
            .withEnv("QDB_CAIRO_COMMIT_LAG", "100")
            .withEnv("JAVA_OPTS", "-Djava.locale.providers=JRE,SPI");

    @BeforeAll
    public static void beforeAll() {

    }

    @BeforeEach
    public void setUp() {
        questDBContainer.start();
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
        questDBContainer.stop();
    }

    private Map<String, String> baseConnectorProps(String topicName) {
        Map<String, String> props = new HashMap<>();
        props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, QuestDBSinkConnector.class.getName());
        props.put("topics", topicName);
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        props.put("host", questDBContainer.getHost() + ":" + questDBContainer.getMappedPort(QuestDBUtils.QUESTDB_ILP_PORT));
        return props;
    }

    @Test
    public void testSmoke() {
        String topicName = "testsmoke";
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = baseConnectorProps(topicName);
        connect.configureConnector(CONNECTOR_NAME, props);
        assertConnectorTaskRunningEventually();
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

        assertSqlEventually(questDBContainer, "\"firstname\",\"lastname\",\"age\"\r\n"
                        + "\"John\",\"Doe\",42\r\n",
                "select firstname,lastname,age from " + topicName);
    }

    @Test
    public void testPrimitiveKey() {
        String topicName = "mytopic";
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = baseConnectorProps(topicName);
        connect.configureConnector(CONNECTOR_NAME, props);
        assertConnectorTaskRunningEventually();
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

        assertSqlEventually(questDBContainer, "\"firstname\",\"lastname\",\"age\",\"key\"\r\n"
                        + "\"John\",\"Doe\",42,\"key\"\r\n",
                "select firstname, lastname, age, key from " + topicName);
    }

    @Test
    public void testStructKey() {
        String topicName = "mytopic";
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = baseConnectorProps(topicName);
        //overrider the convertor from String to Json
        props.put(KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        connect.configureConnector(CONNECTOR_NAME, props);
        assertConnectorTaskRunningEventually();
        Schema schema = SchemaBuilder.struct().name("com.example.Person")
                .field("firstname", Schema.STRING_SCHEMA)
                .field("lastname", Schema.STRING_SCHEMA)
                .build();

        Struct struct = new Struct(schema)
                .put("firstname", "John")
                .put("lastname", "Doe");

        String json = new String(converter.fromConnectData(topicName, schema, struct));
        connect.kafka().produce(topicName, json, json);

        assertSqlEventually(questDBContainer, "\"firstname\",\"lastname\",\"key_firstname\",\"key_lastname\"\r\n"
                        + "\"John\",\"Doe\",\"John\",\"Doe\"\r\n",
                "select firstname, lastname, key_firstname, key_lastname from " + topicName);
    }

    @Test
    public void testStructKeyAndPrimitiveValue() {
        String topicName = "mytopic";
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = baseConnectorProps(topicName);
        //overrider the convertor from String to Json
        props.put(KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        connect.configureConnector(CONNECTOR_NAME, props);
        assertConnectorTaskRunningEventually();
        Schema schema = SchemaBuilder.struct().name("com.example.Person")
                .field("firstname", Schema.STRING_SCHEMA)
                .field("lastname", Schema.STRING_SCHEMA)
                .build();

        Struct struct = new Struct(schema)
                .put("firstname", "John")
                .put("lastname", "Doe");

        String json = new String(converter.fromConnectData(topicName, schema, struct));
        connect.kafka().produce(topicName, json, "foo");

        assertSqlEventually(questDBContainer, "\"key_firstname\",\"key_lastname\",\"value\"\r\n"
                        + "\"John\",\"Doe\",\"foo\"\r\n",
                "select key_firstname, key_lastname, value from " + topicName);
    }


    @Test
    public void testExplicitTableName() {
        String tableName = "explicitTableName";
        String topicName = "testExplicitTableName";
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = baseConnectorProps(topicName);
        props.put(QuestDBSinkConnectorConfig.TABLE_CONFIG, tableName);
        connect.configureConnector(CONNECTOR_NAME, props);
        assertConnectorTaskRunningEventually();
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

        assertSqlEventually(questDBContainer, "\"firstname\",\"lastname\",\"age\"\r\n"
                        + "\"John\",\"Doe\",42\r\n",
                "select firstname,lastname,age from " + tableName);
    }

    @Test
    public void testLogicalTypes() {
        String topicName = "mytopic";
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = baseConnectorProps(topicName);
        props.put(QuestDBSinkConnectorConfig.TABLE_CONFIG, topicName);
        connect.configureConnector(CONNECTOR_NAME, props);
        assertConnectorTaskRunningEventually();
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

        assertSqlEventually(questDBContainer, "\"firstname\",\"lastname\",\"age\",\"col_timestamp\",\"col_date\",\"col_time\"\r\n"
                        + "\"John\",\"Doe\",42,\"2022-10-23T13:53:59.123000Z\",\"2022-10-23T00:00:00.000000Z\",50039123\r\n",
                "select firstname,lastname,age, col_timestamp, col_date, col_time from " + topicName);
    }

    @Test
    public void testDecimalTypeNotSupported() {
        String topicName = "mytopic";
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = baseConnectorProps(topicName);
        props.put(QuestDBSinkConnectorConfig.TABLE_CONFIG, topicName);
        connect.configureConnector(CONNECTOR_NAME, props);
        assertConnectorTaskRunningEventually();
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

        assertConnectorTaskFailedEventually();
    }

    @Test
    public void testNestedStructInValue() {
        String topicName = "mytopic";
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = baseConnectorProps(topicName);
        props.put(QuestDBSinkConnectorConfig.TABLE_CONFIG, topicName);
        connect.configureConnector(CONNECTOR_NAME, props);
        assertConnectorTaskRunningEventually();

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

        assertSqlEventually(questDBContainer, "\"name_firstname\",\"name_lastname\"\r\n"
                        + "\"John\",\"Doe\"\r\n",
                "select name_firstname, name_lastname from " + topicName);
    }

    @Test
    public void testMultiLevelNestedStructInValue() {
        String topicName = "mytopic";
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = baseConnectorProps(topicName);
        props.put(QuestDBSinkConnectorConfig.TABLE_CONFIG, topicName);
        connect.configureConnector(CONNECTOR_NAME, props);
        assertConnectorTaskRunningEventually();

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

        assertSqlEventually(questDBContainer, "\"partner1_name_firstname\",\"partner1_name_lastname\",\"partner2_name_firstname\",\"partner2_name_lastname\"\r\n"
                        + "\"John\",\"Doe\",\"Jane\",\"Doe\"\r\n",
                "select partner1_name_firstname, partner1_name_lastname, partner2_name_firstname, partner2_name_lastname from " + topicName);
    }

    private void assertConnectorTaskRunningEventually() {
        assertConnectorTaskStateEventually(AbstractStatus.State.RUNNING);
    }

    private void assertConnectorTaskFailedEventually() {
        assertConnectorTaskStateEventually(AbstractStatus.State.FAILED);
    }

    private void assertConnectorTaskStateEventually(AbstractStatus.State expectedState) {
        await().atMost(CONNECTOR_START_TIMEOUT_MS, MILLISECONDS).untilAsserted(() -> assertConnectorTaskState(CONNECTOR_NAME, expectedState));
    }

    private void assertConnectorTaskState(String connectorName, AbstractStatus.State expectedState) {
        ConnectorStateInfo info = connect.connectorStatus(connectorName);
        if (info == null) {
            fail("Connector " + connectorName + " not found");
        }
        List<ConnectorStateInfo.TaskState> taskStates = info.tasks();
        if (taskStates.size() == 0) {
            fail("No tasks found for connector " + connectorName);
        }
        for (ConnectorStateInfo.TaskState taskState : taskStates) {
            if (!Objects.equals(taskState.state(), expectedState.toString())) {
                fail("Task " + taskState.id() + " for connector " + connectorName + " is in state " + taskState.state() + " but expected " + expectedState);
            }
        }
    }
}
