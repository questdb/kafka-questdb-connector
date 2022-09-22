package org.questdb.kafka;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
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

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.kafka.connect.runtime.AbstractStatus.State.RUNNING;
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

    private static final GenericContainer questDBContainer = new GenericContainer("questdb/questdb:6.5.2")
            .withExposedPorts(QuestDBUtils.QUESTDB_HTTP_PORT, QuestDBUtils.QUESTDB_ILP_PORT)
            .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("questdb")))
            .withEnv("QDB_CAIRO_COMMIT_LAG", "100")
            .withEnv("JAVA_OPTS", "-Djava.locale.providers=JRE,SPI");

    @BeforeAll
    public static void beforeAll() {
        questDBContainer.start();
    }

    @BeforeEach
    public void setUp() {
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
        assertConnectorStartsEventually();
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
    public void testExplicitTableName() {
        String tableName = "explicitTableName";
        String topicName = "testExplicitTableName";
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = baseConnectorProps(topicName);
        props.put(QuestDBSinkConnectorConfig.TABLE_CONFIG, tableName);
        connect.configureConnector(CONNECTOR_NAME, props);
        assertConnectorStartsEventually();
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

    private void assertConnectorStartsEventually() {
        await().atMost(CONNECTOR_START_TIMEOUT_MS, MILLISECONDS).untilAsserted(() -> assertConnectorRunning(CONNECTOR_NAME));
    }

    private void assertConnectorRunning(String connectorName) {
        ConnectorStateInfo info = connect.connectorStatus(connectorName);
        if (info == null || !info.connector().state().equals(RUNNING.toString())
                || info.tasks().stream().anyMatch(s -> !s.state().equals(RUNNING.toString()))) {
            fail("Connector " + connectorName + " or its tasks are not in RUNNING state. Connector info: " + info);
        }
    }
}
