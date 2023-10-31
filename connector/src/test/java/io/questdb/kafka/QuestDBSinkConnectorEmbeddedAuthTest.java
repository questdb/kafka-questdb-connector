package io.questdb.kafka;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import java.util.Map;

import static java.util.Collections.singletonMap;

@Testcontainers
public class QuestDBSinkConnectorEmbeddedAuthTest {
    private EmbeddedConnectCluster connect;
    private Converter converter;
    private String topicName;

    // must match the user in authDb.txt
    private static final String TEST_USER_TOKEN = "UvuVb1USHGRRT08gEnwN2zGZrvM4MsLQ5brgF6SVkAw=";
    private static final String TEST_USER_NAME = "testUser1";

    @Container
    private static GenericContainer<?> questDBContainer = newQuestDbConnector();

    private static GenericContainer<?> newQuestDbConnector() {
        FixedHostPortGenericContainer<?> container = new FixedHostPortGenericContainer<>("questdb/questdb:7.3");
        container.addExposedPort(QuestDBUtils.QUESTDB_HTTP_PORT);
        container.addExposedPort(QuestDBUtils.QUESTDB_ILP_PORT);
        container.setWaitStrategy(new LogMessageWaitStrategy().withRegEx(".*server-main enjoy.*"));
        container.withCopyFileToContainer(MountableFile.forClasspathResource("/authDb.txt"), "/var/lib/questdb/conf/authDb.txt");
        container.withEnv("QDB_LINE_TCP_AUTH_DB_PATH", "conf/authDb.txt");
        return container.withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("questdb")));
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

    @Test
    public void testSmoke() {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName);
        props.put(QuestDBSinkConnectorConfig.USERNAME, TEST_USER_NAME);
        props.put(QuestDBSinkConnectorConfig.TOKEN, TEST_USER_TOKEN);

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
                questDBContainer.getMappedPort(QuestDBUtils.QUESTDB_HTTP_PORT));
    }
}
