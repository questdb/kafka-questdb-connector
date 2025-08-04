package io.questdb.kafka;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
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

    private final static Network network = Network.newNetwork();

    @Container
    private static final GenericContainer<?> questDBContainer = newQuestDbConnector();
    @Container
    private static final GenericContainer<?> tlsProxy = newTlsProxyContainer();

    private static GenericContainer<?> newQuestDbConnector() {
        FixedHostPortGenericContainer<?> container = new FixedHostPortGenericContainer<>("questdb/questdb:9.0.1");
        container.addExposedPort(QuestDBUtils.QUESTDB_HTTP_PORT);
        container.addExposedPort(QuestDBUtils.QUESTDB_ILP_PORT);
        container.setWaitStrategy(new LogMessageWaitStrategy().withRegEx(".*server-main enjoy.*"));
        container.withCopyFileToContainer(MountableFile.forClasspathResource("/authDb.txt"), "/var/lib/questdb/conf/authDb.txt")
                .withEnv("QDB_LINE_TCP_AUTH_DB_PATH", "conf/authDb.txt")
                .withNetwork(network)
                .withNetworkAliases("questdb");
        return container.withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("questdb")));
    }

    public static GenericContainer<?> newTlsProxyContainer() {
        return new GenericContainer<>("hitch")
                .withCommand("--backend=[questdb]:9009 --write-proxy-v2=off")
                .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("hitch")))
                .withExposedPorts(443)
                .withNetwork(network)
                .dependsOn(questDBContainer)
                .waitingFor(new HostPortWaitStrategy().forPorts(443));
    }

    @AfterAll
    public static void stopTLS() {
        tlsProxy.stop();
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

    @ParameterizedTest(name = "useTls = {0}")
    @ValueSource(booleans = {false, true})
    public void testSmoke(boolean useTls) {
        connect.kafka().createTopic(topicName, 1);
        Map<String, String> props = ConnectTestUtils.baseConnectorProps(questDBContainer, topicName, false);
        String confString;
        if (useTls) {
            confString = "tcps::addr=localhost:" + tlsProxy.getMappedPort(443) + ";protocol_version=2;tls_verify=unsafe_off;";
        } else {
            confString = "tcp::addr=" + questDBContainer.getHost() + ":" + questDBContainer.getMappedPort(QuestDBUtils.QUESTDB_ILP_PORT) + ";protocol_version=2;";
        }
        confString += "username=" + TEST_USER_NAME + ";token=" + TEST_USER_TOKEN + ";";
        // override the original confString with the one that has auth info
        props.put("client.conf.string", confString);

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
