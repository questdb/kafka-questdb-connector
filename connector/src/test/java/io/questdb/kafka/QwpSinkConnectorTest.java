package io.questdb.kafka;

import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.Map;

@Testcontainers(disabledWithoutDocker = true)
class QwpSinkConnectorTest {
    private static final DockerImageName QUESTDB_10 = DockerImageName.parse("questdb/questdb:10.0.0");

    @Container
    private static final GenericContainer<?> QUESTDB = new GenericContainer<>(QUESTDB_10)
            .withExposedPorts(QuestDBUtils.QUESTDB_HTTP_PORT);

    private EmbeddedConnectCluster connect;
    private String topic;

    @BeforeEach
    void setUp() {
        topic = ConnectTestUtils.newTopicName();
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put("plugin.discovery", "hybrid_warn");
        connect = new EmbeddedConnectCluster.Builder()
                .name("questdb-qwp-connect-cluster")
                .workerProps(workerProps)
                .numWorkers(1)
                .build();
        connect.start();
    }

    @AfterEach
    void tearDown() {
        if (connect != null) {
            connect.stop();
        }
    }

    @Test
    void ingestsAndCommitsOverQwp() {
        connect.kafka().createTopic(topic, 1);
        Map<String, String> props = new HashMap<>();
        props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, QuestDBSinkConnector.class.getName());
        props.put(ConnectorConfig.NAME_CONFIG, ConnectTestUtils.CONNECTOR_NAME);
        props.put("topics", topic);
        props.put("tasks.max", "1");
        props.put("key.converter", StringConverter.class.getName());
        props.put("value.converter", JsonConverter.class.getName());
        props.put("value.converter.schemas.enable", "false");
        props.put(QuestDBSinkConnectorConfig.INCLUDE_KEY_CONFIG, "false");
        props.put(QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG,
                "ws::addr=" + QUESTDB.getHost() + ':' + QUESTDB.getMappedPort(QuestDBUtils.QUESTDB_HTTP_PORT)
                        + ";auto_flush_rows=1;sf_max_total_bytes=67108864;");

        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);
        connect.kafka().produce(topic, "key", "{\"city\":\"Berlin\",\"temperature\":21.5}");

        QuestDBUtils.assertSqlEventually(
                "\"city\",\"temperature\"\r\n\"Berlin\",21.5\r\n",
                "select city, temperature from " + topic,
                QUESTDB.getMappedPort(QuestDBUtils.QUESTDB_HTTP_PORT));
    }
}
