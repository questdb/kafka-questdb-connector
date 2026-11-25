package io.questdb.kafka;

import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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

    /**
     * The integer case matters as much as the text one: the key is rendered to text on both
     * sides, by the lookup load and by the record conversion, and the two must agree.
     */
    @ParameterizedTest
    @ValueSource(strings = {"LONG", "VARCHAR"})
    void enrichesRowsFromLookupTable(String keyType) throws InterruptedException {
        int httpPort = QUESTDB.getMappedPort(QuestDBUtils.QUESTDB_HTTP_PORT);
        boolean textKey = keyType.equals("VARCHAR");
        String lookup = topic + "_sec_list";
        QuestDBUtils.assertSql("{\"ddl\":\"OK\"}",
                "create table " + lookup + " (sec_id " + keyType + ", symbol SYMBOL, timestamp TIMESTAMP) timestamp(timestamp) partition by day wal",
                httpPort, QuestDBUtils.Endpoint.EXEC);
        QuestDBUtils.assertSql("{\"dml\":\"OK\"}",
                "insert into " + lookup + " values (" + sqlKey(1, textKey) + ", 'AAPL', now())",
                httpPort, QuestDBUtils.Endpoint.EXEC);
        QuestDBUtils.assertSqlEventually("\"count()\"\r\n1\r\n", "select count() from " + lookup, httpPort);

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
        props.put(QuestDBSinkConnectorConfig.ENRICHMENT_SQL_CONFIG,
                "SELECT sec_id, symbol AS ticker FROM " + lookup + " LATEST ON timestamp PARTITION BY sec_id");
        props.put(QuestDBSinkConnectorConfig.ENRICHMENT_KEY_FIELD_CONFIG, "sec_id");
        props.put(QuestDBSinkConnectorConfig.ENRICHMENT_REFRESH_MS_CONFIG, "500");
        props.put(QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG,
                "ws::addr=" + QUESTDB.getHost() + ':' + httpPort + ";auto_flush_rows=1;sf_max_total_bytes=67108864;");

        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);
        connect.kafka().produce(topic, "key", "{\"sec_id\":" + jsonKey(1, textKey) + ",\"px\":1.5}");
        connect.kafka().produce(topic, "key", "{\"sec_id\":" + jsonKey(2, textKey) + ",\"px\":2.5}");

        String firstRows = csvKey(1, textKey) + ",1.5,\"AAPL\"\r\n"
                + csvKey(2, textKey) + ",2.5,\r\n"; // no mapping yet: written at once, with a null
        String query = "select sec_id, px, ticker from " + topic + " order by px";
        QuestDBUtils.assertSqlEventually("\"sec_id\",\"px\",\"ticker\"\r\n" + firstRows, query, httpPort);
        QuestDBUtils.assertSql("\"type\"\r\n\"SYMBOL\"\r\n",
                "select type from table_columns('" + topic + "') where \"column\" = 'ticker'", httpPort);

        QuestDBUtils.assertSql("{\"dml\":\"OK\"}",
                "insert into " + lookup + " values (" + sqlKey(2, textKey) + ", 'MSFT', now())",
                httpPort, QuestDBUtils.Endpoint.EXEC);
        QuestDBUtils.assertSqlEventually("\"count()\"\r\n2\r\n", "select count() from " + lookup, httpPort);
        Thread.sleep(1_000); // two enrichment.refresh.ms: the refresher has reloaded before the next batch
        connect.kafka().produce(topic, "key", "{\"sec_id\":" + jsonKey(2, textKey) + ",\"px\":3.5}");

        // the new mapping applies from now on; the row written without it stays null
        QuestDBUtils.assertSqlEventually("\"sec_id\",\"px\",\"ticker\"\r\n" + firstRows
                + csvKey(2, textKey) + ",3.5,\"MSFT\"\r\n", query, httpPort);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);
    }

    private static String csvKey(int id, boolean text) {
        return text ? "\"" + id + "\"" : String.valueOf(id);
    }

    private static String jsonKey(int id, boolean text) {
        return csvKey(id, text);
    }

    private static String sqlKey(int id, boolean text) {
        return text ? "'" + id + "'" : String.valueOf(id);
    }

    @Test
    void resumesConsumptionAfterBackpressureSettles() {
        connect.kafka().createTopic(topic, 1);
        for (int i = 0; i < 5_000; i++) {
            connect.kafka().produce(topic, "key", "{\"id\":" + i + "}");
        }

        Map<String, String> props = new HashMap<>();
        props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, QuestDBSinkConnector.class.getName());
        props.put(ConnectorConfig.NAME_CONFIG, ConnectTestUtils.CONNECTOR_NAME);
        props.put("topics", topic);
        props.put("tasks.max", "1");
        props.put("key.converter", StringConverter.class.getName());
        props.put("value.converter", JsonConverter.class.getName());
        props.put("value.converter.schemas.enable", "false");
        props.put(QuestDBSinkConnectorConfig.INCLUDE_KEY_CONFIG, "false");
        props.put(QuestDBSinkConnectorConfig.QWP_MAX_INFLIGHT_ROWS_CONFIG, "1");
        props.put(QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG,
                "ws::addr=" + QUESTDB.getHost() + ':' + QUESTDB.getMappedPort(QuestDBUtils.QUESTDB_HTTP_PORT)
                        + ";auto_flush_rows=500;auto_flush_interval=25;sf_max_total_bytes=67108864;");

        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME, props);
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);

        QuestDBUtils.assertSqlEventually(
                "\"count()\"\r\n5000\r\n",
                "select count() from " + topic,
                QUESTDB.getMappedPort(QuestDBUtils.QUESTDB_HTTP_PORT));
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);
    }
}
