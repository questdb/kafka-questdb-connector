package io.questdb.kafka;

import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.awaitility.Awaitility;
import org.testcontainers.containers.GenericContainer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.fail;

public final class ConnectTestUtils {
    public static final long CONNECTOR_START_TIMEOUT_MS = SECONDS.toMillis(60);
    public static final String CONNECTOR_NAME = "questdb-sink-connector";
    private static final AtomicInteger ID_GEN = new AtomicInteger(0);

    private ConnectTestUtils() {
    }

    static void assertConnectorTaskRunningEventually(EmbeddedConnectCluster connect) {
        assertConnectorTaskStateEventually(connect, AbstractStatus.State.RUNNING);
    }

    static void assertConnectorTaskFailedEventually(EmbeddedConnectCluster connect) {
        assertConnectorTaskStateEventually(connect, AbstractStatus.State.FAILED);
    }

    static void assertConnectorTaskStateEventually(EmbeddedConnectCluster connect, AbstractStatus.State expectedState) {
        Awaitility.await().atMost(CONNECTOR_START_TIMEOUT_MS, MILLISECONDS).untilAsserted(() -> assertConnectorTaskState(connect, CONNECTOR_NAME, expectedState));
    }

    static Map<String, String> baseConnectorProps(GenericContainer<?> questDBContainer, String topicName, boolean useHttp) {
        String host = questDBContainer.getHost();

        Map<String, String> props = new HashMap<>();
        props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, QuestDBSinkConnector.class.getName());
        props.put("topics", topicName);
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());

        String confString;
        if (useHttp) {
            int port = questDBContainer.getMappedPort(QuestDBUtils.QUESTDB_HTTP_PORT);
            confString = "http::addr=" + host + ":" + port + ";";
            props.put("client.conf.string", confString);
        } else {
            int port = questDBContainer.getMappedPort(QuestDBUtils.QUESTDB_ILP_PORT);
            confString = "tcp::addr=" + host + ":" + port + ";protocol_version=2;";
            props.put("client.conf.string", confString);
        }
        return props;
    }

    static void assertConnectorTaskState(EmbeddedConnectCluster connect, String connectorName, AbstractStatus.State expectedState) {
        ConnectorStateInfo info = null;
        try {
            info = connect.connectorStatus(connectorName);
        } catch (ConnectRestException e) {
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

    static String newTopicName() {
        return "topic" + ID_GEN.getAndIncrement();
    }

    static String newTableName() {
        return "table" + ID_GEN.getAndIncrement();
    }
}
