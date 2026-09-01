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

    enum Transport {
        HTTP, TCP, QWP
    }

    static java.util.stream.Stream<Transport> defaultTransports() {
        String override = System.getProperty("questdb.test.transports");
        if (override == null || override.trim().isEmpty()) {
            return java.util.stream.Stream.of(Transport.HTTP, Transport.QWP, Transport.TCP);
        }
        return java.util.Arrays.stream(override.split(","))
                .map(String::trim)
                .map(Transport::valueOf);
    }

    static Map<String, String> baseConnectorProps(GenericContainer<?> questDBContainer, String topicName, boolean useHttp) {
        return baseConnectorProps(questDBContainer, topicName, useHttp ? Transport.HTTP : Transport.TCP);
    }

    static Map<String, String> baseConnectorProps(GenericContainer<?> questDBContainer, String topicName, Transport transport) {
        String host = questDBContainer.getHost();

        Map<String, String> props = new HashMap<>();
        props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, QuestDBSinkConnector.class.getName());
        props.put("topics", topicName);
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        // Tests write tiny batches; the default 1s allowed.lag would delay the
        // timer-driven flush (and thus every visibility assert) by ~1s.
        props.put(QuestDBSinkConnectorConfig.ALLOWED_LAG_CONFIG, "100");

        String confString;
        switch (transport) {
            case HTTP:
                confString = "http::addr=" + host + ":" + questDBContainer.getMappedPort(QuestDBUtils.QUESTDB_HTTP_PORT) + ";";
                break;
            case TCP:
                confString = "tcp::addr=" + host + ":" + questDBContainer.getMappedPort(QuestDBUtils.QUESTDB_ILP_PORT) + ";protocol_version=2;";
                break;
            case QWP:
                // QWP is WebSocket over the HTTP port
                confString = "ws::addr=" + host + ":" + questDBContainer.getMappedPort(QuestDBUtils.QUESTDB_HTTP_PORT) + ";";
                break;
            default:
                throw new IllegalArgumentException("Unknown transport: " + transport);
        }
        props.put("client.conf.string", confString);
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
                fail("Task " + taskState.id() + " for connector " + connectorName + " is in state " + taskState.state()
                        + " but expected " + expectedState + ". Trace: " + singleLine(taskState.trace()));
            }
        }
    }

    /**
     * A task trace is a full stack trace. Embedding one in a failure message corrupts
     * surefire's fork channel ("Corrupted channel by directly writing to native stream"),
     * and a corrupted channel loses every result for the class: the run then reports
     * "Tests run: 0" and the build goes GREEN despite a genuine failure. Measured on this
     * suite: a ~2000 char trace corrupts, 300 does not. The head of the trace is also the
     * useful part - exception type, message, and the frame that threw.
     */
    private static String singleLine(String trace) {
        if (trace == null || trace.isEmpty()) {
            return "<no trace>";
        }
        String flattened = trace.replaceAll("\\R+", " | ").replace('\t', ' ');
        return flattened.length() <= 300 ? flattened : flattened.substring(0, 300) + " ...(truncated)";
    }

    static String newTopicName() {
        return "topic" + ID_GEN.getAndIncrement();
    }

    static String newTableName() {
        return "table" + ID_GEN.getAndIncrement();
    }
}
