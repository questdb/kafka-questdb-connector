package io.questdb.kafka;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QuestDBSinkTaskTest {
    @Test
    void connectorAlwaysPublishesStableDispatcherClass() {
        assertEquals(QuestDBSinkTask.class, new QuestDBSinkConnector().taskClass());
    }

    @Test
    void currentRuntimeProvidesOriginalCoordinateApi() {
        assertDoesNotThrow(() -> QuestDBSinkTask.requireOriginalCoordinatesApi());
    }

    /**
     * The QuestDB client implements the whole Sender contract over QWP, so the legacy
     * task can drive a ws:: sender end to end without noticing - which would make every
     * QWP-parameterized test pass while the QWP task never runs. Each task therefore
     * checks the transport it serves, so a mis-routed configuration fails loudly instead
     * of silently degrading to the wrong delivery model.
     */
    @Test
    void legacyTaskRefusesQwpConfiguration() {
        for (String confStr : new String[]{"ws::addr=localhost:9000;", "wss::addr=localhost:9000;"}) {
            ConfigException failure = assertThrows(
                    ConfigException.class,
                    () -> new LegacyQuestDBSinkTask().start(props(confStr)),
                    confStr + " must not be served by the legacy task");
            assertTrue(failure.getMessage().contains("require the QWP task"), failure.getMessage());
        }
    }

    @Test
    void qwpTaskRefusesLegacyConfiguration() {
        for (String confStr : new String[]{"http::addr=localhost:9000;", "tcp::addr=localhost:9009;"}) {
            ConfigException failure = assertThrows(
                    ConfigException.class,
                    () -> new QwpSinkTask().start(props(confStr)),
                    confStr + " must not be served by the QWP task");
            assertTrue(failure.getMessage().contains("requires a ws:: or wss::"), failure.getMessage());
        }
    }

    private static Map<String, String> props(String confStr) {
        Map<String, String> props = new HashMap<>();
        props.put("client.conf.string", confStr);
        props.put("topics", "tab");
        props.put("table", "tab");
        return props;
    }

    @Test
    void qwpFailsFastWhenOriginalCoordinateApiIsMissing() {
        ConnectException failure = assertThrows(
                ConnectException.class,
                () -> QuestDBSinkTask.requireOriginalCoordinatesApi(Object.class));
        assertTrue(failure.getMessage().contains("3.6 or newer"));
    }
}
