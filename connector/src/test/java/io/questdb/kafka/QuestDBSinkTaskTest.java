package io.questdb.kafka;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;

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

    @Test
    void qwpFailsFastWhenOriginalCoordinateApiIsMissing() {
        ConnectException failure = assertThrows(
                ConnectException.class,
                () -> QuestDBSinkTask.requireOriginalCoordinatesApi(Object.class));
        assertTrue(failure.getMessage().contains("3.6 or newer"));
    }
}
