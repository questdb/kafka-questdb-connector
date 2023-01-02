package io.questdb.kafka;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class QuestDBSinkConnectorConfigTest {

    @Test
    public void testTimeunitsValidator() {
        ConfigDef conf = QuestDBSinkConnectorConfig.conf();
        ConfigDef.ConfigKey configKey = conf.configKeys().get(QuestDBSinkConnectorConfig.TIMESTAMP_UNITS_CONFIG);

        // positive cases
        configKey.validator.ensureValid(QuestDBSinkConnectorConfig.TIMESTAMP_UNITS_CONFIG, "auto");
        configKey.validator.ensureValid(QuestDBSinkConnectorConfig.TIMESTAMP_UNITS_CONFIG, "millis");
        configKey.validator.ensureValid(QuestDBSinkConnectorConfig.TIMESTAMP_UNITS_CONFIG, "micros");
        configKey.validator.ensureValid(QuestDBSinkConnectorConfig.TIMESTAMP_UNITS_CONFIG, "nanos");

        // negative cases
        try {
            configKey.validator.ensureValid(QuestDBSinkConnectorConfig.TIMESTAMP_UNITS_CONFIG, "foo");
            fail("Expected ConfigException");
        } catch (ConfigException e) {
            assertEquals("Invalid value foo for configuration timestamp.units: String must be one of: auto, millis, micros, nanos", e.getMessage());
        }
    }

    @Test
    public void testTimeunitsRecommender() {
        ConfigDef conf = QuestDBSinkConnectorConfig.conf();
        ConfigDef.ConfigKey configKey = conf.configKeys().get(QuestDBSinkConnectorConfig.TIMESTAMP_UNITS_CONFIG);
        List<Object> objects = configKey.recommender.validValues(QuestDBSinkConnectorConfig.TIMESTAMP_UNITS_CONFIG, new HashMap<>());

        assertEquals(Arrays.asList("auto", "millis", "micros", "nanos"), objects);
    }

    @Test
    public void foo() {
        ConfigDef conf = QuestDBSinkConnectorConfig.conf();
        Map<String, ConfigDef.ConfigKey> stringConfigKeyMap = conf.configKeys();
        for (Map.Entry<String, ConfigDef.ConfigKey> entry : stringConfigKeyMap.entrySet()) {
            ConfigDef.ConfigKey value = entry.getValue();

            System.out.println(entry.getKey());
        }
    }
}