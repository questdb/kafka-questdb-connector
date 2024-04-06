package io.questdb.kafka;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.storage.StringConverter;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.*;

public class QuestDBSinkConnectorConfigTest {

    @Test
    public void testClientConfigurationStringCannotBeCombinedWithExplicitClientConfig() {
        assertCannotBeSetTogetherWithConfigString(QuestDBSinkConnectorConfig.HOST_CONFIG, "localhost");
        assertCannotBeSetTogetherWithConfigString(QuestDBSinkConnectorConfig.USERNAME, "joe");
        assertCannotBeSetTogetherWithConfigString(QuestDBSinkConnectorConfig.TOKEN, "secret");
        assertCannotBeSetTogetherWithConfigString(QuestDBSinkConnectorConfig.TLS, "true");
        assertCannotBeSetTogetherWithConfigString(QuestDBSinkConnectorConfig.TLS, "false");
        assertCannotBeSetTogetherWithConfigString(QuestDBSinkConnectorConfig.TLS_VALIDATION_MODE_CONFIG, "default");
        assertCannotBeSetTogetherWithConfigString(QuestDBSinkConnectorConfig.TLS_VALIDATION_MODE_CONFIG, "insecure");
    }

    @Test
    public void testEitherHostOrClientConfigStringMustBeSet() {
        Map<String, String> config = baseConnectorProps();
        QuestDBSinkConnector connector = new QuestDBSinkConnector();
        try {
            connector.validate(config);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("Either 'client.conf.string' or 'host' must be set.", e.getMessage());
        }
    }

    private void assertCannotBeSetTogetherWithConfigString(String configKey, String configValue) {
        Map<String, String> config = baseConnectorProps();
        config.put(QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG, "http::addr=localhost;");
        config.put(configKey, configValue);

        QuestDBSinkConnector connector = new QuestDBSinkConnector();
        try {
            connector.validate(config);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("Only one of '" + configKey + "' or 'client.conf.string' must be set.", e.getMessage());
        }
    }

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
            assertEquals("Invalid value foo for configuration timestamp.units: String must be one of: auto, seconds, millis, micros, nanos", e.getMessage());
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
    public void testTlsConfig() {
        ConfigDef confDef = QuestDBSinkConnectorConfig.conf();
        Map<String, String> config = baseConnectorProps();
        config.put("client.conf.string", "http::addr=localhost;tls=true");
        config.put("tls", "true");
        QuestDBSinkConnectorConfig sinkConnectorConfig = new QuestDBSinkConnectorConfig(confDef, config);

        assertTrue(sinkConnectorConfig.isTls());
    }

    @Test
    public void testTlsValidationModeValidation() {
        ConfigDef conf = QuestDBSinkConnectorConfig.conf();
        ConfigDef.ConfigKey configKey = conf.configKeys().get(QuestDBSinkConnectorConfig.TLS_VALIDATION_MODE_CONFIG);

        // positive cases
        configKey.validator.ensureValid(QuestDBSinkConnectorConfig.TLS_VALIDATION_MODE_CONFIG, "default");
        configKey.validator.ensureValid(QuestDBSinkConnectorConfig.TLS_VALIDATION_MODE_CONFIG, "insecure");

        // negative cases
        try {
            configKey.validator.ensureValid(QuestDBSinkConnectorConfig.TLS_VALIDATION_MODE_CONFIG, "foo");
            fail("Expected ConfigException");
        } catch (ConfigException e) {
            assertEquals("Invalid value foo for configuration tls.validation.mode: String must be one of: default, insecure", e.getMessage());
        }
    }

    @Test
    public void testExplicitTablenameValidation() {
        ConfigDef confDef = QuestDBSinkConnectorConfig.conf();
        Map<String, String> config = baseConnectorProps();
        config.put("client.conf.string", "http::addr=localhost;tls=true");
        // positive case I - valid explicit table name
        ConfigValue configValue = confDef.validate(config).stream().filter(c -> c.name().equals(QuestDBSinkConnectorConfig.TABLE_CONFIG)).findFirst().get();
        assertTrue(configValue.errorMessages().isEmpty());

        // positive case II - missing explicit table name
        config = baseConnectorProps();
        config.put("client.conf.string", "http::addr=localhost;tls=true");
        config.remove(QuestDBSinkConnectorConfig.TABLE_CONFIG);
        configValue = confDef.validate(config).stream().filter(c -> c.name().equals(QuestDBSinkConnectorConfig.TABLE_CONFIG)).findFirst().get();
        assertTrue(configValue.errorMessages().isEmpty());

        // negative case - invalid characters in explicit table name
        config = baseConnectorProps();
        config.put("client.conf.string", "http::addr=localhost;tls=true");
        config.put(QuestDBSinkConnectorConfig.TABLE_CONFIG, "not?valid");
        configValue = confDef.validate(config).stream().filter(c -> c.name().equals(QuestDBSinkConnectorConfig.TABLE_CONFIG)).findFirst().get();
        assertEquals(1, configValue.errorMessages().size());
        assertTrue(configValue.errorMessages().get(0).contains("Invalid value not?valid for configuration table"));
    }

    private Map<String, String> baseConnectorProps() {
        Map<String, String> props = new HashMap<>();
        props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, QuestDBSinkConnector.class.getName());
        props.put("topics", "myTopic");
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        return props;
    }
}