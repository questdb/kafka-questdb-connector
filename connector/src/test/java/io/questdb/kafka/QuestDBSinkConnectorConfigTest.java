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
import java.util.function.Function;

import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.*;

public class QuestDBSinkConnectorConfigTest {

    @Test
    public void testQwpDefaultsAndRanges() {
        Map<String, String> props = baseConnectorProps();
        props.put(QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG, "ws::addr=localhost:9000;");
        QuestDBSinkConnectorConfig config = new QuestDBSinkConnectorConfig(props);
        assertEquals(300_000L, config.getQwpProgressTimeoutMs());
        assertEquals(150_000, config.getQwpMaxInflightRows());
        assertEquals(500L, config.getQwpCommitAckTimeoutMs());
        assertEquals(1_000L, config.getQwpQuarantineAckTimeoutMs());

        props.put(QuestDBSinkConnectorConfig.QWP_COMMIT_ACK_TIMEOUT_MS_CONFIG, "-1");
        assertThrows(ConfigException.class, () -> new QuestDBSinkConnectorConfig(props));
        props.put(QuestDBSinkConnectorConfig.QWP_COMMIT_ACK_TIMEOUT_MS_CONFIG, "0");
        assertEquals(0L, new QuestDBSinkConnectorConfig(props).getQwpCommitAckTimeoutMs());

        props.put(QuestDBSinkConnectorConfig.QWP_PROGRESS_TIMEOUT_MS_CONFIG, "0");
        assertThrows(ConfigException.class, () -> new QuestDBSinkConnectorConfig(props));
        props.put(QuestDBSinkConnectorConfig.QWP_PROGRESS_TIMEOUT_MS_CONFIG, "1");
        props.put(QuestDBSinkConnectorConfig.QWP_MAX_INFLIGHT_ROWS_CONFIG, "0");
        assertThrows(ConfigException.class, () -> new QuestDBSinkConnectorConfig(props));
        props.put(QuestDBSinkConnectorConfig.QWP_MAX_INFLIGHT_ROWS_CONFIG, "1");
        props.put(QuestDBSinkConnectorConfig.QWP_QUARANTINE_ACK_TIMEOUT_MS_CONFIG, "0");
        assertThrows(ConfigException.class, () -> new QuestDBSinkConnectorConfig(props));
    }

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
        assertEquals(Arrays.asList("Either 'client.conf.string' or 'host' must be set."), clientConfErrors(connector, config));
    }

    @Test
    public void testValidateRejectsBadClientConfigurationStringBeforeTaskStart() {
        assertValidateRejectsConfString("ws::addr=localhost:9000;sf_dir=/var/lib/qdb;",
                "QuestDB Kafka connector supports memory-only store-and-forward; sf_dir is not allowed with QWP");
        assertValidateRejectsConfString("wss::addr=localhost:9000;sf_durability=sync;",
                "QuestDB Kafka connector supports memory-only store-and-forward; sf_durability is not allowed with QWP");
        assertValidateRejectsConfString("ws::addr=localhost:9000;initial_connect_retry=on;",
                "QuestDB Kafka connector requires initial_connect_retry=off for QWP");
        assertValidateRejectsConfString("ws::addr=localhost:9000;auto_flush_rows=off;",
                "QuestDB Kafka connector cannot have auto_flush_rows disabled");
        assertValidateRejectsConfString("http::addr=localhost:9000;auto_flush_interval=off;",
                "QuestDB Kafka connector cannot have auto_flush_interval disabled");
    }

    @Test
    public void testValidateRejectsAppendDeadlineAtOrAbovePollInterval() {
        Map<String, String> config = baseConnectorProps();
        config.put(QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG, "ws::addr=localhost:9000;sf_append_deadline_millis=30000;");
        config.put("consumer.override.max.poll.interval.ms", "30000");
        QuestDBSinkConnector connector = new QuestDBSinkConnector();
        assertEquals(Arrays.asList("sf_append_deadline_millis must be lower than consumer.override.max.poll.interval.ms"),
                clientConfErrors(connector, config));

        config.put("consumer.override.max.poll.interval.ms", "30001");
        assertTrue(clientConfErrors(connector, config).isEmpty());
    }

    @Test
    public void testValidateAcceptsValidClientConfigurationStrings() {
        for (String confStr : Arrays.asList(
                "ws::addr=localhost:9000;",
                "wss::addr=localhost:9000;sf_max_total_bytes=268435456;auto_flush_rows=1000;",
                "http::addr=localhost:9000;",
                "tcp::addr=localhost:9009;")) {
            Map<String, String> config = baseConnectorProps();
            config.put(QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG, confStr);
            assertTrue(clientConfErrors(new QuestDBSinkConnector(), config).isEmpty());
        }
    }

    @Test
    public void testValidateLeavesUnresolvableEnvironmentVariablesToTheTask() {
        // the variable may exist only on the worker that runs the task
        Map<String, String> config = baseConnectorProps();
        config.put(QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG, "ws::addr=${QDB_KAFKA_TEST_UNDEFINED_HOST}:9000;sf_dir=/var/lib/qdb;");
        assertTrue(clientConfErrors(new QuestDBSinkConnector(), config).isEmpty());
    }

    @Test
    public void testValidateRejectsMalformedEnvironmentReferences() {
        for (String suffix : Arrays.asList("${};", "${HOST;", "${BAD-NAME};",
                "${QDB_KAFKA_TEST_UNDEFINED_HOST};token=${};")) {
            Map<String, String> config = baseConnectorProps();
            config.put(QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG, "ws::addr=" + suffix);
            assertFalse(clientConfErrors(new QuestDBSinkConnector(), config).isEmpty(), suffix);
        }
    }

    @Test
    public void testValidateTrimsClientConfigurationLikeTaskStartup() {
        assertValidateRejectsConfString("  ws::addr=localhost:9000;sf_dir=/var/lib/qdb;  ",
                "QuestDB Kafka connector supports memory-only store-and-forward; sf_dir is not allowed with QWP");
    }

    @Test
    public void testClientConfStringAndEnvironmentVariableCannotBeCombined() {
        Map<String, String> props = baseConnectorProps();
        props.put(QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG, "ws::addr=localhost:9000;");
        Function<String, String> env = name -> "QDB_CLIENT_CONF".equals(name) ? "http::addr=localhost:9000;" : null;
        assertEquals(Arrays.asList("Only one of 'client.conf.string' or QDB_CLIENT_CONF environment variable"
                        + " must be set. They cannot be used together."),
                confStringErrors(new QuestDBSinkConnector().validate(props, env)));
        // the same properties are clean on a worker that does not export the variable
        assertTrue(confStringErrors(new QuestDBSinkConnector().validate(props, NO_ENV)).isEmpty());
    }

    @Test
    public void testValidateReportsEveryConflictingClientSetting() {
        Map<String, String> props = baseConnectorProps();
        props.put(QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG, "ws::addr=localhost:9000;");
        props.put(QuestDBSinkConnectorConfig.HOST_CONFIG, "localhost");
        props.put(QuestDBSinkConnectorConfig.TOKEN, "secret");
        props.put(QuestDBSinkConnectorConfig.USERNAME, "admin");
        QuestDBSinkConnector connector = new QuestDBSinkConnector();
        // every offending key is reported in one pass, so the operator does not fix them one 400 at a time
        for (String name : Arrays.asList(QuestDBSinkConnectorConfig.HOST_CONFIG,
                QuestDBSinkConnectorConfig.TOKEN, QuestDBSinkConnectorConfig.USERNAME)) {
            assertEquals(Arrays.asList("Only one of '" + name + "' or 'client.conf.string' must be set."),
                    fieldErrors(connector, props, name), name);
        }
    }

    /** No worker environment, so an exported QDB_CLIENT_CONF cannot leak into the assertions. */
    private static final Function<String, String> NO_ENV = name -> null;

    private List<String> clientConfErrors(QuestDBSinkConnector connector, Map<String, String> config) {
        return connector.validate(config, NO_ENV).configValues().stream()
                .filter(value -> value.name().equals(QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG))
                .findFirst().orElseThrow().errorMessages();
    }

    @Test
    public void testValidateRejectsUnparseablePollInterval() {
        Map<String, String> config = baseConnectorProps();
        config.put(QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG, "ws::addr=localhost:9000;sf_append_deadline_millis=1000;");
        config.put("consumer.override.max.poll.interval.ms", "30s");
        assertEquals(1, clientConfErrors(new QuestDBSinkConnector(), config).size());
        assertTrue(clientConfErrors(new QuestDBSinkConnector(), config).get(0).contains("must be a long"));
    }

    @Test
    public void testDefaultAppendDeadlineIsCheckedAgainstPollInterval() {
        // no sf_append_deadline_millis in the string: the patched default must reach the comparison
        Map<String, String> config = baseConnectorProps();
        config.put(QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG, "ws::addr=localhost:9000;");
        // literal on purpose: deriving it from the constant would make the mutant move the expectation
        assertEquals(30_000L, ClientConfUtils.DEFAULT_QWP_SF_APPEND_DEADLINE_MILLIS);
        config.put("consumer.override.max.poll.interval.ms", "30000");
        assertEquals(Arrays.asList("sf_append_deadline_millis must be lower than consumer.override.max.poll.interval.ms"),
                clientConfErrors(new QuestDBSinkConnector(), config));

        config.put("consumer.override.max.poll.interval.ms", "30001");
        assertTrue(clientConfErrors(new QuestDBSinkConnector(), config).isEmpty());
    }

    private List<String> confStringErrors(org.apache.kafka.common.config.Config config) {
        return config.configValues().stream()
                .filter(value -> value.name().equals(QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG))
                .findFirst().orElseThrow().errorMessages();
    }

    private void assertValidateRejectsConfString(String confStr, String expectedMessage) {
        Map<String, String> config = baseConnectorProps();
        config.put(QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG, confStr);
        QuestDBSinkConnector connector = new QuestDBSinkConnector();
        assertEquals(Arrays.asList(expectedMessage), clientConfErrors(connector, config));
    }

    private void assertCannotBeSetTogetherWithConfigString(String configKey, String configValue) {
        Map<String, String> config = baseConnectorProps();
        config.put(QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG, "http::addr=localhost;");
        config.put(configKey, configValue);

        QuestDBSinkConnector connector = new QuestDBSinkConnector();
        assertEquals(Arrays.asList("Only one of '" + configKey + "' or 'client.conf.string' must be set."),
                fieldErrors(connector, config, configKey));
    }

    @Test
    public void testTimestampFieldsRejectEmptyAndDuplicateNamesBeforeStartup() {
        for (String fields : Arrays.asList("", " ", ",", "date,", ",time", "date,,time", "date, ,time", "date,date", "date, date")) {
            Map<String, String> props = baseConnectorProps();
            props.put("client.conf.string", "ws::addr=localhost:9000;");
            props.put("timestamp.field.name", fields);
            assertFalse(fieldErrors(new QuestDBSinkConnector(), props, "timestamp.field.name").isEmpty(), fields);
            assertThrows(ConfigException.class, () -> new QuestDBSinkConnectorConfig(props), fields);
        }
    }

    @Test
    public void testTimestampConflictsRejectDuringValidationAndStartup() {
        for (String format : Arrays.asList("json", "json_envelope", "connect")) {
            Map<String, String> props = baseConnectorProps();
            props.put("client.conf.string", "ws::addr=localhost:9000;");
            props.put("timestamp.field.name", "date,time");
            props.put("value.format", format);
            if (format.equals("connect")) {
                props.put("timestamp.kafka.native", "true");
            }
            assertFalse(fieldErrors(new QuestDBSinkConnector(), props, "timestamp.field.name").isEmpty());
            assertThrows(ConfigException.class, () -> new QuestDBSinkConnectorConfig(props));
        }
    }

    @Test
    public void testValidTimestampConfigurations() {
        for (String format : Arrays.asList("connect", "json", "json_envelope")) {
            Map<String, String> props = baseConnectorProps();
            props.put("client.conf.string", "ws::addr=localhost:9000;");
            props.put("value.format", format);
            props.put("timestamp.field.name", format.equals("connect") ? "date, time" : "timestamp");
            assertTrue(fieldErrors(new QuestDBSinkConnector(), props, "timestamp.field.name").isEmpty());
            QuestDBSinkConnectorConfig config = new QuestDBSinkConnectorConfig(props);
            RecordToRowHandler handler = new RecordToRowHandler(config, null, false, false);
            // a single field name stays on the scalar path; only "date, time" is composed
            assertEquals(format.equals("connect"), handler.hasComposedTimestamp(), format);
        }
    }

    @Test
    public void testDlqCategoriesValidatedBeforeStartup() {
        Map<String, String> props = baseConnectorProps();
        props.put("client.conf.string", "ws::addr=localhost:9000;");
        props.put("qwp.dlq.terminal.categories", "SCHEMA_MISSMATCH");
        assertTrue(fieldErrors(new QuestDBSinkConnector(), props, "qwp.dlq.terminal.categories").get(0).contains("unknown QWP terminal category"));
        assertThrows(ConfigException.class, () -> new QuestDBSinkConnectorConfig(props));
        props.put("qwp.dlq.terminal.categories", null);
        assertFalse(fieldErrors(new QuestDBSinkConnector(), props, "qwp.dlq.terminal.categories").isEmpty());
        assertThrows(ConfigException.class, () -> new QuestDBSinkConnectorConfig(props));
        for (String categories : Arrays.asList("schema_mismatch", "", " SCHEMA_MISMATCH ")) {
            props.put("qwp.dlq.terminal.categories", categories);
            assertTrue(fieldErrors(new QuestDBSinkConnector(), props, "qwp.dlq.terminal.categories").isEmpty());
            new QuestDBSinkConnectorConfig(props);
        }
    }

    private List<String> fieldErrors(QuestDBSinkConnector connector, Map<String, String> props, String name) {
        return connector.validate(props, NO_ENV).configValues().stream().filter(v -> v.name().equals(name))
                .findFirst().orElseThrow().errorMessages();
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
