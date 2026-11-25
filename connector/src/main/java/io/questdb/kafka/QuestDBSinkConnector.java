package io.questdb.kafka;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public final class QuestDBSinkConnector extends SinkConnector {
    // The deprecated 'host' option still works but is deliberately not advertised here: new
    // deployments should configure the connection through the client configuration string.
    static final String MISSING_CLIENT_CONFIGURATION_MESSAGE = "No QuestDB connection configured. Set '"
            + QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG
            + "' (for example: wss::addr=localhost:9000;) or the QDB_CLIENT_CONF environment variable.";

    private Map<String, String> configProps;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        configProps = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return QuestDBSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            configs.add(configProps);
        }
        return configs;
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return QuestDBSinkConnectorConfig.conf();
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        return validate(connectorConfigs, System::getenv);
    }

    // Package-private so tests can supply a worker environment instead of inheriting the one
    // the test JVM happens to run with.
    Config validate(Map<String, String> connectorConfigs, Function<String, String> env) {
        Config result = super.validate(connectorConfigs);
        validateClientConfiguration(connectorConfigs, result, env);
        ConfigValue timestampField = configValue(result, QuestDBSinkConnectorConfig.DESIGNATED_TIMESTAMP_COLUMN_NAME_CONFIG);
        try {
            QuestDBSinkConnectorConfig.validateTimestampOptions((String) timestampField.value(),
                    Boolean.TRUE.equals(configValue(result, QuestDBSinkConnectorConfig.DESIGNATED_TIMESTAMP_KAFKA_NATIVE_CONFIG).value()),
                    (String) configValue(result, QuestDBSinkConnectorConfig.VALUE_FORMAT_CONFIG).value());
        } catch (ConfigException e) {
            timestampField.addErrorMessage(e.getMessage());
        }
        // Environment-only configuration is resolved on the worker running the task.
        String confString = connectorConfigs.get(QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG);
        ConfigValue enrichmentSql = configValue(result, QuestDBSinkConnectorConfig.ENRICHMENT_SQL_CONFIG);
        try {
            QuestDBSinkConnectorConfig.validateEnrichment((String) enrichmentSql.value(),
                    (String) configValue(result, QuestDBSinkConnectorConfig.ENRICHMENT_KEY_FIELD_CONFIG).value(),
                    isKnownNonQwp(confString, connectorConfigs.get(QuestDBSinkConnectorConfig.HOST_CONFIG), env));
        } catch (ConfigException e) {
            enrichmentSql.addErrorMessage(e.getMessage());
        }
        if (confString != null) {
            try {
                ClientConfUtils.validateConfString(confString.trim(), connectorConfigs);
            } catch (ConfigException e) {
                configValue(result, QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG).addErrorMessage(e.getMessage());
            }
        }
        return result;
    }

    /**
     * Whether the task will use a legacy transport, as far as this worker can tell. Like the rest
     * of validate(), it takes this worker's environment to be the task's: with neither
     * client.conf.string nor QDB_CLIENT_CONF set, the task uses the deprecated host option. The
     * content of QDB_CLIENT_CONF is left to the task, as its validation is.
     */
    private static boolean isKnownNonQwp(String confString, String host, Function<String, String> env) {
        if (blankToNull(confString) == null) {
            // no connection at all is reported as such by validateClientConfiguration
            return blankToNull(host) != null && blankToNull(env.apply("QDB_CLIENT_CONF")) == null;
        }
        try {
            return !ClientConfUtils.isQwp(ConfStringEnvInterpolator.expand(confString.trim(), env));
        } catch (ConfigException e) {
            return false; // not expandable on this worker; reported above or left to the task
        }
    }

    private static String blankToNull(String value) {
        return value == null || value.trim().isEmpty() ? null : value;
    }

    private static ConfigValue configValue(Config config, String name) {
        return config.configValues().stream().filter(value -> value.name().equals(name)).findFirst().orElseThrow();
    }

    private static void validateClientConfiguration(Map<String, String> connectorConfigs, Config result, Function<String, String> env) {
        String host = connectorConfigs.get(QuestDBSinkConnectorConfig.HOST_CONFIG);
        // A blank value is what a task treats as unset (see ClientConfUtils.resolveConfString), so
        // validate it the same way instead of letting it slip through to a failing task start.
        String confString = blankToNull(connectorConfigs.get(QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG));
        String envConfString = blankToNull(env.apply("QDB_CLIENT_CONF"));

        // cannot set client configuration string via both explicit config and environment variable
        if (confString != null && envConfString != null) {
            configValue(result, QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG).addErrorMessage("Only one of '" + QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG + "' or QDB_CLIENT_CONF environment variable must be set. They cannot be used together.");
        }

        if (confString == null && envConfString == null) {
            if (host == null) {
                configValue(result, QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG).addErrorMessage(MISSING_CLIENT_CONFIGURATION_MESSAGE);
            }
            return; // configuration string is not used, nothing else to validate
        }

        // configuration string is used, let's validate no other client configuration is set
        for (String name : new String[]{QuestDBSinkConnectorConfig.HOST_CONFIG, QuestDBSinkConnectorConfig.TLS,
                QuestDBSinkConnectorConfig.TLS_VALIDATION_MODE_CONFIG, QuestDBSinkConnectorConfig.TOKEN, QuestDBSinkConnectorConfig.USERNAME}) {
            if (connectorConfigs.get(name) != null) {
                configValue(result, name).addErrorMessage("Only one of '" + name + "' or '"
                        + QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG + "' must be set.");
            }
        }
    }
}
