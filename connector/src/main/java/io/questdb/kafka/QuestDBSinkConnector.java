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

public final class QuestDBSinkConnector extends SinkConnector {
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
        Config result = super.validate(connectorConfigs);
        validateClientConfiguration(connectorConfigs, result);
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
        if (confString != null) {
            try {
                ClientConfUtils.validateConfString(confString.trim(), connectorConfigs);
            } catch (ConfigException e) {
                configValue(result, QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG).addErrorMessage(e.getMessage());
            }
        }
        return result;
    }

    private static ConfigValue configValue(Config config, String name) {
        return config.configValues().stream().filter(value -> value.name().equals(name)).findFirst().orElseThrow();
    }

    private static void validateClientConfiguration(Map<String, String> connectorConfigs, Config result) {
        String host = connectorConfigs.get(QuestDBSinkConnectorConfig.HOST_CONFIG);
        String confString = connectorConfigs.get(QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG);
        String envConfString = System.getenv("QDB_CLIENT_CONF");

        // cannot set client configuration string via both explicit config and environment variable
        if (confString != null && envConfString != null) {
            configValue(result, QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG).addErrorMessage("Only one of '" + QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG + "' or QDB_CLIENT_CONF environment variable must be set. They cannot be used together.");
        }

        if (confString == null && envConfString == null) {
            if (host == null) {
                configValue(result, QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG).addErrorMessage("Either '" + QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG + "' or '" + QuestDBSinkConnectorConfig.HOST_CONFIG + "' must be set.");
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
