package io.questdb.kafka;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
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
        String s = connectorConfigs.get(QuestDBSinkConnectorConfig.DESIGNATED_TIMESTAMP_KAFKA_NATIVE_CONFIG);
        if (Boolean.parseBoolean(s) && connectorConfigs.get(QuestDBSinkConnectorConfig.DESIGNATED_TIMESTAMP_COLUMN_NAME_CONFIG) != null) {
            throw new IllegalArgumentException("Cannot use '" + QuestDBSinkConnectorConfig.DESIGNATED_TIMESTAMP_COLUMN_NAME_CONFIG
                    + "' with '" + QuestDBSinkConnectorConfig.DESIGNATED_TIMESTAMP_KAFKA_NATIVE_CONFIG +"'. These options are mutually exclusive.");
        }

        validateClientConfiguration(connectorConfigs);
        return super.validate(connectorConfigs);
    }

    private static void validateClientConfiguration(Map<String, String> connectorConfigs) {
        String host = connectorConfigs.get(QuestDBSinkConnectorConfig.HOST_CONFIG);
        String confString = connectorConfigs.get(QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG);
        String envConfString = System.getenv("QDB_CLIENT_CONF");

        // cannot set client configuration string via both explicit config and environment variable
        if (confString != null && envConfString != null) {
            throw new IllegalArgumentException("Only one of '" + QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG + "' or QDB_CLIENT_CONF environment variable must be set. They cannot be used together.");
        }

        if (confString == null && envConfString == null) {
            if (host == null) {
                throw new IllegalArgumentException("Either '" + QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG + "' or '" + QuestDBSinkConnectorConfig.HOST_CONFIG + "' must be set.");
            }
            return; // configuration string is not used, nothing else to validate
        }

        // configuration string is used, let's validate no other client configuration is set
        if (host != null) {
            throw new IllegalArgumentException("Only one of '" + QuestDBSinkConnectorConfig.HOST_CONFIG + "' or '" + QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG + "' must be set.");
        }
        if (connectorConfigs.get(QuestDBSinkConnectorConfig.TLS) != null) {
            throw new IllegalArgumentException("Only one of '" + QuestDBSinkConnectorConfig.TLS + "' or '" + QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG + "' must be set.");
        }
        if (connectorConfigs.get(QuestDBSinkConnectorConfig.TLS_VALIDATION_MODE_CONFIG) != null) {
            throw new IllegalArgumentException("Only one of '" + QuestDBSinkConnectorConfig.TLS_VALIDATION_MODE_CONFIG + "' or '" + QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG + "' must be set.");
        }
        if (connectorConfigs.get(QuestDBSinkConnectorConfig.TOKEN) != null) {
            throw new IllegalArgumentException("Only one of '" + QuestDBSinkConnectorConfig.TOKEN + "' or '" + QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG + "' must be set.");
        }
        if (connectorConfigs.get(QuestDBSinkConnectorConfig.USERNAME) != null) {
            throw new IllegalArgumentException("Only one of '" + QuestDBSinkConnectorConfig.USERNAME + "' or '" + QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG + "' must be set.");
        }
    }
}
