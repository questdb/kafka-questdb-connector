package io.questdb.kafka;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;

public final class QuestDBSinkConnectorConfig extends AbstractConfig {
    public static final String HOST_CONFIG = "host";
    private static final String HOST_DOC = "QuestDB server host and port";

    public static final String TABLE_CONFIG = "table";
    private static final String TABLE_DOC = "Table name in the target QuestDB database";

    public static final String KEY_PREFIX_CONFIG = "key.prefix";
    private static final String KEY_PREFIX_DOC = "Prefix for key fields";

    public static final String VALUE_PREFIX_CONFIG = "value.prefix";
    private static final String VALUE_PREFIX_DOC = "Prefix for value fields";

    public static final String SKIP_UNSUPPORTED_TYPES_CONFIG = "skip.unsupported.types";
    private static final String SKIP_UNSUPPORTED_TYPES_DOC = "Skip unsupported types";

    public static final String DESIGNATED_TIMESTAMP_COLUMN_NAME_CONFIG = "timestamp.field.name";
    private static final String DESIGNATED_TIMESTAMP_COLUMN_NAME_DOC = "Designated timestamp field name";

    public static final String INCLUDE_KEY_CONFIG = "include.key";
    private static final String INCLUDE_KEY_DOC = "Include key in the table";

    public QuestDBSinkConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public QuestDBSinkConnectorConfig(Map<String, String> parsedConfig) {
        this(conf(), parsedConfig);
    }

    public static ConfigDef conf() {
        return new ConfigDef()
                .define(HOST_CONFIG, Type.STRING, Importance.HIGH, HOST_DOC)
                .define(TABLE_CONFIG, Type.STRING, null, Importance.HIGH, TABLE_DOC)
                .define(KEY_PREFIX_CONFIG, Type.STRING, "key", Importance.MEDIUM, KEY_PREFIX_DOC)
                .define(VALUE_PREFIX_CONFIG, Type.STRING, "", Importance.MEDIUM, VALUE_PREFIX_DOC)
                .define(SKIP_UNSUPPORTED_TYPES_CONFIG, Type.BOOLEAN, false, Importance.MEDIUM, SKIP_UNSUPPORTED_TYPES_DOC)
                .define(DESIGNATED_TIMESTAMP_COLUMN_NAME_CONFIG, Type.STRING, null, Importance.MEDIUM, DESIGNATED_TIMESTAMP_COLUMN_NAME_DOC)
                .define(INCLUDE_KEY_CONFIG, Type.BOOLEAN, true, Importance.MEDIUM, INCLUDE_KEY_DOC);
    }

    public String getHost() {
        return getString(HOST_CONFIG);
    }

    public String getTable() {
        return getString(TABLE_CONFIG);
    }

    public String getKeyPrefix() {
        return getString(KEY_PREFIX_CONFIG);
    }

    public String getValuePrefix() {
        return getString(VALUE_PREFIX_CONFIG);
    }

    public boolean isSkipUnsupportedTypes() {
        return getBoolean(SKIP_UNSUPPORTED_TYPES_CONFIG);
    }

    public String getDesignatedTimestampColumnName() {
        return getString(DESIGNATED_TIMESTAMP_COLUMN_NAME_CONFIG);
    }

    public boolean isIncludeKey() {
        return getBoolean(INCLUDE_KEY_CONFIG);
    }
}
