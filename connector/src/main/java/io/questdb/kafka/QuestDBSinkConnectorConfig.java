package io.questdb.kafka;

import io.questdb.cairo.TableUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;

import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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

    public static final String TIMESTAMP_STRING_FIELDS = "timestamp.string.fields";
    private static final String TIMESTAMP_STRING_FIELDS_DOC = "Comma separated list of string fields that should be parsed as timestamp.";

    public static final String TIMESTAMP_UNITS_CONFIG = "timestamp.units";
    private static final String TIMESTAMP_UNITS_DOC = "Units of designated timestamp field. Possible values: auto, millis, micros, nanos";

    public static final String INCLUDE_KEY_CONFIG = "include.key";
    private static final String INCLUDE_KEY_DOC = "Include key in the table";

    public static final String SYMBOL_COLUMNS_CONFIG = "symbols";
    private static final String SYMBOL_COLUMNS_DOC = "Comma separated list of columns that should be symbol type";

    public static final String DOUBLE_COLUMNS_CONFIG = "doubles";
    private static final String DOUBLE_COLUMNS_DOC = "Comma separated list of columns that should be double type";

    public static final String USERNAME = "username";
    private static final String USERNAME_DOC = "Username for QuestDB ILP authentication";

    public static final String TOKEN = "token";
    private static final String TOKEN_DOC = "Token for QuestDB ILP authentication";

    public static final String TLS = "tls";
    public static final String TLS_DOC = "Use TLS for connecting to QuestDB";

    public static final String RETRY_BACKOFF_MS = "retry.backoff.ms";
    private static final String RETRY_BACKOFF_MS_DOC = "The time in milliseconds to wait following an error before a retry attempt is made";

    public static final String MAX_RETRIES = "max.retries";
    private static final String MAX_RETRIES_DOC = "The maximum number of times to retry on errors before failing the task";

    public static final String TIMESTAMP_FORMAT = "timestamp.string.format";
    private static final String TIMESTAMP_FORMAT_DOC = "Default timestamp format. Used when parsing timestamp string fields";

    private static final String DEFAULT_TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'";

    public QuestDBSinkConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public QuestDBSinkConnectorConfig(Map<String, String> parsedConfig) {
        this(conf(), parsedConfig);
    }

    public static ConfigDef conf() {
        return new ConfigDef()
                .define(HOST_CONFIG, Type.STRING, Importance.HIGH, HOST_DOC)
                .define(TABLE_CONFIG, Type.STRING, null, TablenameValidator.INSTANCE, Importance.HIGH, TABLE_DOC)
                .define(KEY_PREFIX_CONFIG, Type.STRING, "key", Importance.MEDIUM, KEY_PREFIX_DOC)
                .define(VALUE_PREFIX_CONFIG, Type.STRING, "", Importance.MEDIUM, VALUE_PREFIX_DOC)
                .define(SKIP_UNSUPPORTED_TYPES_CONFIG, Type.BOOLEAN, false, Importance.MEDIUM, SKIP_UNSUPPORTED_TYPES_DOC)
                .define(DESIGNATED_TIMESTAMP_COLUMN_NAME_CONFIG, Type.STRING, null, Importance.MEDIUM, DESIGNATED_TIMESTAMP_COLUMN_NAME_DOC)
                .define(INCLUDE_KEY_CONFIG, Type.BOOLEAN, true, Importance.MEDIUM, INCLUDE_KEY_DOC)
                .define(SYMBOL_COLUMNS_CONFIG, Type.STRING, null, Importance.MEDIUM, SYMBOL_COLUMNS_DOC)
                .define(DOUBLE_COLUMNS_CONFIG, Type.STRING, null, Importance.MEDIUM, DOUBLE_COLUMNS_DOC)
                .define(USERNAME, Type.STRING, "admin", Importance.MEDIUM, USERNAME_DOC)
                .define(TOKEN, Type.PASSWORD, null, Importance.MEDIUM, TOKEN_DOC)
                .define(TLS, Type.BOOLEAN, false, Importance.MEDIUM, TLS_DOC)
                .define(TIMESTAMP_UNITS_CONFIG, Type.STRING, "auto", ConfigDef.ValidString.in("auto", "millis", "micros", "nanos"), Importance.LOW, TIMESTAMP_UNITS_DOC, null, -1, ConfigDef.Width.NONE, TIMESTAMP_UNITS_CONFIG, Collections.emptyList(), TimestampUnitsRecommender.INSTANCE)
                .define(RETRY_BACKOFF_MS, Type.LONG, 3_000, Importance.LOW, RETRY_BACKOFF_MS_DOC)
                .define(MAX_RETRIES, Type.INT, 10, Importance.LOW, MAX_RETRIES_DOC)
                .define(TIMESTAMP_FORMAT, Type.STRING, DEFAULT_TIMESTAMP_FORMAT, TimestampFormatValidator.INSTANCE, Importance.MEDIUM, TIMESTAMP_FORMAT_DOC)
                .define(TIMESTAMP_STRING_FIELDS, Type.STRING, null, Importance.MEDIUM, TIMESTAMP_STRING_FIELDS_DOC);
    }

    public String getHost() {
        return getString(HOST_CONFIG);
    }

    public String getTable() {
        return getString(TABLE_CONFIG);
    }

    public String getDefaultTimestampFormat() {
        return getString(TIMESTAMP_FORMAT);
    }

    public String getTimestampStringFields() {
        return getString(TIMESTAMP_STRING_FIELDS);
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

    public String getSymbolColumns() {
        return getString(SYMBOL_COLUMNS_CONFIG);
    }

    public String getDoubleColumns() {
        return getString(DOUBLE_COLUMNS_CONFIG);
    }

    public String getUsername() {
        return getString(USERNAME);
    }

    public String getToken() {
        return getString(TOKEN);
    }

    public boolean isTls() {
        return getBoolean(TLS);
    }

    public TimeUnit getTimestampUnitsOrNull() {
        String configured = getString(TIMESTAMP_UNITS_CONFIG);
        switch (configured) {
            case "millis":
                return TimeUnit.MILLISECONDS;
            case "micros":
                return TimeUnit.MICROSECONDS;
            case "nanos":
                return TimeUnit.NANOSECONDS;
            case "auto":
                return null;
            default:
                throw new ConnectException("Unknown timestamp units mode: " + configured + ". Possible values: auto, millis, micros, nanos");
        }
    }

    public long getRetryBackoffMs() {
        return getLong(RETRY_BACKOFF_MS);
    }

    public int getMaxRetries() {
        return getInt(MAX_RETRIES);
    }

    private static class TimestampUnitsRecommender implements ConfigDef.Recommender {
        private static final TimestampUnitsRecommender INSTANCE = new TimestampUnitsRecommender();
        private static final List<Object> VALID_UNITS = Arrays.asList("auto", "millis", "micros", "nanos");

        @Override
        public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
            return VALID_UNITS;
        }

        @Override
        public boolean visible(String name, Map<String, Object> parsedConfig) {
            return true;
        }
    }

    private static class TimestampFormatValidator implements ConfigDef.Validator {
        private static final TimestampFormatValidator INSTANCE = new TimestampFormatValidator();
        @Override
        public void ensureValid(String name, Object value) {
            if (!(value instanceof String)) {
                throw new ConfigException(name, value, "Timestamp format must be a string");
            }
        }
    }

    private static class TablenameValidator implements ConfigDef.Validator {
        private static final TablenameValidator INSTANCE = new TablenameValidator();
        @Override
        public void ensureValid(String name, Object value) {
            if (value == null) {
                // null is allowed, it means we will use the topic name as the table name
                return;
            }
            String tableName = (String) value;
            if (!TableUtils.isValidTableName(tableName, Integer.MAX_VALUE)) {
                throw new ConfigException(name, value, "table name contain an illegal char: '\\n', '\\r', '?', ',', ''', " +
                        "'\"', '\\', '/', ':', ')', '(', '+', '*' '%%', '~', or a non-printable char");
            }
        }
    }
}
