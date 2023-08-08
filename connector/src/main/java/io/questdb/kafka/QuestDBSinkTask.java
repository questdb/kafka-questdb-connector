package io.questdb.kafka;

import io.questdb.client.Sender;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

public final class QuestDBSinkTask extends SinkTask {
    private static final char STRUCT_FIELD_SEPARATOR = '_';
    private static final String PRIMITIVE_KEY_FALLBACK_NAME = "key";
    private static final String PRIMITIVE_VALUE_FALLBACK_NAME = "value";

    private static final Logger log = LoggerFactory.getLogger(QuestDBSinkTask.class);
    private Sender sender;
    private QuestDBSinkConnectorConfig config;
    private String timestampColumnName;
    private long timestampColumnValue = Long.MIN_VALUE;
    private TimeUnit timestampUnits;
    private Set<CharSequence> doubleColumns;
    private Set<String> stringTimestampColumns;
    private int remainingRetries;
    private long batchesSinceLastError = 0;
    private DateFormat dataFormat;
    private boolean kafkaTimestampsEnabled;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        this.config = new QuestDBSinkConnectorConfig(map);
        String timestampStringFields = config.getTimestampStringFields();
        if (timestampStringFields != null) {
            stringTimestampColumns = new HashSet<>();
            for (String symbolColumn : timestampStringFields.split(",")) {
                stringTimestampColumns.add(symbolColumn.trim());
            }
            dataFormat = TimestampParserCompiler.compilePattern(config.getTimestampFormat());
        } else {
            stringTimestampColumns = Collections.emptySet();
        }

        String doubleColumnsConfig = config.getDoubleColumns();
        if (doubleColumnsConfig == null) {
            doubleColumns = Collections.emptySet();
        } else {
            doubleColumns = new HashSet<>();
            for (String symbolColumn : doubleColumnsConfig.split(",")) {
                doubleColumns.add(symbolColumn.trim());
            }
        }
        this.sender = createSender();
        this.remainingRetries = config.getMaxRetries();
        this.timestampColumnName = config.getDesignatedTimestampColumnName();
        this.kafkaTimestampsEnabled = config.isDesignatedTimestampKafkaNative();
        this.timestampUnits = config.getTimestampUnitsOrNull();
    }

    private Sender createSender() {
        log.debug("Creating a new sender");
        Sender.LineSenderBuilder builder = Sender.builder().address(config.getHost());
        if (config.isTls()) {
            builder.enableTls();
        }
        if (config.getToken() != null) {
            String username = config.getUsername();
            if (username == null || username.isEmpty()) {
                throw new ConnectException("Username cannot be empty when using ILP authentication");
            }
            builder.enableAuth(username).authToken(config.getToken().value());
        }
        Sender rawSender = builder.build();
        String symbolColumns = config.getSymbolColumns();
        if (symbolColumns == null) {
            log.debug("No symbol columns configured. Using raw sender");
            return rawSender;
        }
        log.debug("Symbol columns configured. Using buffering sender");
        return new BufferingSender(rawSender, symbolColumns);
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        if (collection.isEmpty()) {
            log.debug("Received empty collection, ignoring");
            return;
        }
        if (log.isDebugEnabled()) {
            SinkRecord record = collection.iterator().next();
            log.debug("Received {} records. First record kafka coordinates:({}-{}-{}). ",
                    collection.size(), record.topic(), record.kafkaPartition(), record.kafkaOffset());
        }
        try {
            if (sender == null) {
                sender = createSender();
            }
            for (SinkRecord record : collection) {
                handleSingleRecord(record);
            }
            sender.flush();
            log.debug("Successfully sent {} records", collection.size());
            if (++batchesSinceLastError == 10) {
                // why 10? why not to reset the retry counter immediately upon a successful flush()?
                // there are two reasons for server disconnections:
                // 1. the server is down / unreachable / other_infrastructure_issues
                // 2. the client is sending bad data (e.g. pushing a string to a double column)
                // errors in the latter case are not recoverable. upon receiving bad data the server will *eventually* close the connection,
                // after a while, the client will notice that the connection is closed and will try to reconnect
                // if we reset the retry counter immediately upon first successful flush() then we end-up in a loop where we flush bad data,
                // the server closes the connection, the client reconnects, reset the retry counter, and sends bad data again, etc.
                // to avoid this, we only reset the retry counter after a few successful flushes.
                log.debug("Successfully sent 10 batches in a row. Resetting retry counter");
                remainingRetries = config.getMaxRetries();
            }
        } catch (LineSenderException e) {
            onSenderException(e);
        }
    }

    private void onSenderException(LineSenderException e) {
        batchesSinceLastError = 0;
        if (--remainingRetries > 0) {
            closeSenderSilently();
            sender = null;
            log.debug("Sender exception, retrying in {} ms", config.getRetryBackoffMs());
            context.timeout(config.getRetryBackoffMs());
            throw new RetriableException(e);
        } else {
            throw new ConnectException("Failed to send data to QuestDB after " + config.getMaxRetries() + " retries");
        }
    }

    private void closeSenderSilently() {
        try {
            if (sender != null) {
                sender.close();
            }
        } catch (Exception ex) {
            log.warn("Failed to close sender", ex);
        }
    }

    private void handleSingleRecord(SinkRecord record) {
        assert timestampColumnValue == Long.MIN_VALUE;
        String explicitTable = config.getTable();
        String tableName = explicitTable == null ? record.topic() : explicitTable;
        sender.table(tableName);

        if (config.isIncludeKey()) {
            handleObject(config.getKeyPrefix(), record.keySchema(), record.key(), PRIMITIVE_KEY_FALLBACK_NAME);
        }
        handleObject(config.getValuePrefix(), record.valueSchema(), record.value(), PRIMITIVE_VALUE_FALLBACK_NAME);

        if (kafkaTimestampsEnabled) {
            timestampColumnValue = TimeUnit.MILLISECONDS.toNanos(record.timestamp());
        }
        if (timestampColumnValue == Long.MIN_VALUE) {
            sender.atNow();
        } else {
            try {
                sender.at(timestampColumnValue);
            } finally {
                timestampColumnValue = Long.MIN_VALUE;
            }
        }
    }

    private void handleStruct(String parentName, Struct value, Schema schema) {
        List<Field> valueFields = schema.fields();
        for (Field field : valueFields) {
            String fieldName = field.name();
            Schema fieldSchema = field.schema();
            Object fieldValue = value.get(fieldName);

            String name = parentName.isEmpty() ? fieldName : parentName + STRUCT_FIELD_SEPARATOR + fieldName;
            handleObject(name, fieldSchema, fieldValue, "");
        }
    }

    private void handleMap(String name, Map<?, ?> value, String fallbackName) {
        for (Map.Entry<?, ?> entry : value.entrySet()) {
            Object mapKey = entry.getKey();
            if (!(mapKey instanceof String)) {
                throw new ConnectException("Map keys must be strings");
            }
            String mapKeyName = (String) mapKey;
            String entryName = name.isEmpty() ? mapKeyName : name + STRUCT_FIELD_SEPARATOR + mapKeyName;
            handleObject(entryName, null, entry.getValue(), fallbackName);
        }
    }

    private boolean isDesignatedColumnName(String name, String fallbackName) {
        if (timestampColumnName == null) {
            return false;
        }
        if (timestampColumnName.equals(name)) {
            return true;
        }
        if (name != null) {
            return false;
        }
        return timestampColumnName.equals(fallbackName);
    }

    private void handleObject(String name, Schema schema, Object value, String fallbackName) {
        assert !name.isEmpty() || !fallbackName.isEmpty();

        if (isDesignatedColumnName(name, fallbackName)) {
            assert timestampColumnValue == Long.MIN_VALUE;
            if (value == null) {
                throw new ConnectException("Timestamp column value cannot be null");
            }
            timestampColumnValue = resolveDesignatedTimestampColumnValue(value, schema);
            return;
        }
        if (value == null) {
            return;
        }
        if (tryWriteLogicalType(name.isEmpty() ? fallbackName : name, schema, value)) {
            return;
        }
        // ok, not a known logical type, try primitive types
        if (tryWritePhysicalTypeFromSchema(name, schema, value, fallbackName)) {
            return;
        }
        writePhysicalTypeWithoutSchema(name, value, fallbackName);
    }

    private long resolveDesignatedTimestampColumnValue(Object value, Schema schema) {
        if (value instanceof java.util.Date) {
            log.debug("Timestamp column value is a java.util.Date");
            return TimeUnit.MILLISECONDS.toNanos(((java.util.Date) value).getTime());
        }
        if (value instanceof String) {
            log.debug("Timestamp column value is a string");
            return parseToMicros((String) value) * 1000;
        }
        if (!(value instanceof Long)) {
            throw new ConnectException("Unsupported timestamp column type: " + value.getClass());
        }
        long longValue = (Long) value;
        TimeUnit inputUnit;
        if (schema == null || !"io.debezium.time.MicroTimestamp".equals(schema.name())) {
            inputUnit = TimestampHelper.getTimestampUnits(timestampUnits, longValue);
            log.debug("Detected {} as timestamp units", inputUnit);
        } else {
            // special case: Debezium micros since epoch
            inputUnit = TimeUnit.MICROSECONDS;
            log.debug("Detected Debezium micros as timestamp units");
        }
        return inputUnit.toNanos(longValue);
    }

    private void writePhysicalTypeWithoutSchema(String name, Object value, String fallbackName) {
        if (value == null) {
            return;
        }
        String actualName = name.isEmpty() ? fallbackName : sanitizeName(name);
        if (value instanceof String) {
            String stringVal = (String) value;
            if (stringTimestampColumns.contains(actualName)) {
                long timestamp = parseToMicros(stringVal);
                sender.timestampColumn(actualName, timestamp);
            } else {
                sender.stringColumn(actualName, stringVal);
            }
        } else if (value instanceof Long) {
            Long longValue = (Long) value;
            if (doubleColumns.contains(actualName)) {
                sender.doubleColumn(actualName, longValue.doubleValue());
            } else {
                sender.longColumn(actualName, longValue);
            }
        } else if (value instanceof Integer) {
            Integer intValue = (Integer) value;
            if (doubleColumns.contains(actualName)) {
                sender.doubleColumn(actualName, intValue.doubleValue());
            } else {
                sender.longColumn(actualName, intValue);
            }
        } else if (value instanceof Boolean) {
            sender.boolColumn(actualName, (Boolean) value);
        } else if (value instanceof Double) {
            sender.doubleColumn(actualName, (Double) value);
        } else if (value instanceof Map) {
            handleMap(name, (Map<?, ?>) value, fallbackName);
        } else if (value instanceof java.util.Date) {
            long epochMillis = ((java.util.Date) value).getTime();
            sender.timestampColumn(actualName, TimeUnit.MILLISECONDS.toMicros(epochMillis));
        } else {
            onUnsupportedType(actualName, value.getClass().getName());
        }
    }

    private long parseToMicros(String timestamp) {
        try {
            return dataFormat.parse(timestamp, DateFormatUtils.enLocale);
        } catch (NumericException e) {
            throw new ConnectException("Cannot parse timestamp: " + timestamp + " with the configured format '" + config.getTimestampFormat() +"' use '"
                    + QuestDBSinkConnectorConfig.TIMESTAMP_FORMAT + "' to configure the right timestamp format. " +
                    "See https://questdb.io/docs/reference/function/date-time/#date-and-timestamp-format for timestamp parser documentation. ", e);
        }
    }

    private static String sanitizeName(String name) {
        // todo: proper implementation
        return name.replace('.', '_');
    }

    private boolean tryWritePhysicalTypeFromSchema(String name, Schema schema, Object value, String fallbackName) {
        if (schema == null) {
            return false;
        }
        Schema.Type type = schema.type();
        String primitiveTypesName = name.isEmpty() ? fallbackName : name;
        String sanitizedName = sanitizeName(primitiveTypesName);
        switch (type) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
                Number l = (Number) value;
                sender.longColumn(sanitizedName, l.longValue());
                break;
            case FLOAT32:
            case FLOAT64:
                Number d = (Number) value;
                sender.doubleColumn(sanitizedName, d.doubleValue());
                break;
            case BOOLEAN:
                Boolean b = (Boolean) value;
                sender.boolColumn(sanitizedName, b);
                break;
            case STRING:
                String s = (String) value;
                if (stringTimestampColumns.contains(primitiveTypesName)) {
                    long timestamp = parseToMicros(s);
                    sender.timestampColumn(sanitizedName, timestamp);
                } else {
                    sender.stringColumn(sanitizedName, s);
                }
                break;
            case STRUCT:
                handleStruct(name, (Struct) value, schema);
                break;
            case BYTES:
            case ARRAY:
            case MAP:
            default:
                onUnsupportedType(name, type);
        }
        return true;
    }

    private void onUnsupportedType(String name, Object type) {
        if (config.isSkipUnsupportedTypes()) {
            log.debug("Skipping unsupported type: {}, name: {}", type, name);
        } else {
            throw new ConnectException("Unsupported type: " + type + ", name: " + name);
        }
    }

    private boolean tryWriteLogicalType(String name, Schema schema, Object value) {
        if (schema == null || schema.name() == null) {
            return false;
        }
        switch (schema.name()) {
            case "io.debezium.time.MicroTimestamp":
                long l = (Long) value;
                sender.timestampColumn(name, l);
                return true;
            case "io.debezium.time.Date":
                int i = (Integer) value;
                long micros = Timestamps.addDays(0, i);
                sender.timestampColumn(name, micros);
                return true;
            case Timestamp.LOGICAL_NAME:
            case Date.LOGICAL_NAME:
                java.util.Date d = (java.util.Date) value;
                long epochMillis = d.getTime();
                sender.timestampColumn(name, TimeUnit.MILLISECONDS.toMicros(epochMillis));
                return true;
            case Time.LOGICAL_NAME:
                d = (java.util.Date) value;
                long dayMillis = d.getTime();
                sender.longColumn(name, dayMillis);
                return true;
            case Decimal.LOGICAL_NAME:
                onUnsupportedType(name, schema.name());
        }
        return false;
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
        // not needed as put() flushes after each record
    }

    @Override
    public void stop() {
        closeSenderSilently();
    }
}
