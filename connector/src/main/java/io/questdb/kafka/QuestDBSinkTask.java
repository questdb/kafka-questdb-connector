package io.questdb.kafka;

import io.questdb.client.Sender;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class QuestDBSinkTask extends SinkTask {
    private static final char STRUCT_FIELD_SEPARATOR = '_';
    private static final String PRIMITIVE_KEY_FALLBACK_NAME = "key";
    private static final String PRIMITIVE_VALUE_FALLBACK_NAME = "value";

    private static final Logger log = LoggerFactory.getLogger(QuestDBSinkTask.class);
    private Sender sender;
    private QuestDBSinkConnectorConfig config;
    private String timestampColumnName;
    private Object timestampColumnValue;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        this.config = new QuestDBSinkConnectorConfig(map);
        this.sender = Sender.builder().address(config.getHost()).build();
        this.timestampColumnName = config.getDesignatedTimestampColumnName();
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        for (SinkRecord record : collection) {
            handleSingleRecord(record);
        }
        sender.flush();
    }

    private void handleSingleRecord(SinkRecord record) {
        assert timestampColumnValue == null;
        String explicitTable = config.getTable();
        String tableName = explicitTable == null ? record.topic() : explicitTable;
        sender.table(tableName);

        //todo: detect duplicated columns
        if (config.isIncludeKey()) {
            handleObject(config.getKeyPrefix(), record.keySchema(), record.key(), PRIMITIVE_KEY_FALLBACK_NAME);
        }
        handleObject(config.getValuePrefix(), record.valueSchema(), record.value(), PRIMITIVE_VALUE_FALLBACK_NAME);

        if (timestampColumnValue == null) {
            sender.atNow();
        } else {
            if (timestampColumnValue instanceof Long) {
                sender.at(TimeUnit.MILLISECONDS.toNanos((Long) timestampColumnValue));
            } else if (timestampColumnValue instanceof java.util.Date) {
                sender.at(TimeUnit.MILLISECONDS.toNanos(((java.util.Date) timestampColumnValue).getTime()));
            } else {
                throw new ConnectException("Unsupported timestamp column type: " + timestampColumnValue.getClass());
            }
            timestampColumnValue = null;
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
            assert timestampColumnValue == null;
            if (value == null) {
                throw new ConnectException("Timestamp column value cannot be null");
            }
            timestampColumnValue = value;
            return;
        }
        if (tryWriteLogicalType(name.isEmpty() ? fallbackName : name, schema, value)) {
            return;
        }
        // ok, not a known logical try, try primitive types
        if (tryWritePhysicalTypeFromSchema(name, schema, value, fallbackName)) {
            return;
        }
        writePhysicalTypeWithoutSchema(name, value, fallbackName);
    }

    private void writePhysicalTypeWithoutSchema(String name, Object value, String fallbackName) {
        if (value == null) {
            return;
        }
        String actualName = name.isEmpty() ? fallbackName : sanitizeName(name);
        if (value instanceof String) {
            sender.stringColumn(actualName, (String) value);
        } else if (value instanceof Long) {
            sender.longColumn(actualName, (Long) value);
        } else if (value instanceof Integer) {
            sender.longColumn(actualName, (Integer) value);
        } else if (value instanceof Boolean) {
            sender.boolColumn(actualName, (Boolean) value);
        } else if (value instanceof Double) {
            sender.doubleColumn(actualName, (Double) value);
        } else if (value instanceof Map) {
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                Object mapKey = entry.getKey();
                if (!(mapKey instanceof String)) {
                    throw new ConnectException("Map keys must be strings");
                }
                String mapKeyName = (String) mapKey;
                String entryName = name.isEmpty() ? mapKeyName : name + STRUCT_FIELD_SEPARATOR + mapKeyName;
                handleObject(entryName, null, entry.getValue(), fallbackName);
            }
        } else {
            onUnsupportedType(actualName, value.getClass().getName());
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
                if (value != null) {
                    Number l = (Number) value;
                    sender.longColumn(sanitizedName, l.longValue());
                }
                break;
            case FLOAT32:
            case FLOAT64:
                if (value != null) {
                    Number d = (Number) value;
                    sender.doubleColumn(sanitizedName, d.doubleValue());
                }
                break;
            case BOOLEAN:
                if (value != null) {
                    Boolean b = (Boolean) value;
                    sender.boolColumn(sanitizedName, b);
                }
                break;
            case STRING:
                if (value != null) {
                    String s = (String) value;
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
        sender.flush();
    }

    @Override
    public void stop() {
        sender.close();
    }
}
