package org.questdb.kafka;

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
    private static final Logger log = LoggerFactory.getLogger(QuestDBSinkTask.class);
    private Sender sender;
    private QuestDBSinkConnectorConfig config;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        this.config = new QuestDBSinkConnectorConfig(map);
        this.sender = Sender.builder().address(config.getHost()).build();
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        //todo: add support for event time
        for (SinkRecord record : collection) {
            handleSingleRecord(record);
        }
        sender.flush();
    }

    private void handleSingleRecord(SinkRecord record) {
        String explicitTable = config.getTable();
        String tableName = explicitTable == null ? record.topic() : explicitTable;
        sender.table(tableName);

        handleObject("key", record.keySchema(), record.key(), "key");
        handleObject("", record.valueSchema(), record.value(), "value");

        sender.atNow();
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

    private void handleObject(String name, Schema schema, Object value, String fallbackName) {
        assert !name.isEmpty() || !fallbackName.isEmpty();
        if (tryWriteLogicalType(name.isEmpty() ? fallbackName : name, schema, value)) {
            return;
        }
        // ok, not a known logical try, try primitive types
        writePhysicalType(name, schema, value, fallbackName);
    }

    private void writePhysicalType(String name, Schema schema, Object value, String fallbackName) {
        Schema.Type type = schema.type();
        String primitiveTypesName = name.isEmpty() ? fallbackName : name;
        switch (type) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
                if (value != null) {
                    Number l = (Number) value;
                    sender.longColumn(primitiveTypesName, l.longValue());
                }
                break;
            case FLOAT32:
            case FLOAT64:
                if (value != null) {
                    Number d = (Number) value;
                    sender.doubleColumn(primitiveTypesName, d.doubleValue());
                }
                break;
            case BOOLEAN:
                if (value != null) {
                    Boolean b = (Boolean) value;
                    sender.boolColumn(primitiveTypesName, b);
                }
                break;
            case STRING:
                if (value != null) {
                    String s = (String) value;
                    sender.stringColumn(primitiveTypesName, s);
                }
                break;
            case STRUCT:
                handleStruct(name, (Struct) value, schema);
                break;
            case BYTES:
            case ARRAY:
            case MAP:
            default:
                throw new ConnectException("Unsupported type " + type);
        }
    }

    private boolean tryWriteLogicalType(String name, Schema schema, Object value) {
        if (schema.name() != null) {
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
                    throw new ConnectException("Unsupported logical type " + schema.name());
            }
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
