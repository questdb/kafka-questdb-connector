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
        //todo: do something with keys
        //todo: add support for event time
        String explicitTable = config.getTable();
        for (SinkRecord record : collection) {
            String tableName = explicitTable == null ? record.topic() : explicitTable;
            sender.table(tableName);
            Struct recordValue = (Struct) record.value();
            Schema valueSchema = record.valueSchema();
            List<Field> valueFields = valueSchema.fields();
            for (Field field : valueFields) {
                String fieldName = field.name();
                Schema fieldSchema = field.schema();
                Object fieldValue = recordValue.get(fieldName);

                // try logical types first
                if (fieldSchema.name() != null) {
                    switch (fieldSchema.name()) {
                        case Timestamp.LOGICAL_NAME:
                        case Date.LOGICAL_NAME:
                            long epochMillis = ((java.util.Date) fieldValue).getTime();
                            sender.timestampColumn(fieldName, TimeUnit.MILLISECONDS.toMicros(epochMillis));
                            continue;
                        case Time.LOGICAL_NAME:
                            long dayMillis = ((java.util.Date) fieldValue).getTime();
                            sender.longColumn(fieldName, dayMillis);
                            continue;
                        case Decimal.LOGICAL_NAME:
                            throw new ConnectException("Unsupported logical type " + fieldSchema.name());
                    }
                }

                // ok, not a known logical try, try primitive types
                Schema.Type type = fieldSchema.type();
                switch (type) {
                    case INT8:
                    case INT16:
                    case INT32:
                    case INT64:
                        if (fieldValue != null) {
                            Number l = (Number) fieldValue;
                            sender.longColumn(fieldName, l.longValue());
                        }
                        break;
                    case FLOAT32:
                    case FLOAT64:
                        if (fieldValue != null) {
                            Number d = (Number) recordValue.get(field);
                            sender.doubleColumn(fieldName, d.doubleValue());
                        }
                        break;
                    case BOOLEAN:
                        if (fieldValue != null) {
                            Boolean b = (Boolean) recordValue.get(field);
                            sender.boolColumn(fieldName, b);
                        }
                        break;
                    case STRING:
                        if (fieldValue != null) {
                            String s = (String) recordValue.get(field);
                            sender.stringColumn(fieldName, s);
                        }
                        break;
                    case BYTES:
                    case ARRAY:
                    case MAP:
                    case STRUCT:
                    default:
                        throw new ConnectException("Unsupported type " + type);
                }
            }
            sender.atNow();
        }
        sender.flush();
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
