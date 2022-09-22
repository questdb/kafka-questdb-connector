package org.questdb.kafka;

import io.questdb.client.Sender;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;

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
        //todo: add logical types support
        //todo: do something with keys
        //todo: add support for event time
        String explicitTable = config.getTable();
        for (SinkRecord record : collection) {
            String tableName = explicitTable == null ? record.topic() : explicitTable;
            sender.table(tableName);
            Struct value = (Struct) record.value();
            Schema valueSchema = record.valueSchema();
            List<Field> valueFields = valueSchema.fields();
            for (Field field : valueFields) {
                String fieldName = field.name();
                Schema.Type type = field.schema().type();
                switch (type) {
                    case INT8:
                    case INT16:
                    case INT32:
                    case INT64:
                        Number l = (Number) value.get(field);
                        if (l != null) {
                            sender.longColumn(fieldName, l.longValue());
                        }
                        break;
                    case FLOAT32:
                    case FLOAT64:
                        Number d = (Number) value.get(field);
                        if (d != null) {
                            sender.doubleColumn(fieldName, d.doubleValue());
                        }
                        break;
                    case BOOLEAN:
                        Boolean b = (Boolean) value.get(field);
                        if (b != null) {
                            sender.boolColumn(fieldName, b);
                        }
                        break;
                    case STRING:
                        String s = (String) value.get(field);
                        if (s != null) {
                            sender.stringColumn(fieldName, s);
                        }
                        break;
                    case BYTES:
                    case ARRAY:
                    case MAP:
                    case STRUCT:
                    default:
                        throw new UnsupportedOperationException("Unsupported type " + type);
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
