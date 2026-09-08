package io.questdb.kafka;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Map;

/**
 * Stable task entry point. Transport selection happens here, on the worker
 * that actually runs the task, so worker-local QDB_CLIENT_CONF is honored.
 */
public final class QuestDBSinkTask extends SinkTask {
    private static final String QWP_TASK_CLASS = "io.questdb.kafka.QwpSinkTask";

    private SinkTask delegate;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        QuestDBSinkConnectorConfig config = new QuestDBSinkConnectorConfig(props);
        String confStr = ClientConfUtils.resolveConfString(config);
        delegate = confStr != null && ClientConfUtils.isQwp(confStr)
                ? newQwpDelegate()
                : new LegacyQuestDBSinkTask();
        delegate.initialize(context);
        delegate.start(props);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        delegate.put(records);
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        delegate.flush(currentOffsets);
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        return delegate.preCommit(currentOffsets);
    }

    @Override
    public void open(Collection<TopicPartition> partitions) {
        delegate.open(partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        delegate.onPartitionsAssigned(partitions);
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        delegate.close(partitions);
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        delegate.onPartitionsRevoked(partitions);
    }

    @Override
    public void stop() {
        if (delegate != null) {
            delegate.stop();
        }
    }

    static void requireOriginalCoordinatesApi() {
        requireOriginalCoordinatesApi(SinkRecord.class);
    }

    static void requireOriginalCoordinatesApi(Class<?> sinkRecordClass) {
        try {
            sinkRecordClass.getMethod("originalTopic");
            sinkRecordClass.getMethod("originalKafkaPartition");
            sinkRecordClass.getMethod("originalKafkaOffset");
        } catch (NoSuchMethodException e) {
            throw new ConnectException("QWP transport requires Kafka Connect 3.6 or newer", e);
        }
    }

    private static SinkTask newQwpDelegate() {
        requireOriginalCoordinatesApi();
        try {
            return (SinkTask) Class.forName(QWP_TASK_CLASS).getDeclaredConstructor().newInstance();
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException e) {
            throw new ConnectException("Cannot initialize the QWP task", e);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new ConnectException("Cannot initialize the QWP task", cause);
        }
    }
}
