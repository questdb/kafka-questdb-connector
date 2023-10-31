package io.questdb.kafka;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTaskContext;

import java.util.Collection;
import java.util.Map;

public final class EmptyOffsetTracker implements OffsetTracker {
    @Override
    public void onPartitionsOpened(Collection<TopicPartition> partitions) {

    }

    @Override
    public void onPartitionsClosed(Collection<TopicPartition> partitions) {

    }

    @Override
    public void onObservedOffset(int partition, String topic, long offset) {

    }

    @Override
    public void configureSafeOffsets(SinkTaskContext sinkTaskContext, long rewindOffset) {
        assert rewindOffset == 0;
    }

    @Override
    public void transformPreCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets, long rewindOffset) {
        assert rewindOffset == 0;
    }
}
