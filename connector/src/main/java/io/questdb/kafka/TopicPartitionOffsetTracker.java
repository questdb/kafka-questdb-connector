package io.questdb.kafka;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTaskContext;

import java.util.Collection;
import java.util.Map;

public interface TopicPartitionOffsetTracker {
    void onPartitionsOpened(Collection<TopicPartition> partitions);

    void onPartitionsClosed(Collection<TopicPartition> partitions);

    void onObservedOffset(int partition, String topic, long offset);

    void configureSafeOffsets(SinkTaskContext sinkTaskContext, long rewindOffset);

    void transformPreCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets, long rewindOffset);
}
