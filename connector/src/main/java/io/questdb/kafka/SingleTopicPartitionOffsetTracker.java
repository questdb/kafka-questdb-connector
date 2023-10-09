package io.questdb.kafka;

import io.questdb.std.LongList;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTaskContext;

import java.util.Collection;
import java.util.Map;

public final class SingleTopicPartitionOffsetTracker implements TopicPartitionOffsetTracker {
    private final LongList offsets = new LongList();
    private String topic;

    @Override
    public void onPartitionsOpened(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            if (topic == null) {
                topic = partition.topic();
            } else if (!topic.equals(partition.topic())) {
                throw new IllegalArgumentException("SingleTopicPartitionOffsetTracker can only track a single topic");
            }
            offsets.extendAndSet(partition.partition(), -1);
        }
    }

    @Override
    public void onPartitionsClosed(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            offsets.extendAndSet(partition.partition(), -1);
        }
    }

    @Override
    public void onObservedOffset(int partition, String topic, long offset) {
        long currentOffset = offsets.get(partition);
        if (currentOffset < offset) {
            offsets.extendAndSet(partition, offset);
        }
    }

    @Override
    public void configureSafeOffsets(SinkTaskContext sinkTaskContext, long rewindOffset) {
        assert topic != null;

        for (int partition = 0; partition < offsets.size(); partition++) {
            long offset = offsets.get(partition);
            if (offset != -1) {
                long newOffset = Math.max(0, offset - rewindOffset);
                sinkTaskContext.offset(new TopicPartition(topic, partition), newOffset);
            }
        }
    }

    @Override
    public void transformPreCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets, long rewindOffset) {
        assert topic != null;

        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : currentOffsets.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            if (!topicPartition.topic().equals(topic)) {
                throw new IllegalArgumentException("SingleTopicPartitionOffsetTracker can only track a single topic");
            }
            long offset = offsets.get(topicPartition.partition());
            if (offset != -1) {
                long newOffset = Math.max(0, offset - rewindOffset);
                OffsetAndMetadata offsetAndMetadata = entry.getValue();
                currentOffsets.put(topicPartition, new OffsetAndMetadata(newOffset, offsetAndMetadata.leaderEpoch(), offsetAndMetadata.metadata()));
            }
        }
    }
}
