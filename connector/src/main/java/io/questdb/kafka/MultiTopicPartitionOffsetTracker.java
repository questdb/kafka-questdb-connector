package io.questdb.kafka;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTaskContext;

import java.util.*;

public final class MultiTopicPartitionOffsetTracker implements TopicPartitionOffsetTracker {
    private final List<Map<String, Long>> offsets = new ArrayList<>();

    @Override
    public void onPartitionsOpened(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            if (offsets.size() - 1 < partition.partition() || offsets.get(partition.partition()) == null) {
                Map<String, Long> topic2offset = new HashMap<>();
                topic2offset.put(partition.topic(), -1L);
                offsets.add(partition.partition(), topic2offset);
            } else {
                offsets.get(partition.partition()).put(partition.topic(), -1L);
            }
        }
    }

    @Override
    public void onPartitionsClosed(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            Map<String, Long> topic2offset = offsets.get(partition.partition());
            topic2offset.remove(partition.topic());
            if (topic2offset.isEmpty()) {
                offsets.set(partition.partition(), null);
            }
        }
    }

    @Override
    public void onObservedOffset(int partition, String topic, long offset) {
        Map<String, Long> partitionOffsets = offsets.get(partition);
        Long maxOffset = partitionOffsets.get(topic);
        if (maxOffset < offset) {
            partitionOffsets.put(topic, offset);
        }
    }


    @Override
    public void configureSafeOffsets(SinkTaskContext sinkTaskContext, long rewindOffset) {
        for (int partition = 0; partition < offsets.size(); partition++) {
            Map<String, Long> topicOffsets = offsets.get(partition);
            if (topicOffsets != null) {
                for (Map.Entry<String, Long> entry : topicOffsets.entrySet()) {
                    String topic = entry.getKey();
                    Long offset = entry.getValue();
                    if (offset != -1) {
                        long newOffset = Math.max(0, offset - rewindOffset);
                        sinkTaskContext.offset(new TopicPartition(topic, partition), newOffset);
                    }
                }
            }
        }
    }

    @Override
    public void transformPreCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets, long rewindOffset) {
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : currentOffsets.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            Map<String, Long> partitionOffsets = offsets.get(topicPartition.partition());
            assert partitionOffsets != null;
            Long offset = partitionOffsets.get(topicPartition.topic());
            if (offset != -1) {
                long newOffset = Math.max(0, offset - rewindOffset);
                entry.setValue(new OffsetAndMetadata(newOffset));
            }
        }
    }
}
