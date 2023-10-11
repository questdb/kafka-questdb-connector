package io.questdb.kafka;

import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTaskContext;

import java.util.Collection;
import java.util.Map;

public final class MultiTopicPartitionOffsetTracker implements TopicPartitionOffsetTracker {
    private static final int EMPTY = -1;
    private static final int CLOSED = -2;

    private final CharSequenceObjHashMap<LongList> offsets = new CharSequenceObjHashMap<>();

    private String lastTopicCache;
    private LongList lastTopicOffsetsCache;

    @Override
    public void onPartitionsOpened(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            String topic = partition.topic();
            LongList topicOffsets = offsets.get(topic);
            if (topicOffsets == null) {
                topicOffsets = new LongList(4);
                offsets.put(topic, topicOffsets);
            }

            int partitionId = partition.partition();
            int currentSize = topicOffsets.size();
            if (currentSize <= partitionId) {
                topicOffsets.extendAndSet(partitionId, EMPTY);
                if (currentSize != partitionId) {
                    topicOffsets.fill(currentSize, partitionId, EMPTY);
                }
            } else if (topicOffsets.get(partitionId) == CLOSED) {
                topicOffsets.set(partitionId, EMPTY);
            }
        }
    }

    @Override
    public void onPartitionsClosed(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            String topic = partition.topic();
            LongList topicOffsets = offsets.get(topic);
            topicOffsets.set(partition.partition(), CLOSED);
        }

        // Remove topics that have all partitions closed
        ObjList<CharSequence> keys = offsets.keys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            CharSequence topic = keys.getQuick(i);
            LongList topicOffsets = offsets.get(topic);
            boolean allClosed = true;
            for (int partition = 0, m = topicOffsets.size(); partition < m; partition++) {
                if (topicOffsets.get(partition) != CLOSED) {
                    allClosed = false;
                    break;
                }
            }
            if (allClosed) {
                offsets.remove(topic);
            }
        }
    }



    @Override
    public void onObservedOffset(int partition, String topic, long offset) {
        LongList topicOffsets;

        // intentional reference equality check - Kafka Connect use the same String instances
        // so we can avoid hash map lookup
        if (lastTopicCache == topic) {
            topicOffsets = lastTopicOffsetsCache;
        } else {
            topicOffsets = offsets.get(topic);
            lastTopicCache = topic;
            lastTopicOffsetsCache = topicOffsets;
        }
        long maxOffset = topicOffsets.get(partition);
        topicOffsets.set(partition, Math.max(maxOffset, offset));
    }


    @Override
    public void configureSafeOffsets(SinkTaskContext sinkTaskContext, long rewindOffset) {
        ObjList<CharSequence> keys = offsets.keys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            CharSequence topic = keys.getQuick(i);
            LongList topicOffsets = offsets.get(topic);
            for (int partition = 0, m = topicOffsets.size(); partition < m; partition++) {
                long offset = topicOffsets.get(partition);
                // only rewind if we ever observed an offset for this partition
                if (offset >= 0) {
                    sinkTaskContext.offset(new TopicPartition(topic.toString(), partition), Math.max(0, offset - rewindOffset));
                }
            }
        }
    }

    @Override
    public void transformPreCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets, long rewindOffset) {
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : currentOffsets.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            String topic = topicPartition.topic();
            LongList topicOffsets = offsets.get(topic);
            long offset = topicOffsets.get(topicPartition.partition());

            // only transform if we ever observed an offset for this partition
            if (offset >= 0) {
                long newOffset = Math.max(0, offset - rewindOffset);
                entry.setValue(new OffsetAndMetadata(newOffset));
            }
        }
    }
}
