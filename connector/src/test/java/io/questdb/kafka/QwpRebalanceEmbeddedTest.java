package io.questdb.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QwpRebalanceEmbeddedTest {
    private EmbeddedConnectCluster connect;
    private boolean connectStarted;
    private RebalanceGate gate;
    private ScriptedQwpPeer peer;

    @BeforeEach
    void setUp() {
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put("connector.client.config.override.policy", "All");
        workerProps.put("offset.flush.interval.ms", "250");
        workerProps.put("offset.storage.partitions", "1");
        workerProps.put("status.storage.partitions", "1");
        workerProps.put("plugin.discovery", "hybrid_warn");
        connect = new EmbeddedConnectCluster.Builder()
                .name("questdb-qwp-rebalance-cluster")
                .workerProps(workerProps)
                .numWorkers(1)
                .build();
        connect.start();
        connectStarted = true;
    }

    @AfterEach
    void tearDown() {
        if (gate != null) {
            gate.release();
        }
        GatedConnector.gate = null;
        if (connectStarted) {
            connect.stop();
        }
        if (peer != null) {
            peer.close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void lateNackAfterEagerRebalanceIsHandledOnFreshSender(boolean dlqWholeBatch) throws Exception {
        String topicPrefix = ConnectTestUtils.newTopicName();
        String sourceTopic = topicPrefix + "-source";
        String rebalanceTopic = topicPrefix + "-rebalance";
        String dlqTopic = topicPrefix + "-dlq";
        TopicPartition sourcePartition = new TopicPartition(sourceTopic, 0);
        peer = new ScriptedQwpPeer();
        gate = new RebalanceGate();
        GatedConnector.gate = gate;

        connect.kafka().createTopic(sourceTopic, 1);
        connect.kafka().createTopic(dlqTopic, 1);
        connect.configureConnector(ConnectTestUtils.CONNECTOR_NAME,
                connectorProps(topicPrefix, dlqTopic, dlqWholeBatch));
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);

        connect.kafka().produce(sourceTopic, "bad", "{\"value\":10}");
        peer.awaitFirstFrame();

        connect.kafka().createTopic(rebalanceTopic, 1);
        gate.awaitClose();
        peer.rejectHeldFrame();
        gate.release();

        ConsumerRecords<byte[], byte[]> dlqRecords = connect.kafka().consume(1, 60_000, dlqTopic);
        assertEquals(1, dlqRecords.count());
        ConsumerRecord<byte[], byte[]> dlqRecord = dlqRecords.iterator().next();
        assertEquals(dlqTopic, dlqRecord.topic());
        assertEquals("{\"value\":10}", new String(dlqRecord.value(), StandardCharsets.UTF_8));

        try (Admin admin = connect.kafka().createAdminClient()) {
            Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
                OffsetAndMetadata committed = admin.listConsumerGroupOffsets(
                                "connect-" + ConnectTestUtils.CONNECTOR_NAME)
                        .partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS).get(sourcePartition);
                assertNotNull(committed);
                assertEquals(1L, committed.offset());
                long dlqEnd = admin.listOffsets(Map.of(
                                new TopicPartition(dlqTopic, 0), OffsetSpec.latest()))
                        .all().get(10, TimeUnit.SECONDS).get(new TopicPartition(dlqTopic, 0)).offset();
                assertEquals(1L, dlqEnd, "the bad record must be reported exactly once");
            });
        }
        ConnectTestUtils.assertConnectorTaskRunningEventually(connect);
        peer.assertHealthy();
    }

    private Map<String, String> connectorProps(String topicPrefix, String dlqTopic, boolean dlqWholeBatch) {
        Map<String, String> props = new HashMap<>();
        props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, GatedConnector.class.getName());
        props.put(ConnectorConfig.NAME_CONFIG, ConnectTestUtils.CONNECTOR_NAME);
        props.put("topics.regex", topicPrefix + "-(source|rebalance)");
        props.put("tasks.max", "1");
        props.put("key.converter", StringConverter.class.getName());
        props.put("value.converter", JsonConverter.class.getName());
        props.put("value.converter.schemas.enable", "false");
        props.put("consumer.override.metadata.max.age.ms", "250");
        props.put("consumer.override.partition.assignment.strategy", RangeAssignor.class.getName());
        props.put("errors.deadletterqueue.topic.name", dlqTopic);
        props.put("errors.deadletterqueue.topic.replication.factor", "1");
        props.put("errors.tolerance", "all");
        props.put(QuestDBSinkConnectorConfig.DLQ_SEND_BATCH_ON_ERROR_CONFIG,
                Boolean.toString(dlqWholeBatch));
        props.put(QuestDBSinkConnectorConfig.INCLUDE_KEY_CONFIG, "false");
        props.put(QuestDBSinkConnectorConfig.TABLE_CONFIG, "rebalance_test");
        props.put(QuestDBSinkConnectorConfig.ALLOWED_LAG_CONFIG, "25");
        props.put(QuestDBSinkConnectorConfig.QWP_ISOLATION_SLICE_MS_CONFIG, "25");
        props.put(QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG,
                "ws::addr=localhost:" + peer.port()
                        + ";auto_flush_rows=1;auto_flush_interval=25;sf_max_total_bytes=67108864;");
        return props;
    }

    /** Test connector that changes only the task class used by the embedded worker. */
    public static final class GatedConnector extends SinkConnector {
        private static volatile RebalanceGate gate;
        private Map<String, String> props;

        @Override
        public ConfigDef config() {
            return QuestDBSinkConnectorConfig.conf();
        }

        @Override
        public void start(Map<String, String> props) {
            this.props = new HashMap<>(props);
        }

        @Override
        public void stop() {
        }

        @Override
        public Class<? extends Task> taskClass() {
            return GatedTask.class;
        }

        @Override
        public List<Map<String, String>> taskConfigs(int maxTasks) {
            List<Map<String, String>> configs = new ArrayList<>(maxTasks);
            for (int i = 0; i < maxTasks; i++) {
                configs.add(props);
            }
            return configs;
        }

        @Override
        public String version() {
            return VersionUtil.getVersion();
        }
    }

    /** Delegates through the public SinkTask lifecycle and gates after close() returns. */
    public static final class GatedTask extends SinkTask {
        private final QuestDBSinkTask delegate = new QuestDBSinkTask();

        @Override
        public void close(Collection<TopicPartition> partitions) {
            delegate.close(partitions);
            RebalanceGate current = GatedConnector.gate;
            if (current != null) {
                current.afterClose();
            }
        }

        @Override
        public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
            delegate.flush(currentOffsets);
        }

        @Override
        public void open(Collection<TopicPartition> partitions) {
            delegate.open(partitions);
        }

        @Override
        public Map<TopicPartition, OffsetAndMetadata> preCommit(
                Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
            return delegate.preCommit(currentOffsets);
        }

        @Override
        public void put(Collection<SinkRecord> records) {
            delegate.put(records);
        }

        @Override
        public void start(Map<String, String> props) {
            delegate.initialize(context);
            delegate.start(props);
        }

        @Override
        public void stop() {
            delegate.stop();
        }

        @Override
        public String version() {
            return delegate.version();
        }
    }

    private static final class RebalanceGate {
        private final CountDownLatch closed = new CountDownLatch(1);
        private final AtomicBoolean firstClose = new AtomicBoolean();
        private final CountDownLatch released = new CountDownLatch(1);

        private void afterClose() {
            if (!firstClose.compareAndSet(false, true)) {
                return;
            }
            closed.countDown();
            try {
                assertTrue(released.await(30, TimeUnit.SECONDS), "test did not release rebalance close");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }

        private void awaitClose() throws InterruptedException {
            assertTrue(closed.await(60, TimeUnit.SECONDS), "eager rebalance did not close the partition");
        }

        private void release() {
            released.countDown();
        }
    }
}
