package io.questdb.kafka;

import io.questdb.client.LineSenderServerException;
import io.questdb.client.Sender;
import io.questdb.client.SenderError;
import io.questdb.client.cutlass.line.LineSenderException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QwpSinkTaskTest {
    private static final TopicPartition SOURCE = new TopicPartition("source", 3);

    @Test
    void clampsCommitsToOriginalCoordinatesUntilAcked() {
        FakeSender fakeSender = new FakeSender();
        fakeSender.drainSucceeds = false; // acks arrive only when the test says so
        TestTask task = startTask(fakeSender, 2);

        task.put(java.util.List.of(record(0L, 10L), record(1L, 11L)));
        assertEquals(0, task.fakeContext.requestCommitCalls);

        Map<TopicPartition, OffsetAndMetadata> current = Collections.singletonMap(SOURCE, new OffsetAndMetadata(2L));
        assertEquals(0L, task.preCommit(current).get(SOURCE).offset());
        assertEquals(0, task.fakeContext.requestCommitCalls);
        fakeSender.ackedFsn = 0L;
        assertEquals(2L, task.preCommit(current).get(SOURCE).offset());
        assertEquals(1, task.fakeContext.requestCommitCalls);
        task.preCommit(current);
        assertEquals(1, task.fakeContext.requestCommitCalls);
        assertEquals(1, fakeSender.flushes);
        assertEquals(2, fakeSender.rows);
    }

    @Test
    void tombstonesAndUndeliveredOffsetsDoNotCreateLedgerHoles() {
        FakeSender fakeSender = new FakeSender();
        fakeSender.drainSucceeds = false; // acks arrive only when the test says so
        TestTask task = startTask(fakeSender, 1);

        task.put(Collections.singletonList(tombstone(0L)));
        Map<TopicPartition, OffsetAndMetadata> current = Collections.singletonMap(SOURCE, new OffsetAndMetadata(1L));
        assertEquals(1L, task.preCommit(current).get(SOURCE).offset());

        task.put(Collections.singletonList(record(1L, 12L)));
        current = Collections.singletonMap(SOURCE, new OffsetAndMetadata(2L));
        assertEquals(1L, task.preCommit(current).get(SOURCE).offset());
    }

    @Test
    void clientDlqOnlyBatchDoesNotCreateLedgerHole() {
        TestTask task = startTask(new FakeSender(), 1);
        task.put(Collections.singletonList(recordWithValue(0L, new Object())));
        assertEquals(0, task.fakeContext.requestCommitCalls);

        Map<TopicPartition, OffsetAndMetadata> current = Collections.singletonMap(SOURCE, new OffsetAndMetadata(1L));
        assertEquals(1L, task.preCommit(current).get(SOURCE).offset());
        assertEquals(1, task.fakeContext.requestCommitCalls);
        assertEquals(Collections.singletonList(-1L), task.fakeContext.reportedValues);
    }

    @Test
    void preCommitDefersTerminalFailureToPut() {
        FakeSender fakeSender = new FakeSender();
        TestTask task = startTask(fakeSender, 1);
        task.put(Collections.singletonList(record(0L, 10L)));
        fakeSender.terminal = new LineSenderServerException(new SenderError(
                SenderError.Category.SECURITY_ERROR,
                SenderError.Policy.TERMINAL,
                8,
                "denied",
                1L,
                0L,
                0L,
                null,
                System.nanoTime()));

        assertTrue(task.preCommit(Collections.singletonMap(SOURCE, new OffsetAndMetadata(1L))).isEmpty());
        ConnectException failure = assertThrows(ConnectException.class, () -> task.put(Collections.emptyList()));
        assertTrue(failure.getMessage().contains("SECURITY_ERROR"));
    }

    @Test
    void isolatesRejectedEntryAndDlqsOnlyRejectedRecord() {
        FakeSender initial = new FakeSender();
        FakeSender recoveryOne = new FakeSender();
        recoveryOne.rejectedValue = 11L;
        FakeSender recoveryTwo = new FakeSender();
        TestTask task = startTask(new TestTask(initial, recoveryOne, recoveryTwo), 3);
        task.put(java.util.List.of(record(0L, 10L), record(1L, 11L), record(2L, 12L)));

        initial.terminal = schemaMismatch(0L);
        task.put(Collections.emptyList());

        Map<TopicPartition, OffsetAndMetadata> current = Collections.singletonMap(SOURCE, new OffsetAndMetadata(3L));
        assertEquals(3L, task.preCommit(current).get(SOURCE).offset());
        assertEquals(Collections.singletonList(11L), task.fakeContext.reportedValues);
        assertEquals(2, recoveryOne.rows);
        assertEquals(1, recoveryTwo.rows);
    }

    @Test
    void terminalBeforeAdmissionRedeliversNonEmptyBatch() {
        FakeSender initial = new FakeSender();
        FakeSender recovery = new FakeSender();
        recovery.rejectedValue = 10L;
        FakeSender afterRejected = new FakeSender();
        TestTask task = startTask(new TestTask(initial, recovery, afterRejected), 1);
        task.put(Collections.singletonList(record(0L, 10L)));
        initial.terminal = schemaMismatch(0L);

        assertThrows(RetriableException.class, () -> task.put(Collections.singletonList(record(1L, 20L))));
        assertEquals(1, task.fakeContext.reportedValues.size());
        assertEquals(10L, task.fakeContext.reportedValues.get(0));
    }

    @Test
    void typedTerminalDuringRowBuildingNeverGoesToDlq() {
        FakeSender fakeSender = new FakeSender();
        fakeSender.rowFailure = schemaMismatch(0L);
        TestTask task = startTask(fakeSender, 1);

        ConnectException failure = assertThrows(
                ConnectException.class,
                () -> task.put(Collections.singletonList(record(0L, 10L))));
        assertTrue(failure.getMessage().contains("SCHEMA_MISMATCH"));
        assertTrue(task.fakeContext.reportedValues.isEmpty());
    }

    @Test
    void drainTimeoutDuringProbeDoesNotDlqRecord() {
        FakeSender initial = new FakeSender();
        FakeSender recovery = new FakeSender();
        recovery.drainSucceeds = false;
        TestTask task = startTask(new TestTask(initial, recovery), 1);
        task.put(Collections.singletonList(record(0L, 10L)));
        initial.terminal = schemaMismatch(0L);

        task.put(Collections.emptyList());

        assertTrue(task.fakeContext.reportedValues.isEmpty());
    }

    @Test
    void recoveryReplaysEntriesAfterRejectedEntryAndUnflushedTail() {
        FakeSender initial = new FakeSender();
        FakeSender recovery = new FakeSender();
        TestTask task = startTask(new TestTask(initial, recovery), 2);
        task.put(java.util.List.of(record(0L, 10L), record(1L, 11L)));
        task.put(java.util.List.of(record(2L, 12L), record(3L, 13L)));
        task.put(Collections.singletonList(record(4L, 14L)));

        initial.terminal = schemaMismatch(0L);
        task.put(Collections.emptyList());

        assertEquals(5, recovery.rows);
        assertTrue(task.fakeContext.reportedValues.isEmpty());
        Map<TopicPartition, OffsetAndMetadata> current = Collections.singletonMap(SOURCE, new OffsetAndMetadata(5L));
        assertEquals(5L, task.preCommit(current).get(SOURCE).offset());
    }

    @Test
    void largeIsolationWindowAdvancesAcrossBoundedPutSlices() {
        FakeSender initial = new FakeSender();
        FakeSender recovery = new FakeSender();
        TestTask task = new TestTask(initial, recovery);
        Map<String, String> extra = new HashMap<>();
        extra.put(QuestDBSinkConnectorConfig.QWP_ISOLATION_SLICE_MS_CONFIG, "1");
        startTask(task, 1_000, extra);
        java.util.List<SinkRecord> records = new ArrayList<>(1_000);
        for (int i = 0; i < 1_000; i++) {
            records.add(record(i, i));
        }
        task.put(records);

        task.nanoTimeStep = TimeUnit.MICROSECONDS.toNanos(600);
        initial.terminal = schemaMismatch(0L);
        task.put(Collections.emptyList());
        assertEquals(1, task.fakeContext.pauseCalls);
        assertEquals(0, task.fakeContext.resumeCalls);

        for (int i = 0; i < 2_000 && task.fakeContext.resumeCalls == 0; i++) {
            task.put(Collections.emptyList());
        }
        assertEquals(1, task.fakeContext.resumeCalls);
        assertEquals(1_000, recovery.rows);
    }

    @Test
    void recoveryRecreatesSenderAfterEveryRejectedRecord() {
        FakeSender initial = new FakeSender();
        FakeSender rejectsTen = new FakeSender();
        rejectsTen.rejectedValue = 10L;
        FakeSender rejectsEleven = new FakeSender();
        rejectsEleven.rejectedValue = 11L;
        FakeSender acceptsTwelve = new FakeSender();
        TestTask task = startTask(new TestTask(initial, rejectsTen, rejectsEleven, acceptsTwelve), 3);
        task.put(java.util.List.of(record(0L, 10L), record(1L, 11L), record(2L, 12L)));

        initial.terminal = schemaMismatch(0L);
        task.put(Collections.emptyList());

        assertEquals(java.util.List.of(10L, 11L), task.fakeContext.reportedValues);
        assertEquals(1, rejectsTen.rows);
        assertEquals(1, rejectsEleven.rows);
        assertEquals(1, acceptsTwelve.rows);
        Map<TopicPartition, OffsetAndMetadata> current = Collections.singletonMap(SOURCE, new OffsetAndMetadata(3L));
        assertEquals(3L, task.preCommit(current).get(SOURCE).offset());
    }

    @Test
    void batchDlqModeReportsEntireRetainedWindowForEligibleTerminal() {
        FakeSender initial = new FakeSender();
        TestTask task = new TestTask(initial, new FakeSender());
        startTask(task, 3, Collections.singletonMap(
                QuestDBSinkConnectorConfig.DLQ_SEND_BATCH_ON_ERROR_CONFIG, "true"));
        task.put(java.util.List.of(record(0L, 10L), record(1L, 11L), record(2L, 12L)));

        initial.terminal = schemaMismatch(0L);
        task.put(Collections.emptyList());

        assertEquals(java.util.List.of(10L, 11L, 12L), task.fakeContext.reportedValues);
        Map<TopicPartition, OffsetAndMetadata> current = Collections.singletonMap(SOURCE, new OffsetAndMetadata(3L));
        assertEquals(3L, task.preCommit(current).get(SOURCE).offset());
    }

    @Test
    void partialRevokeRemovesOnlyRevokedRecordsFromRecovery() {
        TopicPartition retainedPartition = new TopicPartition("source", 4);
        FakeSender initial = new FakeSender();
        initial.drainSucceeds = false;
        FakeSender recovery = new FakeSender();
        TestTask task = startTask(new TestTask(initial, recovery), 2);
        task.open(Collections.singleton(retainedPartition));
        task.put(java.util.List.of(record(SOURCE, 0L, 10L), record(retainedPartition, 0L, 20L)));

        task.close(Collections.singleton(SOURCE));
        initial.terminal = schemaMismatch(0L);
        assertTrue(task.preCommit(Collections.singletonMap(retainedPartition, new OffsetAndMetadata(1L))).isEmpty());
        task.put(Collections.emptyList());

        assertEquals(1, recovery.rows);
        assertTrue(task.fakeContext.reportedValues.isEmpty());
        assertEquals(1L, task.preCommit(Collections.singletonMap(retainedPartition, new OffsetAndMetadata(1L))).get(retainedPartition).offset());
    }

    @Test
    void pausesAboveSoftInflightLimitAndResumesAfterAck() {
        FakeSender sender = new FakeSender();
        TestTask task = new TestTask(sender);
        Map<String, String> extra = new HashMap<>();
        extra.put(QuestDBSinkConnectorConfig.QWP_MAX_INFLIGHT_ROWS_CONFIG, "1");
        startTask(task, 2, extra);

        task.put(java.util.List.of(record(0L, 10L), record(1L, 11L)));
        assertEquals(1, task.fakeContext.pauseCalls);

        sender.ackedFsn = 0L;
        task.preCommit(Collections.singletonMap(SOURCE, new OffsetAndMetadata(2L)));
        assertEquals(1, task.fakeContext.resumeCalls);
    }

    @Test
    void stalledPausedTaskFailsFromEmptyPut() {
        TestTask task = new TestTask(new FakeSender());
        Map<String, String> extra = new HashMap<>();
        extra.put(QuestDBSinkConnectorConfig.QWP_MAX_INFLIGHT_ROWS_CONFIG, "1");
        extra.put(QuestDBSinkConnectorConfig.QWP_PROGRESS_TIMEOUT_MS_CONFIG, "1");
        startTask(task, 10, extra);
        task.put(java.util.List.of(record(0L, 10L), record(1L, 11L)));
        assertEquals(1, task.fakeContext.pauseCalls);

        task.nowNanos = TimeUnit.MILLISECONDS.toNanos(2);
        ConnectException failure = assertThrows(ConnectException.class, () -> task.put(Collections.emptyList()));
        assertTrue(failure.getMessage().contains("did not advance"));
    }

    @Test
    void stalledRecoveryFailsFromEmptyPut() {
        FakeSender initial = new FakeSender();
        FakeSender recovery = new FakeSender();
        recovery.drainSucceeds = false;
        TestTask task = startTask(new TestTask(initial, recovery), 1, Collections.singletonMap(
                QuestDBSinkConnectorConfig.QWP_PROGRESS_TIMEOUT_MS_CONFIG, "1"));
        task.put(Collections.singletonList(record(0L, 10L)));
        initial.terminal = schemaMismatch(0L);
        task.put(Collections.emptyList());

        task.nowNanos = TimeUnit.MILLISECONDS.toNanos(2);
        ConnectException failure = assertThrows(ConnectException.class, () -> task.put(Collections.emptyList()));
        assertTrue(failure.getMessage().contains("replay isolation made no progress"));
    }

    @Test
    void validatesExplicitPollIntervalAgainstAppendDeadline() {
        TestTask task = new TestTask(new FakeSender());
        Map<String, String> extra = new HashMap<>();
        extra.put("consumer.override.max.poll.interval.ms", "1000");
        assertThrows(ConfigException.class, () -> startTask(task, 1, extra));
    }

    @Test
    void capacityFailureIsFatalAndNeverDlqEligible() {
        FakeSender fakeSender = new FakeSender();
        fakeSender.rowFailure = new LineSenderException("store-and-forward append deadline exceeded");
        TestTask task = startTask(fakeSender, 1);

        assertThrows(ConnectException.class, () -> task.put(Collections.singletonList(record(0L, 10L))));
        assertTrue(task.fakeContext.reportedValues.isEmpty());
        Map<TopicPartition, OffsetAndMetadata> current = Collections.singletonMap(SOURCE, new OffsetAndMetadata(1L));
        assertEquals(0L, task.preCommit(current).get(SOURCE).offset());
    }

    @Test
    void acceptsAppendDeadlineBelowExplicitPollInterval() {
        TestTask task = new TestTask(new FakeSender());
        Map<String, String> extra = new HashMap<>();
        extra.put("consumer.override.max.poll.interval.ms", "1000");
        extra.put(QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG,
                "ws::addr=localhost:9000;auto_flush_rows=1;sf_append_deadline_millis=999;");
        assertDoesNotThrow(() -> startTask(task, 1, extra));
    }

    @Test
    void rejectsUnknownDlqTerminalCategory() {
        TestTask task = new TestTask(new FakeSender());
        Map<String, String> extra = new HashMap<>();
        extra.put(QuestDBSinkConnectorConfig.QWP_DLQ_TERMINAL_CATEGORIES_CONFIG, "NOT_A_CATEGORY");
        assertThrows(ConfigException.class, () -> startTask(task, 1, extra));
    }

    private static TestTask startTask(FakeSender sender, int flushRows) {
        return startTask(new TestTask(sender), flushRows);
    }

    private static TestTask startTask(TestTask task, int flushRows) {
        return startTask(task, flushRows, Collections.emptyMap());
    }

    private static TestTask startTask(TestTask task, int flushRows, Map<String, String> extra) {
        task.fakeContext = new FakeContext();
        task.initialize(task.fakeContext);
        Map<String, String> props = new HashMap<>();
        props.put(QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG,
                "ws::addr=localhost:9000;auto_flush_rows=" + flushRows + ";auto_flush_interval=60000;");
        props.put(QuestDBSinkConnectorConfig.TABLE_CONFIG, "table");
        props.put(QuestDBSinkConnectorConfig.INCLUDE_KEY_CONFIG, "false");
        props.putAll(extra);
        task.start(props);
        task.open(Collections.singleton(SOURCE));
        return task;
    }

    private static LineSenderServerException schemaMismatch(long fsn) {
        return new LineSenderServerException(new SenderError(
                SenderError.Category.SCHEMA_MISMATCH,
                SenderError.Policy.TERMINAL,
                3,
                "bad column",
                1L,
                fsn,
                fsn,
                null,
                System.nanoTime()));
    }

    private static SinkRecord record(long offset, long value) {
        return record(SOURCE, offset, value);
    }

    private static SinkRecord record(TopicPartition source, long offset, long value) {
        return new SinkRecord(
                "renamed", 9, null, null, null, value, offset,
                null, TimestampType.NO_TIMESTAMP_TYPE, Collections.emptyList(),
                source.topic(), source.partition(), offset);
    }

    private static SinkRecord tombstone(long offset) {
        return new SinkRecord(
                "renamed", 9, null, null, null, null, offset,
                null, TimestampType.NO_TIMESTAMP_TYPE, Collections.emptyList(),
                SOURCE.topic(), SOURCE.partition(), offset);
    }

    private static SinkRecord recordWithValue(long offset, Object value) {
        return new SinkRecord(
                "renamed", 9, null, null, null, value, offset,
                null, TimestampType.NO_TIMESTAMP_TYPE, Collections.emptyList(),
                SOURCE.topic(), SOURCE.partition(), offset);
    }

    private static final class TestTask extends QwpSinkTask {
        private final FakeSender[] senders;
        private int senderIndex;
        private FakeContext fakeContext;
        private long nowNanos;
        private long nanoTimeStep;

        private TestTask(FakeSender... senders) {
            this.senders = senders;
        }

        @Override
        Sender buildSender(String confString) {
            if (senderIndex >= senders.length) {
                throw new AssertionError("Unexpected sender recreation");
            }
            return senders[senderIndex++].proxy();
        }

        @Override
        long nanoTime() {
            long result = nowNanos;
            nowNanos += nanoTimeStep;
            return result;
        }
    }

    private static final class FakeSender {
        private long ackedFsn = -1L;
        private int flushes;
        private int rows;
        private LineSenderServerException terminal;
        private Long rejectedValue;
        private Long currentValue;
        private boolean drainSucceeds = true;
        private RuntimeException rowFailure;
        private final java.util.List<Long> flushedValues = new ArrayList<>();

        private Sender proxy() {
            return (Sender) Proxy.newProxyInstance(
                    Sender.class.getClassLoader(),
                    new Class<?>[]{Sender.class},
                    (proxy, method, args) -> {
                        switch (method.getName()) {
                            case "atNow":
                            case "at":
                                if (rowFailure != null) {
                                    throw rowFailure;
                                }
                                rows++;
                                return null;
                            case "longColumn":
                                currentValue = ((Number) args[1]).longValue();
                                return proxy;
                            case "flushAndGetSequence":
                                if (currentValue != null) {
                                    flushedValues.add(currentValue);
                                    currentValue = null;
                                }
                                return (long) flushes++;
                            case "getAckedFsn":
                                return ackedFsn;
                            case "awaitAckedFsn":
                                if (terminal != null) {
                                    throw terminal;
                                }
                                return (long) args[0] <= ackedFsn;
                            case "drain":
                                if (!drainSucceeds) {
                                    return false;
                                }
                                if (rejectedValue != null && flushedValues.contains(rejectedValue)) {
                                    terminal = schemaMismatch(Math.max(0, flushes - 1L));
                                    throw terminal;
                                }
                                ackedFsn = Math.max(ackedFsn, flushes - 1L);
                                return true;
                            case "close":
                            case "cancelRow":
                            case "reset":
                            case "flush":
                                return null;
                            default:
                                if (Sender.class.isAssignableFrom(method.getReturnType())) {
                                    return proxy;
                                }
                                throw new UnsupportedOperationException(method.getName());
                        }
                    });
        }
    }

    private static final class FakeContext implements SinkTaskContext {
        private final Set<TopicPartition> assignment = new HashSet<>();
        private final java.util.List<Long> reportedValues = new ArrayList<>();
        private int pauseCalls;
        private int requestCommitCalls;
        private int resumeCalls;

        @Override
        public Map<String, String> configs() {
            return Collections.emptyMap();
        }

        @Override
        public void offset(Map<TopicPartition, Long> offsets) {
        }

        @Override
        public void offset(TopicPartition partition, long offset) {
        }

        @Override
        public void timeout(long timeoutMs) {
        }

        @Override
        public Set<TopicPartition> assignment() {
            return assignment;
        }

        @Override
        public void pause(TopicPartition... partitions) {
            pauseCalls++;
            Collections.addAll(assignment, partitions);
        }

        @Override
        public void resume(TopicPartition... partitions) {
            resumeCalls++;
        }

        @Override
        public void requestCommit() {
            requestCommitCalls++;
        }

        @Override
        public ErrantRecordReporter errantRecordReporter() {
            return (record, error) -> {
                reportedValues.add(record.value() instanceof Number ? ((Number) record.value()).longValue() : -1L);
                return CompletableFuture.completedFuture(null);
            };
        }
    }
}
