package io.questdb.kafka;

import io.questdb.client.LineSenderServerException;
import io.questdb.client.Sender;
import io.questdb.client.SenderError;
import io.questdb.client.cutlass.http.client.HttpClientException;
import io.questdb.client.cutlass.http.client.WebSocketUpgradeException;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.BatchTooLargeForCapException;
import io.questdb.client.cutlass.qwp.client.QwpAuthFailedException;
import io.questdb.client.cutlass.qwp.client.QwpDurableAckMismatchException;
import io.questdb.client.cutlass.qwp.client.QwpProtocolVersionException;
import io.questdb.client.cutlass.qwp.client.QwpRoleMismatchException;
import io.questdb.client.cutlass.qwp.client.QwpVersionMismatchException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QwpSinkTaskTest {
    private static final TopicPartition SOURCE = new TopicPartition("source", 3);
    private static final TopicPartition OTHER = new TopicPartition("source", 4);

    @Test
    void startDoesNotConnectAndFirstPutRetriesBuildFailure() {
        TestTask task = startTask(Collections.emptyList(), 1, Collections.emptyMap(), true);
        task.buildFailure = new LineSenderException(new HttpClientException("offline"));

        assertEquals(0, task.buildCalls);
        RetriableException failure = assertThrows(RetriableException.class,
                () -> task.put(Collections.singletonList(record(0, 10))));
        assertTrue(failure.getMessage().contains("unreachable"));
        assertEquals(Collections.singletonList(3_000L), task.fakeContext.timeouts);
    }

    @ParameterizedTest
    @MethodSource("terminalBuildFailures")
    void firstPutFailsFastForTerminalBuildFailure(RuntimeException buildFailure) {
        TestTask task = startTask(Collections.emptyList(), 1, Collections.emptyMap(), true);
        task.buildFailure = buildFailure;

        ConnectException failure = assertThrows(ConnectException.class,
                () -> task.put(Collections.singletonList(record(0, 10))));

        assertFalse(failure instanceof RetriableException);
        assertEquals(buildFailure, failure.getCause());
        assertTrue(task.fakeContext.timeouts.isEmpty());
    }

    @ParameterizedTest
    @MethodSource("transientBuildFailures")
    void firstPutRetriesOnlyTransientBuildFailure(RuntimeException buildFailure) {
        TestTask task = startTask(Collections.emptyList(), 1, Collections.emptyMap(), true);
        task.buildFailure = buildFailure;

        RetriableException failure = assertThrows(RetriableException.class,
                () -> task.put(Collections.singletonList(record(0, 10))));

        assertEquals(buildFailure, failure.getCause());
        assertEquals(Collections.singletonList(3_000L), task.fakeContext.timeouts);
    }

    @Test
    void checkpointClampsUntilAcknowledgedThenMovesCommitForward() {
        FakeSender sender = new FakeSender();
        sender.drainSucceeds = false;
        TestTask task = startTask(sender, 2);

        task.put(List.of(record(0, 10), record(1, 11)));
        assertOffset(0, task.preCommit(offsets(SOURCE, 2)));
        sender.ackedFsn = 0;
        assertOffset(2, task.preCommit(offsets(SOURCE, 2)));
        assertEquals(1, task.fakeContext.requestCommitCalls);
    }

    @Test
    void oldestCheckpointClampsCommitWhenMultipleAreOutstanding() {
        FakeSender sender = new FakeSender();
        sender.drainSucceeds = false;
        TestTask task = startTask(sender, 1);

        task.put(Collections.singletonList(record(0, 10)));
        task.put(Collections.singletonList(record(1, 11)));

        assertOffset(0, task.preCommit(offsets(SOURCE, 2)));
    }

    @Test
    void unknownCheckpointInheritsNextPublishedFsn() {
        FakeSender sender = new FakeSender();
        sender.flushResults.add(-1L);
        sender.flushResults.add(7L);
        sender.zeroDrainSucceeds = false;
        sender.drainSucceeds = false;
        TestTask task = startTask(sender, 1);

        task.put(Collections.singletonList(record(0, 10)));
        task.put(Collections.singletonList(record(1, 11)));
        sender.ackedFsn = 7;
        task.put(Collections.singletonList(tombstone(2)));

        assertOffset(3, task.preCommit(offsets(SOURCE, 3)));
    }

    @Test
    void trailingUnknownCheckpointUsesZeroDrainOnlyWithoutBufferedRows() {
        FakeSender sender = new FakeSender();
        sender.flushResults.add(-1L);
        sender.zeroDrainSucceeds = true;
        TestTask task = startTask(sender, 1);

        task.put(Collections.singletonList(record(0, 10)));
        task.put(Collections.singletonList(tombstone(1)));

        assertTrue(sender.drainTimeouts.contains(0L));
        assertOffset(2, task.preCommit(offsets(SOURCE, 2)));
    }

    @Test
    void probeDoesNotFlushBufferedRows() {
        FakeSender sender = new FakeSender();
        TestTask task = startTask(sender, 10);

        task.put(Collections.singletonList(record(0, 10)));
        task.put(Collections.singletonList(tombstone(1)));

        assertEquals(0, sender.flushes);
    }

    @Test
    void idlePutCheckpointsBeforeProbeAndSettlesInSameTick() {
        FakeSender sender = new FakeSender();
        sender.ackOnFlush = true;
        TestTask task = startTask(sender, 10);
        task.put(Collections.singletonList(record(0, 10)));

        task.put(Collections.emptyList());

        assertEquals(1, sender.flushes);
        assertEquals(0, field(task, "bufferedRows"));
        assertTrue(((Collection<?>) field(task, "checkpoints")).isEmpty());
        assertEquals(1, task.fakeContext.requestCommitCalls);
    }

    @Test
    void tombstoneOnlyPartitionPassesThroughPreCommit() {
        TestTask task = startTask(new FakeSender(), 10);
        task.put(Collections.singletonList(tombstone(0)));
        assertOffset(1, task.preCommit(offsets(SOURCE, 1)));
    }

    @Test
    void rewindPendingClampsDroppedBatchUntilNextPut() {
        FakeSender failed = new FakeSender();
        failed.flushFailure = new LineSenderException("ring full");
        FakeSender replacement = new FakeSender();
        TestTask task = startTask(List.of(failed, replacement), 1, Collections.emptyMap(), true);

        task.put(Collections.singletonList(record(0, 10)));
        assertEquals(0L, task.fakeContext.rewinds.get(0).get(SOURCE));
        assertOffset(0, task.preCommit(offsets(SOURCE, 1)));

        task.put(Collections.singletonList(record(0, 10)));
        assertOffset(1, task.preCommit(offsets(SOURCE, 1)));
    }

    @Test
    void rejectionRewindsOutstandingWindowAndDropsIncomingBatch() {
        FakeSender initial = new FakeSender();
        FakeSender quarantine = new FakeSender();
        TestTask task = startTask(List.of(initial, quarantine), 1, Collections.emptyMap(), true);
        task.put(Collections.singletonList(record(0, 10)));
        initial.awaitFailure = schemaMismatch(0);

        task.put(Collections.singletonList(record(1, 11)));

        assertEquals(0L, task.fakeContext.rewinds.get(0).get(SOURCE));
        assertEquals(Collections.singletonList(10L), initial.writtenValues);
        assertEquals(0, quarantine.rows);
        assertEquals("QUARANTINE", field(task, "mode").toString());
    }

    @Test
    void rejectionPrunesAcknowledgedCheckpointsBeforeRewind() {
        FakeSender initial = new FakeSender();
        FakeSender quarantine = new FakeSender();
        TestTask task = startTask(List.of(initial, quarantine), 1, Collections.emptyMap(), true);
        task.put(Collections.singletonList(record(0, 10)));
        task.put(Collections.singletonList(record(1, 11)));
        initial.ackedFsn = 0;
        initial.awaitFailure = schemaMismatch(1);

        task.put(Collections.emptyList());

        assertEquals(1L, task.fakeContext.rewinds.get(0).get(SOURCE));
        assertEquals(Collections.singletonMap(SOURCE, 2L), field(task, "quarantineUntil"));
        assertEquals("QUARANTINE", field(task, "mode").toString());
    }

    @Test
    void rejectionDuringLargePollKeepsOnlyUnresolvedAndUnprocessedSuffix() {
        FakeSender sender = new FakeSender();
        sender.ackedFsn = 0;
        sender.flushFailure = schemaMismatch(1);
        sender.flushFailureAt = 1;
        TestTask task = startTask(sender, 2);
        List<SinkRecord> batch = List.of(
                record(0, 10), record(1, 11), record(2, 12), record(3, 13), record(4, 14));

        task.put(batch);

        assertEquals(List.of(10L, 11L, 12L, 13L), sender.writtenValues);
        assertEquals(2L, task.fakeContext.rewinds.get(0).get(SOURCE));
        assertEquals(Collections.singletonMap(SOURCE, 4L), field(task, "quarantineUntil"));
    }

    @Test
    void rejectionWithoutUsableDlqIsTerminalAndDiagnostic() {
        FakeSender sender = new FakeSender();
        Map<String, String> props = Collections.singletonMap("errors.tolerance", "none");
        TestTask task = startTask(List.of(sender), 1, props, true);
        task.put(Collections.singletonList(record(0, 10)));
        sender.awaitFailure = schemaMismatch(0);

        ConnectException failure = assertThrows(ConnectException.class,
                () -> task.put(Collections.emptyList()));
        assertTrue(failure.getMessage().contains("SCHEMA_MISMATCH"));
        assertTrue(task.fakeContext.reportedValues.isEmpty());
        assertTrue(task.fakeContext.rewinds.isEmpty());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void revokedOnlyLateRejectionDoesNotRequireUsableDlqAndRewindsOwnedBatch(boolean hasReporter) {
        FakeSender sender = new FakeSender();
        Map<String, String> props = hasReporter
                ? Collections.singletonMap("errors.tolerance", "none")
                : Collections.emptyMap();
        TestTask task = startTask(List.of(sender), 1, props, hasReporter);
        task.open(Collections.singleton(OTHER));
        task.fakeContext.assignment.add(OTHER);
        task.put(Collections.singletonList(record(SOURCE, 0, 10)));
        task.close(Collections.singleton(SOURCE));
        sender.awaitFailure = serverFailure(SenderError.Category.SECURITY_ERROR, 0);

        assertDoesNotThrow(() -> task.put(Collections.singletonList(record(OTHER, 0, 20))));

        assertEquals(Collections.singletonMap(OTHER, 0L), task.fakeContext.rewinds.get(0));
        assertEquals(1, sender.closeCalls);
        assertNull(field(task, "sender"));
        assertEquals("PIPELINED", field(task, "mode").toString());
        assertTrue(task.fakeContext.reportedValues.isEmpty());
    }

    @Test
    void partialRevocationRetiresSenderBeforeItsLateRejectionCanBlameSurvivors() {
        FakeSender initial = new FakeSender();
        FakeSender fresh = new FakeSender();
        TestTask task = startTask(List.of(initial, fresh), 2, Collections.emptyMap(), true);
        task.open(Collections.singleton(OTHER));
        task.fakeContext.assignment.add(OTHER);
        // one unacknowledged checkpoint holding rows of both partitions
        task.put(List.of(record(SOURCE, 0, 10), record(OTHER, 0, 20)));

        task.close(Collections.singleton(SOURCE));
        assertEquals(0, initial.closeCalls, "close() itself must not touch the sender");
        // a late, non-eligible rejection for the shared frame lands on the stale sender
        initial.awaitFailure = serverFailure(SenderError.Category.SECURITY_ERROR, 0);

        assertEquals(0, task.preCommit(offsets(OTHER, 1)).get(OTHER).offset(), "a stale sender is not queried");
        assertDoesNotThrow(() -> task.put(Collections.singletonList(record(OTHER, 1, 21))));

        assertEquals(Collections.singletonMap(OTHER, 0L), task.fakeContext.rewinds.get(0));
        assertEquals(1, initial.closeCalls);
        assertEquals("PIPELINED", field(task, "mode").toString());
        assertTrue(task.fakeContext.reportedValues.isEmpty());

        task.put(List.of(record(OTHER, 0, 20), record(OTHER, 1, 21)));
        assertEquals(List.of(20L, 21L), fresh.writtenValues);
        assertEquals(2, task.preCommit(offsets(OTHER, 2)).get(OTHER).offset());
    }

    @Test
    void partialRevocationDuringQuarantineRetiresSenderAndKeepsBatch() {
        FakeSender initial = new FakeSender();
        FakeSender timedOut = new FakeSender();
        timedOut.drainSucceeds = false;
        FakeSender fresh = new FakeSender();
        TestTask task = startTask(List.of(initial, timedOut, fresh), 2, Collections.emptyMap(), true);
        task.open(Collections.singleton(OTHER));
        task.fakeContext.assignment.add(OTHER);
        List<SinkRecord> batch = List.of(record(SOURCE, 0, 10), record(OTHER, 0, 20));
        task.put(batch);
        initial.awaitFailure = schemaMismatch(0);
        task.put(Collections.emptyList());
        assertThrows(RetriableException.class, () -> task.put(batch));

        task.close(Collections.singleton(SOURCE));
        RetriableException retired = assertThrows(RetriableException.class,
                () -> task.put(Collections.singletonList(record(OTHER, 0, 20))));
        assertTrue(retired.getMessage().contains("partition revocation"));
        assertEquals(1, timedOut.closeCalls);

        task.put(Collections.singletonList(record(OTHER, 0, 20)));
        assertEquals(List.of(20L), fresh.writtenValues);
        assertEquals("PIPELINED", field(task, "mode").toString());
    }

    @Test
    void nonEligibleRejectionIsTerminal() {
        FakeSender sender = new FakeSender();
        TestTask task = startTask(sender, 1);
        task.put(Collections.singletonList(record(0, 10)));
        sender.awaitFailure = serverFailure(SenderError.Category.SECURITY_ERROR, 0);

        ConnectException failure = assertThrows(ConnectException.class,
                () -> task.put(Collections.emptyList()));
        assertTrue(failure.getMessage().contains("SECURITY_ERROR"));
    }

    @Test
    void rejectionDeferredByPreCommitIsRecoveredByPut() {
        FakeSender initial = new FakeSender();
        FakeSender replacement = new FakeSender();
        TestTask task = startTask(List.of(initial, replacement), 1, Collections.emptyMap(), true);
        task.put(Collections.singletonList(record(0, 10)));
        initial.awaitFailure = schemaMismatch(0);

        assertOffset(0, task.preCommit(offsets(SOURCE, 1)));
        assertEquals(Collections.singletonList(1L), task.fakeContext.timeouts.subList(
                task.fakeContext.timeouts.size() - 1, task.fakeContext.timeouts.size()));
        task.put(Collections.emptyList());

        assertEquals(0L, task.fakeContext.rewinds.get(0).get(SOURCE));
    }

    @Test
    void transportFailureRewindsRetiresAndDropsBatch() {
        FakeSender sender = new FakeSender();
        sender.flushFailure = new LineSenderException("append deadline");
        TestTask task = startTask(sender, 1);

        assertDoesNotThrow(() -> task.put(Collections.singletonList(record(0, 10))));
        assertEquals(0L, task.fakeContext.rewinds.get(0).get(SOURCE));
        assertEquals(1, sender.closeCalls);
        assertNull(field(task, "sender"));
    }

    @Test
    void senderDeathHandsBackBackpressurePauseSoRewindCanRun() {
        FakeSender sender = new FakeSender();
        Map<String, String> props = Collections.singletonMap(
                QuestDBSinkConnectorConfig.QWP_MAX_INFLIGHT_ROWS_CONFIG, "1");
        TestTask task = startTask(sender, 2, props, true);
        task.put(List.of(record(0, 10), record(1, 11)));
        assertEquals(1, task.fakeContext.pauseCalls);
        sender.awaitFailure = new LineSenderException("connection lost");

        task.put(Collections.emptyList());

        assertEquals(1, task.fakeContext.resumeCalls);
        assertEquals(0L, task.fakeContext.rewinds.get(0).get(SOURCE));
    }

    @Test
    void acknowledgedCheckpointResumesBackpressuredPartitions() {
        FakeSender sender = new FakeSender();
        sender.drainSucceeds = false;
        Map<String, String> props = Collections.singletonMap(
                QuestDBSinkConnectorConfig.QWP_MAX_INFLIGHT_ROWS_CONFIG, "1");
        TestTask task = startTask(sender, 2, props, true);
        task.put(List.of(record(0, 10), record(1, 11)));
        assertEquals(1, task.fakeContext.pauseCalls);

        sender.ackedFsn = 0;
        task.put(Collections.emptyList());

        assertEquals(1, task.fakeContext.resumeCalls);
    }

    @Test
    void transportFailureNeverRewindsRevokedOriginalPartition() {
        FakeSender sender = new FakeSender();
        sender.flushFailure = new LineSenderException("append deadline");
        TestTask task = startTask(sender, 1);
        task.close(Collections.singleton(SOURCE));

        task.put(Collections.singletonList(record(0, 10)));

        assertTrue(task.fakeContext.rewinds.isEmpty());
        assertEquals(0, sender.rows);
    }

    @Test
    void healthySetterFailureIsRecordLocal() {
        FakeSender sender = new FakeSender();
        sender.setterFailure = new LineSenderException("bad type");
        TestTask task = startTask(sender, 1);

        task.put(Collections.singletonList(record(0, 10)));

        assertEquals(Collections.singletonList(10L), task.fakeContext.reportedValues);
        assertEquals(0, sender.closeCalls);
        assertTrue(task.fakeContext.rewinds.isEmpty());
    }

    @Test
    void healthySetterFailureWithoutDlqPreservesClientCause() {
        FakeSender sender = new FakeSender();
        sender.setterFailure = new LineSenderException("bad type");
        TestTask task = startTask(List.of(sender), 1, Collections.emptyMap(), false);

        InvalidDataException failure = assertThrows(InvalidDataException.class,
                () -> task.put(Collections.singletonList(record(0, 10))));
        assertEquals("bad type", failure.getCause().getMessage());
    }

    @Test
    void setterFailureWithLatchedRejectionUsesRecovery() {
        FakeSender sender = new FakeSender();
        sender.setterFailure = new LineSenderException("setter");
        sender.awaitFailure = schemaMismatch(0);
        TestTask task = startTask(sender, 1);

        task.put(Collections.singletonList(record(0, 10)));

        assertEquals(0L, task.fakeContext.rewinds.get(0).get(SOURCE));
        assertEquals(1, sender.closeCalls);
    }

    @Test
    void typedSetterRejectionUsesRecoveryWithoutLatchedFailure() {
        FakeSender sender = new FakeSender();
        TestTask task = startTask(sender, 1);
        task.put(Collections.singletonList(record(0, 10)));
        sender.setterFailure = schemaMismatch(0);

        task.put(Collections.singletonList(record(1, 11)));

        assertEquals(0L, task.fakeContext.rewinds.get(0).get(SOURCE));
        assertEquals(1, sender.closeCalls);
        assertEquals("QUARANTINE", field(task, "mode").toString());
        assertTrue(task.fakeContext.reportedValues.isEmpty());
    }

    @Test
    void rowTooLargeIsDlqdWithoutRetiringHealthySender() {
        FakeSender sender = new FakeSender();
        sender.rowFailure = new LineSenderException("row too large for server batch cap [size=99]");
        TestTask task = startTask(sender, 1);

        task.put(Collections.singletonList(record(0, 10)));

        assertEquals(Collections.singletonList(10L), task.fakeContext.reportedValues);
        assertEquals(0, sender.closeCalls);
    }

    @Test
    void batchTooLargeEntersQuarantine() {
        FakeSender sender = new FakeSender();
        sender.flushFailure = new BatchTooLargeForCapException("too large");
        TestTask task = startTask(sender, 1);

        task.put(Collections.singletonList(record(0, 10)));

        assertEquals("QUARANTINE", field(task, "mode").toString());
        assertEquals(0L, task.fakeContext.rewinds.get(0).get(SOURCE));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void firstRecordAutoFlushBatchTooLargeEntersQuarantine(boolean kafkaTimestamp) {
        FakeSender initial = new FakeSender();
        initial.autoFlushFailure = new BatchTooLargeForCapException("too large");
        FakeSender quarantine = new FakeSender();
        Map<String, String> props = kafkaTimestamp
                ? Collections.singletonMap(
                        QuestDBSinkConnectorConfig.DESIGNATED_TIMESTAMP_KAFKA_NATIVE_CONFIG, "true")
                : Collections.emptyMap();
        TestTask task = startTask(List.of(initial, quarantine), 1, props, true);
        SinkRecord first = kafkaTimestamp ? recordWithTimestamp(0, 10) : record(0, 10);

        task.put(Collections.singletonList(first));

        assertEquals(kafkaTimestamp ? "at" : "atNow", initial.lastCommitMethod);
        assertEquals("QUARANTINE", field(task, "mode").toString());
        assertEquals(Collections.singletonMap(SOURCE, 1L), field(task, "quarantineUntil"));
        assertEquals(0L, task.fakeContext.rewinds.get(0).get(SOURCE));

        task.put(Collections.singletonList(first));
        assertEquals("PIPELINED", field(task, "mode").toString());
        assertOffset(1, task.preCommit(offsets(SOURCE, 1)));
    }

    @Test
    void repeatedBuildFailuresTripProgressTimeout() {
        Map<String, String> props = Collections.singletonMap(
                QuestDBSinkConnectorConfig.QWP_PROGRESS_TIMEOUT_MS_CONFIG, "10");
        TestTask task = startTask(Collections.emptyList(), 1, props, true);
        task.buildFailure = new LineSenderException(new HttpClientException("offline"));

        assertThrows(RetriableException.class, () -> task.put(Collections.singletonList(record(0, 10))));
        task.nowNanos = TimeUnit.MILLISECONDS.toNanos(9);
        assertThrows(RetriableException.class, () -> task.put(Collections.singletonList(record(0, 10))));
        task.nowNanos = TimeUnit.MILLISECONDS.toNanos(10);
        assertThrows(ConnectException.class, () -> task.put(Collections.singletonList(record(0, 10))));
    }

    @Test
    void probeSettlesAckThatLandedAtProgressTimeout() {
        FakeSender sender = new FakeSender();
        Map<String, String> props = Collections.singletonMap(
                QuestDBSinkConnectorConfig.QWP_PROGRESS_TIMEOUT_MS_CONFIG, "10");
        TestTask task = startTask(sender, 1, props, true);
        task.put(Collections.singletonList(record(0, 10)));

        sender.ackedFsn = 0L;
        task.nowNanos = TimeUnit.MILLISECONDS.toNanos(10);

        assertDoesNotThrow(() -> task.put(Collections.emptyList()));
        assertOffset(1, task.preCommit(offsets(SOURCE, 1)));
    }

    @Test
    void idleTaskStartsNewStallEpochWhenWorkArrives() {
        FakeSender sender = new FakeSender();
        sender.flushFailure = new LineSenderException("ring full");
        Map<String, String> props = Collections.singletonMap(
                QuestDBSinkConnectorConfig.QWP_PROGRESS_TIMEOUT_MS_CONFIG, "10");
        TestTask task = startTask(sender, 1, props, true);
        task.put(Collections.emptyList());
        task.nowNanos = TimeUnit.SECONDS.toNanos(1);

        assertDoesNotThrow(() -> task.put(Collections.singletonList(record(0, 10))));
    }

    @Test
    void quarantineDeliversSynchronouslyAndTerminates() {
        FakeSender initial = new FakeSender();
        FakeSender quarantine = new FakeSender();
        TestTask task = enterQuarantine(initial, quarantine,
                List.of(record(0, 10), record(1, 11)));

        task.put(List.of(record(0, 10), record(1, 11)));

        assertEquals(List.of(10L, 11L), quarantine.writtenValues);
        assertEquals(List.of(1_000L), quarantine.drainTimeouts);
        assertEquals("PIPELINED", field(task, "mode").toString());
        assertOffset(2, task.preCommit(offsets(SOURCE, 2)));
    }

    @Test
    void quarantineBisectsAndReportsOnlyBadRecord() {
        FakeSender initial = new FakeSender();
        List<FakeSender> senders = new ArrayList<>();
        senders.add(initial);
        for (int i = 0; i < 6; i++) {
            FakeSender sender = new FakeSender();
            sender.rejectedValues.add(11L);
            senders.add(sender);
        }
        TestTask task = startTask(senders, 3, Collections.emptyMap(), true);
        List<SinkRecord> batch = List.of(record(0, 10), record(1, 11), record(2, 12));
        task.put(batch);
        initial.awaitFailure = schemaMismatch(0);
        task.put(Collections.emptyList());

        task.put(batch);

        assertEquals(Collections.singletonList(11L), task.fakeContext.reportedValues);
        assertEquals("PIPELINED", field(task, "mode").toString());
    }

    @Test
    void batchDlqReportsRejectedChunk() {
        FakeSender initial = new FakeSender();
        FakeSender quarantine = new FakeSender();
        quarantine.rejectedValues.add(11L);
        Map<String, String> props = Collections.singletonMap(
                QuestDBSinkConnectorConfig.DLQ_SEND_BATCH_ON_ERROR_CONFIG, "true");
        TestTask task = startTask(List.of(initial, quarantine), 3, props, true);
        List<SinkRecord> batch = List.of(record(0, 10), record(1, 11), record(2, 12));
        task.put(batch);
        initial.awaitFailure = schemaMismatch(0);
        task.put(Collections.emptyList());

        task.put(batch);

        assertEquals(List.of(10L, 11L, 12L), task.fakeContext.reportedValues);
    }

    @Test
    void batchDlqNeverReportsMoreThanOneCheckpointChunk() {
        FakeSender initial = new FakeSender();
        FakeSender rejected = new FakeSender();
        rejected.rejectedValues.add(11L);
        FakeSender accepted = new FakeSender();
        Map<String, String> props = Collections.singletonMap(
                QuestDBSinkConnectorConfig.DLQ_SEND_BATCH_ON_ERROR_CONFIG, "true");
        TestTask task = startTask(List.of(initial, rejected, accepted), 2, props, true);
        List<SinkRecord> batch = List.of(
                record(0, 10), record(1, 11), record(2, 12), record(3, 13), record(4, 14));
        task.put(batch);
        initial.awaitFailure = schemaMismatch(0);
        task.put(Collections.emptyList());

        task.put(batch);

        assertEquals(List.of(10L, 11L), task.fakeContext.reportedValues);
        assertEquals(List.of(10L, 11L), rejected.writtenValues);
        assertEquals(List.of(12L, 13L, 14L), accepted.writtenValues);
        assertEquals(Collections.singletonList(1_000L), rejected.drainTimeouts);
        assertEquals(List.of(1_000L, 1_000L), accepted.drainTimeouts);
    }

    @Test
    void quarantineTimeoutRedeliveryDrainsWithoutRewriting() {
        FakeSender initial = new FakeSender();
        FakeSender quarantine = new FakeSender();
        quarantine.drainSucceeds = false;
        TestTask task = enterQuarantine(initial, quarantine, Collections.singletonList(record(0, 10)));

        RetriableException failure = assertThrows(RetriableException.class,
                () -> task.put(Collections.singletonList(record(0, 10))));
        assertTrue(failure.getMessage().contains("not yet acknowledged"));
        assertEquals(1L, task.fakeContext.timeouts.get(task.fakeContext.timeouts.size() - 1));
        quarantine.drainSucceeds = true;
        task.put(Collections.singletonList(record(0, 10)));

        assertEquals(1, quarantine.rows);
    }

    @Test
    void silentServerDuringQuarantineTripsProgressTimeout() {
        FakeSender initial = new FakeSender();
        FakeSender quarantine = new FakeSender();
        quarantine.drainSucceeds = false;
        Map<String, String> props = Collections.singletonMap(
                QuestDBSinkConnectorConfig.QWP_PROGRESS_TIMEOUT_MS_CONFIG, "10");
        TestTask task = startTask(List.of(initial, quarantine), 1, props, true);
        List<SinkRecord> batch = Collections.singletonList(record(0, 10));
        task.put(batch);
        initial.awaitFailure = schemaMismatch(0);
        task.put(Collections.emptyList());

        assertThrows(RetriableException.class, () -> task.put(batch));
        task.nowNanos = TimeUnit.MILLISECONDS.toNanos(9);
        assertThrows(RetriableException.class, () -> task.put(batch));
        task.nowNanos = TimeUnit.MILLISECONDS.toNanos(10);

        ConnectException failure = assertThrows(ConnectException.class, () -> task.put(batch));
        assertFalse(failure instanceof RetriableException);
        assertTrue(failure.getMessage().contains("did not advance"));
    }

    @Test
    void typedRejectionDuringResumeDoesNotDisposeOldSpan() {
        FakeSender initial = new FakeSender();
        FakeSender timedOut = new FakeSender();
        timedOut.drainSucceeds = false;
        FakeSender accepted = new FakeSender();
        TestTask task = startTask(List.of(initial, timedOut, accepted), 1, Collections.emptyMap(), true);
        List<SinkRecord> batch = Collections.singletonList(record(0, 10));
        task.put(batch);
        initial.awaitFailure = schemaMismatch(0);
        task.put(Collections.emptyList());
        assertThrows(RetriableException.class, () -> task.put(batch));
        timedOut.drainFailure = schemaMismatch(0);

        task.put(batch);

        assertEquals(Collections.singletonList(10L), task.fakeContext.reportedValues);
        assertEquals(0, accepted.drainTimeouts.size(), "the old span must not be drained on a fresh sender");
    }

    @Test
    void tombstoneInTimedOutChunkDoesNotAdvanceDispositionEarly() {
        FakeSender initial = new FakeSender();
        FakeSender quarantine = new FakeSender();
        quarantine.drainSucceeds = false;
        List<SinkRecord> batch = List.of(record(0, 10), tombstone(1), record(2, 12));
        TestTask task = enterQuarantine(initial, quarantine, batch);

        assertThrows(RetriableException.class, () -> task.put(batch));
        quarantine.drainSucceeds = true;
        task.put(batch);

        assertEquals(List.of(10L, 12L), quarantine.writtenValues);
        assertEquals(2, quarantine.rows);
    }

    @Test
    void pendingRewindPreventsPrematureQuarantineExit() {
        FakeSender initial = new FakeSender();
        FakeSender quarantine = new FakeSender();
        TestTask task = enterQuarantine(initial, quarantine, Collections.singletonList(record(0, 10)));

        task.preCommit(offsets(SOURCE, 1));
        assertEquals("QUARANTINE", field(task, "mode").toString());
        task.put(Collections.singletonList(record(0, 10)));
        assertEquals("PIPELINED", field(task, "mode").toString());
    }

    @Test
    void revokingAwaitedPartitionRetiresSenderAndRedeliveryWritesAgain() {
        FakeSender initial = new FakeSender();
        FakeSender timedOut = new FakeSender();
        timedOut.drainSucceeds = false;
        FakeSender fresh = new FakeSender();
        TestTask task = startTask(List.of(initial, timedOut, fresh), 1, Collections.emptyMap(), true);
        List<SinkRecord> batch = Collections.singletonList(record(0, 10));
        task.put(batch);
        initial.awaitFailure = schemaMismatch(0);
        task.put(Collections.emptyList());
        assertThrows(RetriableException.class, () -> task.put(batch));

        task.close(Collections.singleton(SOURCE));
        task.open(Collections.singleton(SOURCE));
        task.fakeContext.assignment.add(SOURCE);
        // the sender that flushed the revoked chunk is stale: it is retired before it is asked anything
        RetriableException retired = assertThrows(RetriableException.class, () -> task.put(batch));
        assertTrue(retired.getMessage().contains("partition revocation"));
        assertEquals(1, timedOut.closeCalls);
        assertEquals(1, timedOut.rows);

        task.put(batch);

        assertEquals(1, fresh.rows, "the redelivered chunk is written on a fresh sender");
        assertEquals("PIPELINED", field(task, "mode").toString());
    }

    @Test
    void closeDoesNotTouchSenderAndRemovesOnlyRevokedState() {
        FakeSender sender = new FakeSender();
        sender.drainSucceeds = false;
        TestTask task = startTask(sender, 10);
        task.open(Collections.singleton(OTHER));
        task.fakeContext.assignment.add(OTHER);
        task.put(List.of(record(SOURCE, 0, 10), record(OTHER, 0, 20)));

        task.close(Collections.singleton(SOURCE));

        assertEquals(0, sender.closeCalls);
        Map<TopicPartition, OffsetAndMetadata> current = new HashMap<>();
        current.put(SOURCE, new OffsetAndMetadata(1));
        current.put(OTHER, new OffsetAndMetadata(1));
        Map<TopicPartition, OffsetAndMetadata> clamped = task.preCommit(current);
        assertEquals(1, clamped.get(SOURCE).offset());
        assertEquals(0, clamped.get(OTHER).offset());
    }

    @Test
    void stopAndRetirementBoundBlockingClose() {
        FakeSender sender = new FakeSender();
        sender.blockClose = true;
        TestTask task = startTask(sender, 10);
        task.put(Collections.emptyList());

        long started = System.nanoTime();
        task.stop();
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

        assertTrue(elapsedMillis >= 900 && elapsedMillis < 2_500, "elapsed=" + elapsedMillis);
    }

    @Test
    void taskRetainsNoSinkRecordAfterPutReturns() throws IllegalAccessException {
        TestTask task = startTask(new FakeSender(), 1);
        task.put(Collections.singletonList(record(0, 10)));

        for (Field field : QwpSinkTask.class.getDeclaredFields()) {
            field.setAccessible(true);
            Object value = field.get(task);
            assertFalse(value instanceof SinkRecord, field.getName());
            if (value instanceof Collection<?>) {
                assertFalse(((Collection<?>) value).stream().anyMatch(SinkRecord.class::isInstance), field.getName());
            }
        }
    }

    @Test
    void invalidCategoryFailsAtStart() {
        Map<String, String> props = Collections.singletonMap(
                QuestDBSinkConnectorConfig.QWP_DLQ_TERMINAL_CATEGORIES_CONFIG, "NOT_A_CATEGORY");
        assertThrows(ConfigException.class,
                () -> startTask(Collections.emptyList(), 1, props, true));
    }

    private static TestTask enterQuarantine(FakeSender initial, FakeSender quarantine, List<SinkRecord> batch) {
        TestTask task = startTask(List.of(initial, quarantine), batch.size(), Collections.emptyMap(), true);
        task.put(batch);
        initial.awaitFailure = schemaMismatch(0);
        task.put(Collections.emptyList());
        return task;
    }

    private static void assertOffset(long expected, Map<TopicPartition, OffsetAndMetadata> offsets) {
        assertEquals(expected, offsets.get(SOURCE).offset());
    }

    private static Object field(Object target, String name) {
        try {
            Field field = QwpSinkTask.class.getDeclaredField(name);
            field.setAccessible(true);
            return field.get(target);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private static Map<TopicPartition, OffsetAndMetadata> offsets(TopicPartition partition, long offset) {
        return Collections.singletonMap(partition, new OffsetAndMetadata(offset));
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

    private static SinkRecord recordWithTimestamp(long offset, long value) {
        return new SinkRecord(
                "renamed", 9, null, null, null, value, offset,
                123L, TimestampType.CREATE_TIME, Collections.emptyList(),
                SOURCE.topic(), SOURCE.partition(), offset);
    }

    private static SinkRecord tombstone(long offset) {
        return new SinkRecord(
                "renamed", 9, null, null, null, null, offset,
                null, TimestampType.NO_TIMESTAMP_TYPE, Collections.emptyList(),
                SOURCE.topic(), SOURCE.partition(), offset);
    }

    private static LineSenderServerException schemaMismatch(long fsn) {
        return serverFailure(SenderError.Category.SCHEMA_MISMATCH, fsn);
    }

    private static LineSenderServerException serverFailure(SenderError.Category category, long fsn) {
        return new LineSenderServerException(new SenderError(
                category,
                SenderError.Policy.TERMINAL,
                3,
                "rejected",
                1L,
                fsn,
                fsn,
                null,
                System.nanoTime()));
    }

    private static Stream<RuntimeException> terminalBuildFailures() {
        return Stream.of(
                new QwpAuthFailedException(401, "localhost", 9000),
                new QwpVersionMismatchException(2, 1),
                new LineSenderException(new QwpProtocolVersionException("malformed protocol")),
                new QwpDurableAckMismatchException("localhost", 9000, null),
                new WebSocketUpgradeException(426, null, "upgrade rejected"),
                new LineSenderException("invalid sender configuration")
        );
    }

    private static Stream<RuntimeException> transientBuildFailures() {
        return Stream.of(
                new HttpClientException("connection refused"),
                new LineSenderException(new HttpClientException("all endpoints unreachable")),
                new QwpRoleMismatchException("PRIMARY", null, "no writable primary"),
                new WebSocketUpgradeException(WebSocketUpgradeException.STATUS_NONE, null, "no response"),
                new WebSocketUpgradeException(502, null, "bad gateway"),
                new WebSocketUpgradeException(503, null, "service unavailable"),
                new WebSocketUpgradeException(421, "REPLICA", "misdirected request")
        );
    }

    private static TestTask startTask(FakeSender sender, int flushRows) {
        return startTask(sender, flushRows, Collections.emptyMap(), true);
    }

    private static TestTask startTask(FakeSender sender, int flushRows,
                                      Map<String, String> extra, boolean hasReporter) {
        return startTask(Collections.singletonList(sender), flushRows, extra, hasReporter);
    }

    private static TestTask startTask(List<FakeSender> senders, int flushRows,
                                      Map<String, String> extra, boolean hasReporter) {
        TestTask task = new TestTask(senders);
        task.fakeContext = new FakeContext(hasReporter);
        task.fakeContext.assignment.add(SOURCE);
        task.initialize(task.fakeContext);
        Map<String, String> props = new HashMap<>();
        props.put(QuestDBSinkConnectorConfig.CONFIGURATION_STRING_CONFIG,
                "ws::addr=localhost:9000;auto_flush_rows=" + flushRows + ";auto_flush_interval=60000;");
        props.put(QuestDBSinkConnectorConfig.TABLE_CONFIG, "table");
        props.put(QuestDBSinkConnectorConfig.INCLUDE_KEY_CONFIG, "false");
        if (hasReporter) {
            props.put("errors.tolerance", "all");
        }
        props.putAll(extra);
        task.start(props);
        task.open(Collections.singleton(SOURCE));
        return task;
    }

    private static final class TestTask extends QwpSinkTask {
        private final List<FakeSender> senders;
        private RuntimeException buildFailure;
        private int buildCalls;
        private FakeContext fakeContext;
        private long nowNanos;

        private TestTask(List<FakeSender> senders) {
            this.senders = senders;
        }

        @Override
        Sender buildSender(String confString) {
            buildCalls++;
            if (buildFailure != null) {
                throw buildFailure;
            }
            int index = buildCalls - 1;
            if (index >= senders.size()) {
                throw new AssertionError("Unexpected sender build " + buildCalls);
            }
            return senders.get(index).proxy();
        }

        @Override
        long nanoTime() {
            return nowNanos;
        }
    }

    private static final class FakeSender {
        private boolean ackOnFlush;
        private long ackedFsn = -1L;
        private RuntimeException autoFlushFailure;
        private boolean blockClose;
        private int cancelRows;
        private int closeCalls;
        private RuntimeException drainFailure;
        private boolean drainSucceeds = true;
        private final List<Long> drainTimeouts = new ArrayList<>();
        private int flushes;
        private RuntimeException flushFailure;
        private int flushFailureAt = -1;
        private final ArrayDeque<Long> flushResults = new ArrayDeque<>();
        private RuntimeException awaitFailure;
        private final List<Long> pendingValues = new ArrayList<>();
        private final Set<Long> rejectedValues = new HashSet<>();
        private RuntimeException rowFailure;
        private int rows;
        private RuntimeException setterFailure;
        private boolean zeroDrainSucceeds = true;
        private final List<Long> writtenValues = new ArrayList<>();
        private List<Long> lastFlushed = Collections.emptyList();
        private String lastCommitMethod;
        private Long currentValue;

        private Sender proxy() {
            return (Sender) Proxy.newProxyInstance(
                    Sender.class.getClassLoader(),
                    new Class<?>[]{Sender.class},
                    (proxy, method, args) -> {
                        switch (method.getName()) {
                            case "at":
                            case "atNow":
                                lastCommitMethod = method.getName();
                                if (rowFailure != null) {
                                    currentValue = null;
                                    throw rowFailure;
                                }
                                rows++;
                                if (currentValue != null) {
                                    writtenValues.add(currentValue);
                                    pendingValues.add(currentValue);
                                    currentValue = null;
                                }
                                if (autoFlushFailure != null) {
                                    throw autoFlushFailure;
                                }
                                return null;
                            case "longColumn":
                                if (setterFailure != null) {
                                    throw setterFailure;
                                }
                                currentValue = ((Number) args[1]).longValue();
                                return proxy;
                            case "flushAndGetSequence":
                                if (flushFailure != null && (flushFailureAt < 0 || flushes == flushFailureAt)) {
                                    throw flushFailure;
                                }
                                lastFlushed = new ArrayList<>(pendingValues);
                                pendingValues.clear();
                                long fsn = flushResults.isEmpty() ? flushes : flushResults.removeFirst();
                                flushes++;
                                if (ackOnFlush) {
                                    ackedFsn = Math.max(ackedFsn, fsn);
                                }
                                return fsn;
                            case "getAckedFsn":
                                return ackedFsn;
                            case "awaitAckedFsn":
                                if (awaitFailure != null) {
                                    throw awaitFailure;
                                }
                                return (long) args[0] <= ackedFsn;
                            case "drain":
                                long timeout = (Long) args[0];
                                drainTimeouts.add(timeout);
                                if (drainFailure != null) {
                                    throw drainFailure;
                                }
                                if (timeout == 0L && !zeroDrainSucceeds) {
                                    return false;
                                }
                                if (!drainSucceeds) {
                                    return false;
                                }
                                for (Long value : lastFlushed) {
                                    if (rejectedValues.contains(value)) {
                                        throw schemaMismatch(Math.max(0, flushes - 1L));
                                    }
                                }
                                ackedFsn = Math.max(ackedFsn, flushes - 1L);
                                return true;
                            case "cancelRow":
                                cancelRows++;
                                currentValue = null;
                                return null;
                            case "close":
                                closeCalls++;
                                if (blockClose) {
                                    new CountDownLatch(1).await();
                                }
                                return null;
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
        private final boolean hasReporter;
        private int pauseCalls;
        private final List<Long> reportedValues = new ArrayList<>();
        private int requestCommitCalls;
        private int resumeCalls;
        private final List<Map<TopicPartition, Long>> rewinds = new ArrayList<>();
        private final List<Long> timeouts = new ArrayList<>();

        private FakeContext(boolean hasReporter) {
            this.hasReporter = hasReporter;
        }

        @Override
        public Map<String, String> configs() {
            return Collections.emptyMap();
        }

        @Override
        public void offset(Map<TopicPartition, Long> offsets) {
            rewinds.add(new HashMap<>(offsets));
        }

        @Override
        public void offset(TopicPartition partition, long offset) {
            rewinds.add(Collections.singletonMap(partition, offset));
        }

        @Override
        public void timeout(long timeoutMs) {
            timeouts.add(timeoutMs);
        }

        @Override
        public Set<TopicPartition> assignment() {
            return assignment;
        }

        @Override
        public void pause(TopicPartition... partitions) {
            pauseCalls++;
        }

        @Override
        public void resume(TopicPartition... partitions) {
            resumeCalls++;
            for (TopicPartition partition : partitions) {
                if (!assignment.contains(partition)) {
                    throw new IllegalStateException("Cannot resume unassigned partition " + partition);
                }
            }
        }

        @Override
        public void requestCommit() {
            requestCommitCalls++;
        }

        @Override
        public ErrantRecordReporter errantRecordReporter() {
            if (!hasReporter) {
                return null;
            }
            return (record, error) -> {
                reportedValues.add(record.value() instanceof Number
                        ? ((Number) record.value()).longValue()
                        : -1L);
                return CompletableFuture.completedFuture(null);
            };
        }
    }
}
