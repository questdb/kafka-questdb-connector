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
import static org.junit.jupiter.api.Assertions.assertSame;
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
    void overlongAsciiColumnNameIsDlqdWithoutStoppingTheBatch() {
        assertOverlongColumnNameIsDlqd("x".repeat(127), "x".repeat(128));
    }

    @Test
    void overlongUtf8ColumnNameIsDlqdWithoutStoppingTheBatch() {
        assertOverlongColumnNameIsDlqd("x" + "é".repeat(63), "é".repeat(64));
    }

    @Test
    void overlongTopicDerivedTableNameIsDlqd() {
        FakeSender fakeSender = new FakeSender();
        Map<String, String> extra = Collections.singletonMap(
                QuestDBSinkConnectorConfig.TABLE_CONFIG, "${topic}");
        TestTask task = startTask(new TestTask(fakeSender), 1, extra);

        SinkRecord bad = recordOnTopic("t".repeat(128), 0L, 10L);
        assertDoesNotThrow(() -> task.put(Collections.singletonList(bad)));

        assertEquals(Collections.singletonList(10L), task.fakeContext.reportedValues);
        assertEquals(0, fakeSender.rows);
        assertEquals(0, fakeSender.cancelRows, "table validation runs before a row is started");
        Map<TopicPartition, OffsetAndMetadata> current = Collections.singletonMap(SOURCE, new OffsetAndMetadata(1L));
        assertEquals(1L, task.preCommit(current).get(SOURCE).offset());
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
    void unresolvedPublishedRowsAskConnectToPollBeforeTheCommitDeadline() {
        FakeSender sender = new FakeSender();
        sender.drainSucceeds = false;
        TestTask task = startTask(new TestTask(sender), 1, Collections.singletonMap(
                QuestDBSinkConnectorConfig.ALLOWED_LAG_CONFIG, "25"));

        task.put(Collections.singletonList(record(0L, 10L)));

        assertEquals(Collections.singletonList(25L), task.fakeContext.timeouts);
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

    /**
     * A typed server terminal can surface while a row is being built, because the client
     * latches an asynchronous rejection and rethrows it from the next call. The error then
     * belongs to an already published frame, not to the record in hand, so that record must
     * never be blamed on the strength of the timing alone - replay decides. Here the replay
     * succeeds, so nothing is reported.
     */
    @Test
    void typedTerminalDuringRowBuildingIsolatesRatherThanBlamingTheRecord() {
        FakeSender initial = new FakeSender();
        FakeSender recovery = new FakeSender();
        initial.rowFailure = schemaMismatch(0L);
        TestTask task = startTask(new TestTask(initial, recovery), 1, Collections.singletonMap(
                QuestDBSinkConnectorConfig.QWP_PROGRESS_TIMEOUT_MS_CONFIG, "1"));
        task.nowNanos = TimeUnit.MILLISECONDS.toNanos(2);

        assertDoesNotThrow(() -> task.put(Collections.singletonList(record(0L, 10L))));

        assertTrue(task.fakeContext.reportedValues.isEmpty(), "the record must not be blamed without evidence");
        assertEquals(1, recovery.rows, "it must be replayed to find out whether it is the offender");
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

    /**
     * The client publishes on its own cadence, so a rejected frame routinely belongs to no
     * flush entry the connector recorded - the ledger is empty between checkpoints. That
     * must still isolate the offending record rather than kill the task with an empty DLQ.
     */
    @Test
    void rejectionOfAFrameTheLedgerDoesNotCoverIsStillIsolated() {
        FakeSender initial = new FakeSender();
        FakeSender recovery = new FakeSender();
        // Flush threshold far above the batch, so the connector never checkpoints and holds
        // no flush entry at all - the state the client's own auto-flush leaves behind.
        TestTask task = startTask(new TestTask(initial, recovery), 1_000);
        task.put(java.util.List.of(record(0L, 10L), record(1L, 11L), record(2L, 12L)));

        initial.terminal = schemaMismatch(7L); // an FSN no recorded entry covers
        task.put(Collections.emptyList());

        assertEquals(3, recovery.rows, "every unacked record must be replayed");
        Map<TopicPartition, OffsetAndMetadata> current = Collections.singletonMap(SOURCE, new OffsetAndMetadata(3L));
        assertEquals(3L, task.preCommit(current).get(SOURCE).offset());
    }

    @Test
    void unattributableRejectionWithNothingLeftToReplayStillFailsTheTask() {
        FakeSender initial = new FakeSender();
        TestTask task = startTask(new TestTask(initial), 1);
        task.put(Collections.singletonList(record(0L, 10L)));
        task.preCommit(Collections.singletonMap(SOURCE, new OffsetAndMetadata(1L)));

        initial.terminal = schemaMismatch(7L);
        ConnectException failure = assertThrows(ConnectException.class, () -> task.put(Collections.emptyList()));
        assertTrue(failure.getMessage().contains("QuestDB rejected QWP frames"), failure.getMessage());
    }

    /**
     * Kafka's errant-record reporter completes its future on broker ack, so a DLQ'd record
     * stays pending for a while and the completed prefix cannot move past it. A record
     * replayed successfully behind that gap must keep its result when the sender is recreated
     * for a later rejection - otherwise nothing republishes it and the partition's offset is
     * pinned for good, with the task still reporting itself healthy.
     */
    @Test
    void replayedRecordsSurviveASenderResetTriggeredByALaterRejection() {
        FakeSender initial = new FakeSender();
        FakeSender first = new FakeSender();
        FakeSender second = new FakeSender();
        FakeSender third = new FakeSender();
        TestTask task = startTask(new TestTask(initial, first, second, third), 3);
        task.fakeContext.dlqFuturesPending = true;
        task.put(java.util.List.of(record(0L, 10L), record(1L, 11L), record(2L, 12L)));

        first.rejectedValue = 10L;   // first isolated replay is rejected
        second.rejectedValue = 12L;  // 11 replays cleanly, then 12 is rejected
        initial.terminal = schemaMismatch(0L);
        task.put(Collections.emptyList());

        assertEquals(java.util.List.of(10L, 12L), task.fakeContext.reportedValues);
        assertEquals(1, task.fakeContext.requestCommitCalls,
                "the replayed record should request a commit check without waiting for the retained head");
        Map<TopicPartition, OffsetAndMetadata> current = Collections.singletonMap(SOURCE, new OffsetAndMetadata(3L));
        assertEquals(0L, task.preCommit(current).get(SOURCE).offset(),
                "offsets stay withheld while the DLQ writes are still in flight");

        task.fakeContext.completeDlqFutures();
        task.put(Collections.emptyList());

        assertEquals(3L, task.preCommit(current).get(SOURCE).offset(),
                "every record is resolved, so the offset must advance");
    }

    @Test
    void acknowledgedRecordBehindPendingDlqIsNotReplayedAfterLedgerEntryIsPruned() {
        FakeSender initial = new FakeSender();
        initial.drainSucceeds = false;
        FakeSender rejectsTwelve = new FakeSender();
        rejectsTwelve.rejectedValue = 12L;
        FakeSender afterRejected = new FakeSender();
        TestTask task = startTask(new TestTask(initial, rejectsTwelve, afterRejected), 1);
        task.fakeContext.dlqFuturesPending = true;

        task.put(Collections.singletonList(recordWithValue(0L, new Object())));
        task.put(Collections.singletonList(record(1L, 11L)));
        initial.ackedFsn = 0L;
        task.put(Collections.emptyList());

        task.put(Collections.singletonList(record(2L, 12L)));
        initial.terminal = schemaMismatch(1L);
        task.put(Collections.emptyList());

        assertEquals(java.util.List.of(-1L, 12L), task.fakeContext.reportedValues);
        assertEquals(1, rejectsTwelve.rows, "only the unresolved record should enter recovery");
        assertEquals(0, afterRejected.rows, "the acknowledged record must remain settled after sender reset");

        Map<TopicPartition, OffsetAndMetadata> current = Collections.singletonMap(SOURCE, new OffsetAndMetadata(3L));
        assertEquals(0L, task.preCommit(current).get(SOURCE).offset());
        task.fakeContext.completeDlqFutures();
        task.put(Collections.emptyList());
        assertEquals(3L, task.preCommit(current).get(SOURCE).offset());
    }

    @Test
    void appliesAckWatermarkBeforeBuildingRecoveryPlan() {
        FakeSender initial = new FakeSender();
        initial.drainSucceeds = false;
        FakeSender recovery = new FakeSender();
        recovery.rejectedValue = 12L;
        FakeSender afterRejected = new FakeSender();
        TestTask task = startTask(new TestTask(initial, recovery, afterRejected), 1);

        task.put(Collections.singletonList(record(0L, 11L)));
        task.put(Collections.singletonList(record(1L, 12L)));
        initial.ackedFsn = 0L;
        initial.terminal = schemaMismatch(1L);
        task.put(Collections.emptyList());

        assertEquals(Collections.singletonList(12L), task.fakeContext.reportedValues);
        assertEquals(1, recovery.rows, "the sampled ACK must exclude the first record from recovery");
        assertEquals(0, afterRejected.rows);
    }

    @Test
    void successfulNoFsnDrainSettlesRecordBehindPendingDlq() {
        FakeSender initial = new FakeSender();
        initial.drainSucceeds = false;
        initial.returnNoFsn = true;
        FakeSender recovery = new FakeSender();
        recovery.rejectedValue = 12L;
        FakeSender afterRejected = new FakeSender();
        TestTask task = startTask(new TestTask(initial, recovery, afterRejected), 1);
        task.fakeContext.dlqFuturesPending = true;

        task.put(Collections.singletonList(recordWithValue(0L, new Object())));
        task.put(Collections.singletonList(record(1L, 11L)));
        initial.drainSucceeds = true;
        task.put(Collections.emptyList());

        initial.returnNoFsn = false;
        initial.drainSucceeds = false;
        task.put(Collections.singletonList(record(2L, 12L)));
        initial.terminal = schemaMismatch(2L);
        task.put(Collections.emptyList());

        assertEquals(java.util.List.of(-1L, 12L), task.fakeContext.reportedValues);
        assertEquals(1, recovery.rows);
        assertEquals(0, afterRejected.rows, "the no-FSN drain must make acknowledgement final");
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
    void batchDlqModeExcludesAcknowledgedAndAlreadyReportedRecords() {
        FakeSender initial = new FakeSender();
        initial.drainSucceeds = false;
        TestTask task = new TestTask(initial, new FakeSender());
        startTask(task, 1, Collections.singletonMap(
                QuestDBSinkConnectorConfig.DLQ_SEND_BATCH_ON_ERROR_CONFIG, "true"));
        task.fakeContext.dlqFuturesPending = true;

        task.put(Collections.singletonList(recordWithValue(0L, new Object())));
        task.put(Collections.singletonList(record(1L, 11L)));
        initial.ackedFsn = 0L;
        task.put(Collections.emptyList());
        task.put(Collections.singletonList(record(2L, 12L)));

        initial.terminal = schemaMismatch(1L);
        task.put(Collections.emptyList());

        assertEquals(java.util.List.of(-1L, 12L), task.fakeContext.reportedValues,
                "batch mode should report only unresolved QuestDB-bound records");
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
    void partialRevokeDuringPublishedNoFsnRecoveryDoesNotCreatePendingRows() {
        TopicPartition retainedPartition = new TopicPartition("source", 4);
        FakeSender initial = new FakeSender();
        FakeSender recovery = new FakeSender();
        recovery.returnNoFsn = true;
        recovery.drainSucceeds = false;
        TestTask task = startTask(new TestTask(initial, recovery), 1_000);
        task.open(Collections.singleton(retainedPartition));
        task.put(java.util.List.of(record(SOURCE, 0L, 10L), record(retainedPartition, 0L, 20L)));

        // No connector flush entry covers this rejection, so both records are replayed in one
        // step. The client publishes that step itself and leaves it draining without an FSN.
        initial.terminal = schemaMismatch(7L);
        task.put(Collections.emptyList());
        assertEquals(2, recovery.rows);

        task.close(Collections.singleton(SOURCE));
        recovery.drainSucceeds = true;
        assertDoesNotThrow(() -> task.put(Collections.emptyList()),
                "settling replay must not leave a normal-path flush count behind");

        Map<TopicPartition, OffsetAndMetadata> current =
                Collections.singletonMap(retainedPartition, new OffsetAndMetadata(1L));
        assertEquals(1L, task.preCommit(current).get(retainedPartition).offset());
    }

    @Test
    void partialRevokePreservesNormalPathPendingRows() {
        TopicPartition retainedPartition = new TopicPartition("source", 4);
        FakeSender sender = new FakeSender();
        sender.drainSucceeds = false;
        TestTask task = startTask(sender, 1_000);
        task.open(Collections.singleton(retainedPartition));
        task.put(java.util.List.of(record(SOURCE, 0L, 10L), record(retainedPartition, 0L, 20L)));

        task.close(Collections.singleton(SOURCE));
        task.put(Collections.emptyList());

        assertEquals(Collections.singletonList(20L), sender.flushedValues,
                "the surviving normal-path row must still trigger a flush");
        sender.ackedFsn = 0L;
        Map<TopicPartition, OffsetAndMetadata> current =
                Collections.singletonMap(retainedPartition, new OffsetAndMetadata(1L));
        assertEquals(1L, task.preCommit(current).get(retainedPartition).offset());
    }

    /**
     * Kafka's default assignor is eager, so every rebalance revokes the whole assignment.
     * A backpressure pause must not outlive that: Connect re-applies its own record of the
     * pause when the partitions come back, while the task's flag is cleared with the
     * assignment - leaving partitions paused with nothing able to resume them, no error, and
     * a task that still reports itself healthy.
     */
    @Test
    void backpressurePauseIsHandedBackOnlyForAssignedPartitions() {
        FakeSender sender = new FakeSender();
        TestTask task = new TestTask(sender);
        Map<String, String> extra = new HashMap<>();
        extra.put(QuestDBSinkConnectorConfig.QWP_MAX_INFLIGHT_ROWS_CONFIG, "1");
        startTask(task, 2, extra);

        task.put(java.util.List.of(record(0L, 10L), record(1L, 11L)));
        assertTrue(task.fakeContext.frameworkPaused.contains(SOURCE), "backpressure should pause the partition");

        TopicPartition previouslyRevoked = new TopicPartition("source", 4);
        assertDoesNotThrow(() -> task.close(java.util.List.of(SOURCE, previouslyRevoked)),
                "shutdown may pass stale offsets, but resume accepts only assigned partitions");

        assertTrue(task.fakeContext.frameworkPaused.isEmpty(),
                "a re-assignment must not inherit a pause that nothing can lift");
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
    void pendingDlqDoesNotLookLikeAQuestDbAcknowledgementStall() {
        FakeSender sender = new FakeSender();
        sender.drainSucceeds = false;
        Map<String, String> extra = new HashMap<>();
        extra.put(QuestDBSinkConnectorConfig.QWP_PROGRESS_TIMEOUT_MS_CONFIG, "1");
        TestTask task = startTask(new TestTask(sender), 1, extra);
        task.fakeContext.dlqFuturesPending = true;

        task.put(Collections.singletonList(recordWithValue(0L, new Object())));
        task.put(Collections.singletonList(record(1L, 11L)));
        sender.ackedFsn = 0L;
        task.nowNanos = TimeUnit.MILLISECONDS.toNanos(2);

        assertDoesNotThrow(() -> task.put(Collections.emptyList()));
        task.nowNanos = TimeUnit.MILLISECONDS.toNanos(4);
        assertDoesNotThrow(() -> task.put(Collections.emptyList()));
    }

    @Test
    void failedDlqFutureFailsTaskWithoutCommittingOffset() {
        TestTask task = startTask(new FakeSender(), 1);
        task.fakeContext.dlqFuturesPending = true;
        task.put(Collections.singletonList(recordWithValue(0L, new Object())));
        task.fakeContext.failDlqFutures();

        Map<TopicPartition, OffsetAndMetadata> current = Collections.singletonMap(SOURCE, new OffsetAndMetadata(1L));
        assertTrue(task.preCommit(current).isEmpty());
        ConnectException failure = assertThrows(ConnectException.class, () -> task.put(Collections.emptyList()));
        assertTrue(failure.getMessage().contains("Failed to deliver a QWP record to the DLQ"));
    }

    @Test
    void putPreservesServerFailureWhenDlqCompletionAlsoFails() {
        FakeSender sender = new FakeSender();
        TestTask task = startTask(sender, 1);
        task.fakeContext.dlqFuturesPending = true;
        task.put(Collections.singletonList(recordWithValue(0L, new Object())));

        LineSenderServerException serverFailure = schemaMismatch(0L);
        sender.terminal = serverFailure;
        sender.beforeTerminal = task.fakeContext::failDlqFutures;

        ConnectException failure = assertThrows(ConnectException.class, () -> task.put(Collections.emptyList()));
        assertTrue(failure.getMessage().contains("Failed to deliver a QWP record to the DLQ"));
        assertEquals(1, failure.getSuppressed().length);
        assertSame(serverFailure, failure.getSuppressed()[0]);
    }

    @Test
    void preCommitPreservesServerFailureWhenDlqCompletionAlsoFails() {
        FakeSender sender = new FakeSender();
        TestTask task = startTask(sender, 1);
        task.fakeContext.dlqFuturesPending = true;
        task.put(java.util.List.of(recordWithValue(0L, new Object()), record(1L, 11L)));

        sender.rejectedValue = 11L;
        sender.beforeTerminal = task.fakeContext::failDlqFutures;

        Map<TopicPartition, OffsetAndMetadata> current = Collections.singletonMap(SOURCE, new OffsetAndMetadata(2L));
        ConnectException failure = assertThrows(ConnectException.class, () -> task.preCommit(current));
        assertTrue(failure.getMessage().contains("Failed to deliver a QWP record to the DLQ"));
        assertEquals(1, failure.getSuppressed().length);
        assertSame(sender.terminal, failure.getSuppressed()[0]);
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

    /**
     * Isolation runs one slice per put() with the partitions paused, so Connect's next poll is
     * the only thing that resumes it - and that poll lasts until the offset-commit deadline
     * (offset.flush.interval.ms, 60s by default) unless the task asks for an earlier callback.
     * Without the request, a replay that outlives its first slice advances once a minute.
     */
    @Test
    void unfinishedIsolationAsksConnectToPollBeforeTheCommitDeadline() {
        FakeSender initial = new FakeSender();
        FakeSender replay = new FakeSender();
        replay.drainSucceeds = false; // no slice can finish the replay
        TestTask task = startTask(new TestTask(initial, replay), 1, Collections.singletonMap(
                QuestDBSinkConnectorConfig.QWP_ISOLATION_SLICE_MS_CONFIG, "25"));
        task.put(Collections.singletonList(record(0L, 10L)));
        task.fakeContext.timeouts.clear();
        initial.terminal = schemaMismatch(0L);
        assertTrue(task.fakeContext.timeouts.isEmpty());

        task.put(Collections.emptyList());
        assertEquals(Collections.singletonList(25L), task.fakeContext.timeouts);

        // Connect consumes the request on every poll, so each slice has to re-arm it.
        task.put(Collections.emptyList());
        assertEquals(java.util.List.of(25L, 25L), task.fakeContext.timeouts);
    }

    /**
     * A commit cycle runs between isolation slices and must leave a running replay alone. The
     * replay owns the sender, so probing it here surfaces the rejection the next slice is about
     * to handle - and answering it by rebuilding the plan discards the bisection and re-publishes
     * rows that already settled. Only two senders are available here, so a restart shows up as
     * an unexpected sender recreation.
     */
    @Test
    void aCommitDoesNotRestartIsolationThatIsAlreadyRunning() {
        FakeSender initial = new FakeSender();
        FakeSender replay = new FakeSender();
        replay.drainSucceeds = false;
        TestTask task = startTask(new TestTask(initial, replay), 1);
        task.put(Collections.singletonList(record(0L, 10L)));
        initial.terminal = schemaMismatch(0L);
        task.put(Collections.emptyList());

        replay.terminal = schemaMismatch(0L); // the replay's own rejection, not yet drained
        Map<TopicPartition, OffsetAndMetadata> current = Collections.singletonMap(SOURCE, new OffsetAndMetadata(1L));
        assertEquals(0L, task.preCommit(current).get(SOURCE).offset());
        assertEquals(1, task.fakeContext.pauseCalls, "the running isolation must not be restarted");
    }

    /**
     * Connect holds a batch that put() refused and keeps every partition paused until some
     * put() returns normally. Isolation lifts that pause itself when it ends, so the batch in
     * hand has to be accepted in the very same call - refusing it leaves Connect owning an
     * undelivered batch while the consumer fetches again, which breaks its
     * `messageBatch.isEmpty() || msgs.isEmpty()` invariant and kills the task.
     */
    @Test
    void theBatchInHandIsAcceptedAsSoonAsIsolationEnds() {
        FakeSender initial = new FakeSender();
        FakeSender replay = new FakeSender();
        replay.drainSucceeds = false;
        TestTask task = startTask(new TestTask(initial, replay), 1);
        task.put(Collections.singletonList(record(0L, 10L)));
        initial.terminal = schemaMismatch(0L);

        task.put(Collections.emptyList());
        assertEquals(1, task.fakeContext.pauseCalls);
        assertThrows(RetriableException.class, () -> task.put(Collections.singletonList(record(1L, 11L))),
                "while isolation owns the sender the batch has to go back to Connect");

        replay.drainSucceeds = true; // the replayed row is acked, so the next slice finishes
        assertDoesNotThrow(() -> task.put(Collections.singletonList(record(1L, 11L))));
        assertTrue(replay.flushedValues.contains(11L), "the batch must be written, not refused again");
        assertEquals(1, task.fakeContext.resumeCalls);
    }

    /**
     * Bisecting a rejected batch is forward progress: the server answered and the search space
     * halved. Counting only settlements would let progress.timeout.ms kill a task that is doing
     * exactly what isolation asks of it.
     */
    @Test
    void bisectingARejectedBatchCountsAsProgress() {
        FakeSender initial = new FakeSender();
        FakeSender rejectsBatch = new FakeSender();
        rejectsBatch.rejectedValue = 11L;
        FakeSender bisected = new FakeSender();
        bisected.drainSucceeds = false; // the halves are still in flight when the slice ends
        Map<String, String> extra = new HashMap<>();
        extra.put(QuestDBSinkConnectorConfig.QWP_PROGRESS_TIMEOUT_MS_CONFIG, "10");
        TestTask task = startTask(new TestTask(initial, rejectsBatch, bisected), 2, extra);
        task.put(java.util.List.of(record(0L, 10L), record(1L, 11L)));

        // The rejected frame belongs to no entry the task recorded, so the whole window is
        // replayed as one batch - and that batch is rejected again, which forces a bisection.
        task.nowNanos = TimeUnit.MILLISECONDS.toNanos(8);
        initial.terminal = schemaMismatch(5L);
        task.put(Collections.emptyList());

        // 7ms after the split, well inside the 10ms budget - but 15ms after the last settlement.
        task.nowNanos = TimeUnit.MILLISECONDS.toNanos(15);
        assertDoesNotThrow(() -> task.put(Collections.emptyList()));
        assertThrows(RetriableException.class, () -> task.put(Collections.singletonList(record(2L, 12L))),
                "the bisection is still unfinished, so the split was the only progress on record");
        assertTrue(task.fakeContext.reportedValues.isEmpty(), "nothing is blamed until a half is isolated");
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

    private static void assertOverlongColumnNameIsDlqd(String acceptedName, String rejectedName) {
        FakeSender fakeSender = new FakeSender();
        TestTask task = startTask(fakeSender, 2);

        assertDoesNotThrow(() -> task.put(java.util.List.of(
                recordWithValue(0L, Collections.singletonMap(acceptedName, 10L)),
                recordWithValue(1L, Collections.singletonMap(rejectedName, 11L)),
                record(2L, 12L)
        )));

        assertEquals(Collections.singletonList(-1L), task.fakeContext.reportedValues);
        assertEquals(2, fakeSender.rows, "records around the invalid one must still be written");
        assertEquals(1, fakeSender.cancelRows, "the partially constructed invalid row must be cancelled");
        Map<TopicPartition, OffsetAndMetadata> current = Collections.singletonMap(SOURCE, new OffsetAndMetadata(3L));
        assertEquals(3L, task.preCommit(current).get(SOURCE).offset());
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

    private static SinkRecord recordOnTopic(String topic, long offset, long value) {
        return new SinkRecord(
                topic, 9, null, null, null, value, offset,
                null, TimestampType.NO_TIMESTAMP_TYPE, Collections.emptyList(),
                SOURCE.topic(), SOURCE.partition(), offset);
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
        private int cancelRows;
        private LineSenderServerException terminal;
        private Long rejectedValue;
        private Long currentValue;
        private boolean drainSucceeds = true;
        private boolean returnNoFsn;
        private RuntimeException rowFailure;
        private Runnable beforeTerminal;
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
                                long published = flushes++;
                                return returnNoFsn ? -1L : published;
                            case "getAckedFsn":
                                return ackedFsn;
                            case "awaitAckedFsn":
                                if (terminal != null) {
                                    if (beforeTerminal != null) {
                                        beforeTerminal.run();
                                    }
                                    throw terminal;
                                }
                                return (long) args[0] <= ackedFsn;
                            case "drain":
                                if (!drainSucceeds) {
                                    return false;
                                }
                                if (rejectedValue != null && flushedValues.contains(rejectedValue)) {
                                    terminal = schemaMismatch(Math.max(0, flushes - 1L));
                                    if (beforeTerminal != null) {
                                        beforeTerminal.run();
                                    }
                                    throw terminal;
                                }
                                ackedFsn = Math.max(ackedFsn, flushes - 1L);
                                return true;
                            case "cancelRow":
                                cancelRows++;
                                currentValue = null;
                                return null;
                            case "close":
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

        private final java.util.List<Long> timeouts = new ArrayList<>();

        @Override
        public void timeout(long timeoutMs) {
            timeouts.add(timeoutMs);
        }

        @Override
        public Set<TopicPartition> assignment() {
            return assignment;
        }

        /**
         * Kafka Connect keeps its own record of the partitions a task asked to pause, and
         * re-applies it whenever those partitions are assigned again - the set outlives a
         * revocation. Model that here, otherwise a pause the task forgets to hand back looks
         * harmless in tests and strands the partitions in production.
         */
        private final Set<TopicPartition> frameworkPaused = new HashSet<>();

        @Override
        public void pause(TopicPartition... partitions) {
            pauseCalls++;
            Collections.addAll(assignment, partitions);
            Collections.addAll(frameworkPaused, partitions);
        }

        @Override
        public void resume(TopicPartition... partitions) {
            resumeCalls++;
            for (TopicPartition partition : partitions) {
                if (!assignment.contains(partition)) {
                    throw new IllegalStateException("Cannot resume unassigned partition " + partition);
                }
                frameworkPaused.remove(partition);
            }
        }

        @Override
        public void requestCommit() {
            requestCommitCalls++;
        }

        /**
         * Kafka's own reporter hands back the DLQ producer's future, which completes on broker
         * ack rather than immediately. Set this to model that, and complete the futures when
         * the test wants the broker to catch up.
         */
        private boolean dlqFuturesPending;
        private final java.util.List<CompletableFuture<Void>> issuedDlqFutures = new ArrayList<>();

        private void completeDlqFutures() {
            for (CompletableFuture<Void> future : issuedDlqFutures) {
                future.complete(null);
            }
        }

        private void failDlqFutures() {
            for (CompletableFuture<Void> future : issuedDlqFutures) {
                future.completeExceptionally(new RuntimeException("broker rejected DLQ write"));
            }
        }

        @Override
        public ErrantRecordReporter errantRecordReporter() {
            return (record, error) -> {
                reportedValues.add(record.value() instanceof Number ? ((Number) record.value()).longValue() : -1L);
                CompletableFuture<Void> future = new CompletableFuture<>();
                if (dlqFuturesPending) {
                    issuedDlqFutures.add(future);
                } else {
                    future.complete(null);
                }
                return future;
            };
        }
    }
}
