package io.questdb.kafka;

import io.questdb.client.LineSenderServerException;
import io.questdb.client.Sender;
import io.questdb.client.SenderError;
import io.questdb.client.cutlass.http.client.HttpClientException;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.std.str.StringSink;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

class QwpSinkTask extends SinkTask {
    private static final int COMPACTION_THRESHOLD = 1024;
    private static final Logger log = LoggerFactory.getLogger(QwpSinkTask.class);

    private final List<FlushEntry> flushEntries = new ArrayList<>();
    private final List<PendingRecord> retained = new ArrayList<>();
    // Settled records can be out of order across QuestDB and the DLQ. `head` only
    // finds the reclaimable retained prefix; acknowledgements are copied to records
    // through their FlushEntry before that entry is removed.
    private int head;
    // Records whose completion is a DLQ delivery rather than an ack; those can
    // land out of order, so they are tracked separately. Normally empty.
    private final List<PendingRecord> dlqPending = new ArrayList<>();
    private final List<PendingRecord> publishBuffer = new ArrayList<>();
    private final Map<String, Map<Integer, TopicPartition>> partitionCache = new HashMap<>();
    private final Set<TopicPartition> assignment = new HashSet<>();
    private final FlushConfig flushConfig = new FlushConfig();

    private QuestDBSinkConnectorConfig config;
    private Sender sender;
    private RecordToRowHandler recordHandler;
    private ErrantRecordReporter reporter;
    private EnumSet<SenderError.Category> dlqEligibleCategories;
    private String patchedConfString;
    private Throwable deferredFailure;
    private long lastAckedFsn = -1L;
    private long lastPublishedFsn = -1L;
    private long lastProgressNanos;
    private long nextFlushNanos;
    private int pendingRows;
    private boolean partitionsPaused;
    private Recovery recovery;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting QuestDB QWP sink task [version={}, commit={}]", VersionUtil.getVersion(), VersionUtil.getGitHash());
        config = new QuestDBSinkConnectorConfig(props);
        String confString = ClientConfUtils.resolveConfString(config);
        if (confString == null || !ClientConfUtils.isQwp(confString)) {
            throw new ConfigException("QWP task requires a ws:: or wss:: client configuration string");
        }
        StringSink patched = new StringSink();
        ClientConfUtils.patchConfStr(confString, patched, flushConfig);
        patchedConfString = patched.toString();
        validatePollInterval(props, flushConfig.sfAppendDeadlineMillis);
        dlqEligibleCategories = parseDlqEligibleCategories(config.getQwpDlqTerminalCategories());
        sender = createSender();
        recordHandler = new RecordToRowHandler(
                config,
                sender,
                true,
                false,
                true,
                RecordToRowHandler.NameLimits.QWP
        );
        nextFlushNanos = nanoTime() + flushConfig.autoFlushNanos;
        lastProgressNanos = nanoTime();
        try {
            reporter = context.errantRecordReporter();
        } catch (NoSuchMethodError | NoClassDefFoundError e) {
            reporter = null;
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        throwDeferredFailure();
        if (recovery != null) {
            processRecoverySlice();
            if (recovery != null) {
                if (!records.isEmpty()) {
                    throw new RetriableException("QWP replay isolation is in progress");
                }
                return;
            }
            // Isolation finished inside that slice, so this batch is handled below rather than
            // refused. Connect holds a refused batch and keeps every partition paused until a
            // put() returns normally; isolation has just resumed those partitions itself, so
            // refusing here lets the next poll fetch records while Connect still owns an
            // undelivered batch. That breaks its `messageBatch.isEmpty() || msgs.isEmpty()`
            // invariant: with assertions on - which is every surefire run - the AssertionError
            // kills the task, and with them off convertMessages() appends the new records to
            // the batch still in hand and delivers the two merged.
        }
        boolean batchAdmitted = false;
        try {
            refreshSenderState();
            detectStall();
            if (records.isEmpty()) {
                // An empty poll signals quiescence: publish buffered rows now instead of
                // waiting out the flush timer, so low-volume latency stays close to the
                // legacy HTTP path. Publishing is an async write, so this is cheap.
                publishPendingRows();
                applyBackpressure();
                requestNextSenderCheck();
                return;
            }

            boolean hadPendingRecords = hasServerPending();
            int retainedStart = retained.size();
            for (SinkRecord record : records) {
                if (record.value() != null) {
                    retained.add(new PendingRecord(record, partitionFor(record)));
                }
            }
            batchAdmitted = true;
            if (!hadPendingRecords && retained.size() > retainedStart) {
                // This is also the recovery progress epoch if a latched terminal surfaces while
                // the first row is being built, before it can reach WRITTEN_NO_FSN.
                lastProgressNanos = nanoTime();
            }

            int retainedIndex = retainedStart;
            for (SinkRecord record : records) {
                if (record.value() == null) {
                    continue;
                }
                PendingRecord pending = retained.get(retainedIndex++);
                try {
                    if (recordHandler.handle(record)) {
                        pending.markWritten();
                        pendingRows++;
                    }
                } catch (InvalidDataException e) {
                    if (reporter == null) {
                        throw e;
                    }
                    reportToDlq(pending, e);
                }
            }
            requestNextSenderCheck();
            flushIfDue();
            applyBackpressure();
        } catch (LineSenderServerException e) {
            updateCompletionsAfterServerFailure(e);
            if (beginRecovery(e)) {
                processRecoverySlice();
                if (!batchAdmitted && !records.isEmpty()) {
                    throw new RetriableException("QWP replay isolation started before the incoming batch was admitted", e);
                }
                return;
            }
            throw terminalFailure(e);
        } catch (LineSenderException | HttpClientException e) {
            throw new ConnectException("QWP sender failed", e);
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        if (deferredFailure != null) {
            return Collections.emptyMap();
        }
        try {
            if (recovery == null) {
                // Probing only makes sense while the sender is ours. A replay owns it, and its
                // rejections belong to the slice that provoked them: surfacing one here instead
                // would rebuild the plan from scratch, throwing away a bisection in progress and
                // making records that already settled eligible for a second write.
                refreshSenderState();
                // A commit cycle may be the last thing before a rebalance close(),
                // whose drain cannot influence the commit (Connect uses this method's
                // return value after close() runs). Publish buffered rows now and give
                // acks a short bounded window so a clean rebalance commits instead of
                // redelivering the whole window. A timeout just withholds offsets.
                publishPendingRows();
                if (hasServerPending()) {
                    // drain uses watermark semantics: flush, then wait (bounded)
                    // until everything published so far - including frames the
                    // client auto-flushed on its own - is acked.
                    sender.drain(config.getQwpCommitAckTimeoutMs());
                }
            }
            updateCompletions();
            detectStall();
        } catch (LineSenderServerException e) {
            updateCompletionsAfterServerFailure(e);
            if (!beginRecovery(e)) {
                deferredFailure = terminalFailure(e);
            }
            // The commit that follows resets Connect's poll deadline, so the first slice would
            // otherwise wait out a full offset.flush.interval.ms before put() ever runs.
            requestNextRecoverySlice();
            return Collections.emptyMap();
        } catch (Throwable e) {
            deferredFailure = e;
            return Collections.emptyMap();
        }

        // Offsets grow within a partition, so the first incomplete record seen
        // walking from `head` is that partition's earliest incomplete offset.
        Map<TopicPartition, Long> earliestIncomplete = new HashMap<>();
        for (int i = head, n = retained.size(); i < n; i++) {
            PendingRecord pending = retained.get(i);
            if (pending.isSettled()) {
                continue;
            }
            earliestIncomplete.putIfAbsent(pending.partition, pending.offset);
            if (!assignment.isEmpty() && earliestIncomplete.size() == assignment.size()) {
                break;
            }
        }

        Map<TopicPartition, OffsetAndMetadata> clamped = new LinkedHashMap<>(currentOffsets.size());
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : currentOffsets.entrySet()) {
            OffsetAndMetadata current = entry.getValue();
            Long holdAt = earliestIncomplete.get(entry.getKey());
            if (holdAt != null && current.offset() > holdAt) {
                clamped.put(entry.getKey(), new OffsetAndMetadata(holdAt, current.metadata()));
            } else {
                clamped.put(entry.getKey(), current);
            }
        }
        return clamped;
    }

    @Override
    public void open(Collection<TopicPartition> partitions) {
        assignment.addAll(partitions);
        if (partitionsPaused && !partitions.isEmpty()) {
            context.pause(partitions.toArray(new TopicPartition[0]));
        }
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        // No drain here on purpose. Connect calls preCommit() before close(), so nothing we
        // learn now can change the offsets that were just committed, and rows already handed
        // to the client are on their way to the server whether or not we wait for the acks.
        // Waiting only holds up the rebalance for every other member of the group.
        if (partitions.isEmpty()) {
            return;
        }
        Set<TopicPartition> revoked = new HashSet<>(partitions);
        if (partitionsPaused) {
            Set<TopicPartition> resumable = new HashSet<>(revoked);
            resumable.retainAll(assignment);
            // Connect records the pause we requested and re-applies it when these partitions
            // are assigned again, while our own flag only describes the current assignment.
            // Hand the pause back before letting go, so a re-assignment starts unpaused;
            // open() re-pauses whatever comes back if we are still holding partitions.
            // Otherwise a revocation that empties the assignment clears our flag while
            // Connect keeps the pause, and nothing is left that can ever resume it.
            if (!resumable.isEmpty()) {
                context.resume(resumable.toArray(new TopicPartition[0]));
            }
        }
        assignment.removeAll(revoked);
        retained.subList(head, retained.size()).removeIf(pending -> revoked.contains(pending.partition));
        dlqPending.removeIf(pending -> revoked.contains(pending.partition));
        for (FlushEntry flushEntry : flushEntries) {
            flushEntry.records.removeIf(pending -> revoked.contains(pending.partition));
        }
        flushEntries.removeIf(entry -> entry.records.isEmpty());
        if (recovery != null) {
            recovery.removeRevoked(revoked);
        }
        // RecoveryStep owns publication state while replay isolation owns the sender.
        // resetSenderForRecovery() cleared this normal-path counter, and replay must not
        // repopulate it merely because an FSN-less published step is still draining.
        pendingRows = recovery == null ? countUnflushedRows() : 0;
        if (assignment.isEmpty()) {
            partitionsPaused = false;
        }
    }

    @Override
    public void stop() {
        // Sender.close() flushes whatever is still buffered and then drains, bounded by the
        // client's close_flush_timeout_millis. Draining separately first would simply serialise
        // two waits for the same acks.
        closeSenderSilently();
    }

    Sender buildSender(String confString) {
        return Sender.builder(confString)
                .errorHandler(this::onSenderError)
                .build();
    }

    private Sender createSender() {
        // No BufferingSender here: that class buffers a whole row purely to satisfy
        // ILP's "symbols before other columns" rule, which QWP does not have.
        // RecordToRowHandler writes symbol columns straight to the sender instead.
        return buildSender(patchedConfString);
    }

    private void flushIfDue() {
        if (pendingRows == 0) {
            return;
        }
        if (pendingRows >= flushConfig.autoFlushRows) {
            publishPendingRows();
            return;
        }
        long remainingNanos = nextFlushNanos - nanoTime();
        long remainingMs = TimeUnit.NANOSECONDS.toMillis(remainingNanos);
        if (remainingMs <= 0 || config.getAllowedLag() == 0) {
            publishPendingRows();
        } else {
            context.timeout(Math.min(remainingMs, config.getAllowedLag()));
        }
    }

    private void publishPendingRows() {
        if (pendingRows == 0) {
            return;
        }
        List<PendingRecord> records = publishBuffer;
        records.clear();
        for (int i = head, n = retained.size(); i < n; i++) {
            PendingRecord pending = retained.get(i);
            if (pending.isWrittenWithoutFsn()) {
                records.add(pending);
            }
        }
        long fsn = sender.flushAndGetSequence();
        if (records.isEmpty()) {
            throw new ConnectException("QWP flush published rows the ledger does not track");
        }
        if (fsn < 0L) {
            // The client auto-flushed every buffered row before this checkpoint,
            // so this call published nothing and the rows' real FSNs are unknown.
            // drain(0) is a nonblocking "everything published is acked" probe:
            // when it reports true the rows are durably acked and complete at
            // the current acked watermark; until then they stay pending and a
            // later checkpoint (or preCommit's bounded drain) settles them.
            if (!sender.drain(0L)) {
                return;
            }
            long acked = Math.max(sender.getAckedFsn(), lastAckedFsn);
            if (acked > lastAckedFsn) {
                lastAckedFsn = acked;
                lastProgressNanos = nanoTime();
            }
            boolean completedRecord = false;
            for (PendingRecord pending : records) {
                completedRecord |= pending.acknowledgeByQuestDb();
            }
            pendingRows = 0;
            nextFlushNanos = nanoTime() + flushConfig.autoFlushNanos;
            finishCompletionUpdate(completedRecord);
            return;
        }
        long fromFsn = lastPublishedFsn + 1L;
        for (PendingRecord pending : records) {
            pending.waitForQuestDbAck();
        }
        flushEntries.add(new FlushEntry(fromFsn, fsn, new ArrayList<>(records)));
        lastPublishedFsn = fsn;
        pendingRows = 0;
        nextFlushNanos = nanoTime() + flushConfig.autoFlushNanos;
    }

    private void refreshSenderState() {
        updateCompletions();
        sender.awaitAckedFsn(lastAckedFsn, 0L);
    }

    private void updateCompletions() {
        updateCompletions(false);
    }

    private void updateCompletionsAfterServerFailure(LineSenderServerException serverFailure) {
        try {
            updateCompletions();
        } catch (RuntimeException completionFailure) {
            if (completionFailure != serverFailure) {
                completionFailure.addSuppressed(serverFailure);
            }
            throw completionFailure;
        }
    }

    private void updateCompletions(boolean completedRecord) {
        long acked = Math.max(sender.getAckedFsn(), lastAckedFsn);
        if (acked > lastAckedFsn) {
            lastAckedFsn = acked;
            lastProgressNanos = nanoTime();
        }

        int acknowledgedEntries = 0;
        for (int i = 0, n = flushEntries.size(); i < n; i++) {
            FlushEntry entry = flushEntries.get(i);
            if (entry.toFsn > acked) {
                break;
            }
            for (PendingRecord pending : entry.records) {
                completedRecord |= pending.acknowledgeByQuestDb();
            }
            acknowledgedEntries++;
        }
        if (acknowledgedEntries > 0) {
            flushEntries.subList(0, acknowledgedEntries).clear();
        }

        for (int i = 0; i < dlqPending.size(); i++) {
            PendingRecord pending = dlqPending.get(i);
            Future<Void> future = pending.dlqFuture();
            if (future.isDone()) {
                completeDlqFuture(future);
                pending.acknowledgeByDlq();
                completedRecord = true;
                dlqPending.remove(i--);
            }
        }
        finishCompletionUpdate(completedRecord);
    }

    private void finishCompletionUpdate(boolean completedRecord) {
        int headBefore = head;
        while (head < retained.size() && retained.get(head).isSettled()) {
            head++;
        }
        // Records settled elsewhere (replay, DLQ) are already complete when the prefix reaches
        // them, so an advancing prefix is itself commit-worthy news even if nothing completed here.
        completedRecord |= head != headBefore;
        if (head == retained.size() || head >= COMPACTION_THRESHOLD) {
            retained.subList(0, head).clear();
            head = 0;
        }
        if (completedRecord) {
            context.requestCommit();
        }
        if (recovery == null && partitionsPaused && pendingCount() < config.getQwpMaxInflightRows()) {
            resumeAssignedPartitions();
        }
    }

    private static void completeDlqFuture(Future<Void> future) {
        try {
            future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConnectException("Interrupted while checking QWP DLQ delivery", e);
        } catch (ExecutionException e) {
            throw new ConnectException("Failed to deliver a QWP record to the DLQ", e.getCause());
        }
    }

    private void detectStall() {
        if (!hasServerPending()) {
            return;
        }
        long stalledNanos = nanoTime() - lastProgressNanos;
        if (stalledNanos >= TimeUnit.MILLISECONDS.toNanos(config.getQwpProgressTimeoutMs())) {
            throw new ConnectException("QWP acknowledgements did not advance for " + config.getQwpProgressTimeoutMs() + " ms");
        }
    }

    private boolean hasServerPending() {
        for (int i = head, n = retained.size(); i < n; i++) {
            PendingRecord pending = retained.get(i);
            if (pending.isWaitingForQuestDbProgress()) {
                return true;
            }
        }
        return false;
    }

    private void applyBackpressure() {
        if (!partitionsPaused && pendingCount() > config.getQwpMaxInflightRows() && !assignment.isEmpty()) {
            context.pause(assignment.toArray(new TopicPartition[0]));
            context.timeout(Math.min(config.getAllowedLag(), config.getQwpProgressTimeoutMs()));
            partitionsPaused = true;
        }
    }

    private void resumeAssignedPartitions() {
        if (!assignment.isEmpty()) {
            context.resume(assignment.toArray(new TopicPartition[0]));
        }
        partitionsPaused = false;
    }

    private int pendingCount() {
        return retained.size() - head;
    }

    /** Interned so a record does not allocate a TopicPartition per row. */
    private TopicPartition partitionFor(SinkRecord record) {
        String topic = record.originalTopic();
        Integer partition = record.originalKafkaPartition();
        if (topic == null || partition == null) {
            throw new ConnectException("Kafka Connect did not provide original coordinates for a QWP record");
        }
        // Deliberately not computeIfAbsent: its mapping function captures `topic`,
        // so the lambda is allocated on every record even when the cache hits.
        Map<Integer, TopicPartition> byPartition = partitionCache.get(topic);
        if (byPartition == null) {
            byPartition = new HashMap<>();
            partitionCache.put(topic, byPartition);
        }
        TopicPartition cached = byPartition.get(partition);
        if (cached == null) {
            cached = new TopicPartition(topic, partition);
            byPartition.put(partition, cached);
        }
        return cached;
    }

    private void reportToDlq(PendingRecord pending, Throwable error) {
        Future<Void> future = reporter.report(pending.record, error);
        pending.sendToDlq(future);
        dlqPending.add(pending);
    }

    private int countUnflushedRows() {
        int count = 0;
        for (int i = head, n = retained.size(); i < n; i++) {
            PendingRecord pending = retained.get(i);
            if (pending.isWrittenWithoutFsn()) {
                count++;
            }
        }
        return count;
    }

    private ConnectException terminalFailure(LineSenderServerException e) {
        SenderError error = e.getServerError();
        String message = "QuestDB rejected QWP frames [category=" + error.getCategory()
                + ", fsn=" + error.getFromFsn() + "-" + error.getToFsn() + ']';
        return new ConnectException(message, e);
    }

    private boolean beginRecovery(LineSenderServerException exception) {
        SenderError error = exception.getServerError();
        if (!dlqEligibleCategories.contains(error.getCategory()) || reporter == null) {
            return false;
        }

        if (config.isDlqSendBatchOnError()) {
            for (int i = head, n = retained.size(); i < n; i++) {
                PendingRecord pending = retained.get(i);
                if (pending.needsQuestDbDelivery()) {
                    reportToDlq(pending, exception);
                }
            }
            resetSenderForRecovery();
            resumeAfterRecovery();
            return true;
        }

        // The client publishes on its own cadence (auto_flush_rows/interval), which is far
        // tighter than our checkpoint, so a rejected frame frequently belongs to no entry we
        // recorded - the ledger is empty between checkpoints, and emptied again every time
        // acks prune it. A null suspect therefore means "we cannot name the frame", not
        // "this cannot be isolated": the plan below already replays every unacked record,
        // and a batch step that is rejected again is split into per-record steps. Leaving
        // suspect null simply starts that narrowing one round earlier instead of failing
        // the task with an empty DLQ.
        FlushEntry suspect = null;
        for (FlushEntry entry : flushEntries) {
            if (entry.fromFsn <= error.getToFsn() && error.getFromFsn() <= entry.toFsn) {
                suspect = entry;
                break;
            }
        }

        List<RecoveryStep> steps = new ArrayList<>();
        Set<PendingRecord> scheduled = Collections.newSetFromMap(new IdentityHashMap<>());
        for (FlushEntry entry : flushEntries) {
            List<PendingRecord> eligible = new ArrayList<>(entry.records.size());
            for (PendingRecord pending : entry.records) {
                if (pending.needsQuestDbDelivery()) {
                    eligible.add(pending);
                    scheduled.add(pending);
                }
            }
            if (entry == suspect) {
                for (PendingRecord pending : eligible) {
                    steps.add(new RecoveryStep(Collections.singletonList(pending), true));
                }
            } else if (!eligible.isEmpty()) {
                steps.add(new RecoveryStep(eligible, false));
            }
        }
        List<PendingRecord> tail = new ArrayList<>();
        for (int i = head, n = retained.size(); i < n; i++) {
            PendingRecord pending = retained.get(i);
            if (pending.needsQuestDbDelivery() && !scheduled.contains(pending)) {
                tail.add(pending);
            }
        }
        if (!tail.isEmpty()) {
            steps.add(new RecoveryStep(tail, false));
        }
        if (steps.isEmpty()) {
            // Nothing left to replay: the rejection cannot be attributed to any record we
            // still hold, so there is no record to isolate and none to blame. Fail loudly
            // rather than silently swallowing a terminal error.
            return false;
        }

        recovery = new Recovery(steps);
        pauseAssignedPartitions();
        resetSenderForRecovery();
        return true;
    }

    private void processRecoverySlice() {
        if (recovery == null) {
            return;
        }
        try {
            runRecoverySlice();
        } finally {
            requestNextRecoverySlice();
        }
    }

    /**
     * Isolation advances by one slice per put(), and the partitions are paused for its duration,
     * so nothing else brings the task back. Ask Connect to call us again as soon as the next
     * slice is due: its poll would otherwise run to the offset-commit deadline
     * (offset.flush.interval.ms, 60s by default), stretching a replay that needs a handful of
     * slices into minutes and eventually tripping progress.timeout.ms. Connect consumes the
     * value on every poll, so it has to be re-armed for each slice.
     */
    private void requestNextRecoverySlice() {
        if (recovery != null) {
            context.timeout(config.getQwpIsolationSliceMs());
        }
    }

    /**
     * QWP acknowledgements and terminal errors arrive asynchronously. Once the last Kafka
     * record has been delivered, Connect may otherwise block in poll() until the next offset
     * commit (60 seconds by default), leaving the task RUNNING long after QuestDB rejected a
     * frame. Keep asking the worker thread to observe the sender while QuestDB-bound records
     * remain unresolved.
     */
    private void requestNextSenderCheck() {
        if (hasServerPending()) {
            long timeoutMs = Math.min(config.getAllowedLag(), config.getQwpProgressTimeoutMs());
            // WorkerSinkTask ignores non-positive task timeouts.
            context.timeout(Math.max(1L, timeoutMs));
        }
    }

    private void runRecoverySlice() {
        long now = nanoTime();
        if (now - lastProgressNanos >= TimeUnit.MILLISECONDS.toNanos(config.getQwpProgressTimeoutMs())) {
            recovery = null;
            throw new ConnectException("QWP replay isolation made no progress for " + config.getQwpProgressTimeoutMs() + " ms");
        }
        long deadline = now + TimeUnit.MILLISECONDS.toNanos(config.getQwpIsolationSliceMs());

        while (recovery.index < recovery.steps.size()) {
            RecoveryStep step = recovery.steps.get(recovery.index);
            step.records.removeIf(pending -> !pending.needsQuestDbDelivery());
            if (step.records.isEmpty()) {
                recovery.index++;
                continue;
            }

            try {
                if (!step.published) {
                    publishRecoveryStep(step);
                }
                long remainingNanos = deadline - nanoTime();
                if (remainingNanos <= 0L) {
                    return;
                }
                long waitMillis = Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remainingNanos));
                if (!sender.drain(waitMillis)) {
                    return;
                }
                boolean completedRecord = settleRecoveryStep(step);
                lastProgressNanos = nanoTime();
                updateCompletions(completedRecord);
                recovery.index++;
            } catch (LineSenderServerException e) {
                SenderError error = e.getServerError();
                if (!dlqEligibleCategories.contains(error.getCategory())) {
                    recovery = null;
                    throw terminalFailure(e);
                }
                if (step.isolated && step.records.size() == 1) {
                    PendingRecord rejected = step.records.get(0);
                    reportToDlq(rejected, e);
                    recovery.index++;
                    lastProgressNanos = nanoTime();
                    resetSenderForRecovery();
                } else {
                    // Halving the suspect batch is progress: the server answered, and the
                    // search space shrank irreversibly. Without this, an isolation whose
                    // rejections outnumber its settlements looks stalled to detectStall()
                    // and the progress timeout kills a task that is doing exactly its job.
                    lastProgressNanos = nanoTime();
                    recovery.splitCurrentStep();
                    resetSenderForRecovery();
                }
            } catch (InvalidDataException e) {
                if (step.records.size() != 1) {
                    lastProgressNanos = nanoTime();
                    recovery.splitCurrentStep();
                    resetSenderForRecovery();
                    continue;
                }
                PendingRecord rejected = step.records.get(0);
                reportToDlq(rejected, e);
                recovery.index++;
                lastProgressNanos = nanoTime();
            } catch (LineSenderException | HttpClientException e) {
                recovery = null;
                throw new ConnectException("QWP sender failed during replay isolation", e);
            }

            if (nanoTime() >= deadline) {
                return;
            }
        }

        recovery = null;
        updateCompletions();
        resumeAfterRecovery();
    }

    private void publishRecoveryStep(RecoveryStep step) {
        for (PendingRecord pending : step.records) {
            if (recordHandler.handle(pending.record)) {
                pending.markWritten();
            }
        }
        long fsn = sender.flushAndGetSequence();
        step.fsn = fsn;
        step.published = true;
        if (fsn < 0L) {
            // A replay step can be larger than the client's own auto-flush trigger, so the
            // client may have published every row before this checkpoint and left nothing
            // to seal. The rows are still on the wire; their frame numbers are simply not
            // ours to record. settleRecoveryStep() resolves them from the ack watermark
            // once drain() reports that everything published has been acknowledged.
            return;
        }
        for (PendingRecord pending : step.records) {
            pending.waitForQuestDbAck();
        }
        flushEntries.add(new FlushEntry(lastPublishedFsn + 1L, fsn, new ArrayList<>(step.records)));
        lastPublishedFsn = fsn;
    }

    /**
     * Called once a replay step has drained successfully. A step the client published on our
     * behalf carries no frame number of its own, so its rows complete at the current acked
     * watermark - drain() returning true means everything published is acknowledged.
     */
    private boolean settleRecoveryStep(RecoveryStep step) {
        if (step.fsn >= 0L) {
            lastAckedFsn = Math.max(lastAckedFsn, step.fsn);
        } else {
            long acked = Math.max(0L, sender.getAckedFsn());
            lastAckedFsn = Math.max(lastAckedFsn, acked);
        }
        // These rows are durable now, so record that as a fact about each record rather than
        // leaving it to be inferred later from an FSN. Recreating the sender resets the frame
        // numbering, and the completed prefix cannot advance past a record whose DLQ future is
        // still in flight - so a record settled here but not marked would be silently un-settled
        // by the next sender reset, and nothing would ever republish it.
        boolean completedRecord = false;
        for (PendingRecord pending : step.records) {
            completedRecord |= pending.acknowledgeByQuestDb();
        }
        return completedRecord;
    }

    private void resetSenderForRecovery() {
        closeSenderSilently();
        sender = createSender();
        recordHandler.setSender(sender);
        flushEntries.clear();
        pendingRows = 0;
        lastAckedFsn = -1L;
        lastPublishedFsn = -1L;
        for (int i = head, n = retained.size(); i < n; i++) {
            PendingRecord pending = retained.get(i);
            // A settled record keeps its result: the new sender restarts frame numbering, so
            // only records that still have to be republished may carry a dependency on it.
            if (pending.needsQuestDbDelivery()) {
                pending.queueForReplay();
            }
        }
        if (recovery != null && recovery.index < recovery.steps.size()) {
            recovery.steps.get(recovery.index).published = false;
        }
    }

    private void pauseAssignedPartitions() {
        if (!partitionsPaused && !assignment.isEmpty()) {
            context.pause(assignment.toArray(new TopicPartition[0]));
            partitionsPaused = true;
        }
    }

    private void resumeAfterRecovery() {
        if (partitionsPaused && pendingCount() < config.getQwpMaxInflightRows()) {
            resumeAssignedPartitions();
        }
    }

    private void onSenderError(SenderError error) {
        if (error.getAppliedPolicy() == SenderError.Policy.TERMINAL) {
            log.warn("QuestDB QWP terminal error: {}", error);
        } else {
            log.warn("QuestDB QWP transient error; the client will retry: {}", error);
        }
    }

    private void closeSenderSilently() {
        if (sender != null) {
            try {
                sender.close();
            } catch (Exception e) {
                log.warn("Failed to close QWP sender", e);
            } finally {
                sender = null;
            }
        }
    }

    long nanoTime() {
        return System.nanoTime();
    }

    private void throwDeferredFailure() {
        if (deferredFailure == null) {
            return;
        }
        Throwable failure = deferredFailure;
        deferredFailure = null;
        if (failure instanceof ConnectException) {
            throw (ConnectException) failure;
        }
        throw new ConnectException("QWP task failed", failure);
    }

    private static EnumSet<SenderError.Category> parseDlqEligibleCategories(List<String> configured) {
        EnumSet<SenderError.Category> result = EnumSet.noneOf(SenderError.Category.class);
        for (String value : configured) {
            try {
                result.add(SenderError.Category.valueOf(value.trim().toUpperCase(Locale.ENGLISH)));
            } catch (IllegalArgumentException e) {
                throw new ConfigException(QuestDBSinkConnectorConfig.QWP_DLQ_TERMINAL_CATEGORIES_CONFIG, value, "unknown QWP terminal category");
            }
        }
        return result;
    }

    private static void validatePollInterval(Map<String, String> props, long appendDeadline) {
        String pollInterval = props.get("consumer.override.max.poll.interval.ms");
        if (pollInterval == null) {
            return;
        }
        long maxPollInterval;
        try {
            maxPollInterval = Long.parseLong(pollInterval);
        } catch (NumberFormatException e) {
            throw new ConfigException("consumer.override.max.poll.interval.ms", pollInterval, "must be a long");
        }
        if (appendDeadline >= maxPollInterval) {
            throw new ConfigException("sf_append_deadline_millis must be lower than consumer.override.max.poll.interval.ms");
        }
    }

    private static final class FlushEntry {
        private final long fromFsn;
        private final long toFsn;
        private final List<PendingRecord> records;

        private FlushEntry(long fromFsn, long toFsn, List<PendingRecord> records) {
            this.fromFsn = fromFsn;
            this.toFsn = toFsn;
            this.records = records;
        }
    }

    private enum DeliveryState {
        READY_TO_WRITE,
        WRITTEN_NO_FSN,
        WAITING_FOR_QDB_ACK,
        WAITING_FOR_DLQ_ACK,
        ACKED_BY_QDB,
        ACKED_BY_DLQ
    }

    private static final class PendingRecord {
        private final SinkRecord record;
        private final TopicPartition partition;
        private final long offset;
        private DeliveryState state = DeliveryState.READY_TO_WRITE;
        private Future<Void> dlqFuture;

        private PendingRecord(SinkRecord record, TopicPartition partition) {
            this.record = record;
            this.partition = partition;
            this.offset = record.originalKafkaOffset();
        }

        private void acknowledgeByDlq() {
            if (state != DeliveryState.WAITING_FOR_DLQ_ACK) {
                throw illegalTransition("acknowledge by DLQ");
            }
            state = DeliveryState.ACKED_BY_DLQ;
            dlqFuture = null;
        }

        private boolean acknowledgeByQuestDb() {
            if (state == DeliveryState.ACKED_BY_QDB) {
                return false;
            }
            if (state != DeliveryState.WRITTEN_NO_FSN && state != DeliveryState.WAITING_FOR_QDB_ACK) {
                throw illegalTransition("acknowledge by QuestDB");
            }
            state = DeliveryState.ACKED_BY_QDB;
            return true;
        }

        private Future<Void> dlqFuture() {
            if (state != DeliveryState.WAITING_FOR_DLQ_ACK || dlqFuture == null) {
                throw illegalTransition("read DLQ future");
            }
            return dlqFuture;
        }

        private IllegalStateException illegalTransition(String attempted) {
            return new IllegalStateException("Illegal QWP record transition [state=" + state
                    + ", attempted=" + attempted
                    + ", topic=" + partition.topic()
                    + ", partition=" + partition.partition()
                    + ", offset=" + offset + ']');
        }

        private boolean isSettled() {
            return state == DeliveryState.ACKED_BY_QDB || state == DeliveryState.ACKED_BY_DLQ;
        }

        private boolean isWaitingForQuestDbProgress() {
            return state == DeliveryState.WRITTEN_NO_FSN || state == DeliveryState.WAITING_FOR_QDB_ACK;
        }

        private boolean isWrittenWithoutFsn() {
            return state == DeliveryState.WRITTEN_NO_FSN;
        }

        private void markWritten() {
            if (state != DeliveryState.READY_TO_WRITE) {
                throw illegalTransition("mark written");
            }
            state = DeliveryState.WRITTEN_NO_FSN;
        }

        private boolean needsQuestDbDelivery() {
            return state == DeliveryState.READY_TO_WRITE
                    || state == DeliveryState.WRITTEN_NO_FSN
                    || state == DeliveryState.WAITING_FOR_QDB_ACK;
        }

        private void queueForReplay() {
            if (state == DeliveryState.READY_TO_WRITE) {
                return;
            }
            if (state != DeliveryState.WRITTEN_NO_FSN && state != DeliveryState.WAITING_FOR_QDB_ACK) {
                throw illegalTransition("queue for replay");
            }
            state = DeliveryState.READY_TO_WRITE;
        }

        private void sendToDlq(Future<Void> future) {
            if (!needsQuestDbDelivery()) {
                throw illegalTransition("send to DLQ");
            }
            if (future == null) {
                throw new ConnectException("QWP DLQ reporter returned no future [topic=" + partition.topic()
                        + ", partition=" + partition.partition() + ", offset=" + offset + ']');
            }
            state = DeliveryState.WAITING_FOR_DLQ_ACK;
            dlqFuture = future;
        }

        private void waitForQuestDbAck() {
            if (state != DeliveryState.WRITTEN_NO_FSN) {
                throw illegalTransition("wait for QuestDB ACK");
            }
            state = DeliveryState.WAITING_FOR_QDB_ACK;
        }
    }

    private static final class RecoveryStep {
        private final List<PendingRecord> records;
        private final boolean isolated;
        private long fsn = -1L;
        private boolean published;

        private RecoveryStep(List<PendingRecord> records, boolean isolated) {
            this.records = new ArrayList<>(records);
            this.isolated = isolated;
        }
    }

    private static final class Recovery {
        private final List<RecoveryStep> steps;
        private int index;

        private Recovery(List<RecoveryStep> steps) {
            this.steps = steps;
        }

        private void removeRevoked(Set<TopicPartition> revoked) {
            for (RecoveryStep step : steps) {
                step.records.removeIf(pending -> revoked.contains(pending.partition));
            }
        }

        /**
         * Halve a rejected step instead of exploding it into one step per record. A rejected
         * batch is not written at all, so the offender can be bisected: ~log2(n) rejections
         * rather than n, and the innocent records are replayed in bulk instead of one server
         * round trip each. That difference is what keeps isolating a large in-flight window
         * a matter of seconds rather than hours.
         */
        private void splitCurrentStep() {
            RecoveryStep current = steps.remove(index);
            List<PendingRecord> records = current.records;
            if (records.size() == 1) {
                steps.add(index, new RecoveryStep(records, true));
                return;
            }
            int mid = records.size() / 2;
            // insert the tail first, so the head ends up ahead of it and record order holds
            steps.add(index, new RecoveryStep(records.subList(mid, records.size()), records.size() - mid == 1));
            steps.add(index, new RecoveryStep(records.subList(0, mid), mid == 1));
        }
    }
}
