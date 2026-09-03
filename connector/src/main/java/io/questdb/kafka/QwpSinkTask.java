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
import io.questdb.client.cutlass.qwp.client.QwpIngressRoleRejectedException;
import io.questdb.client.cutlass.qwp.client.QwpRoleMismatchException;
import io.questdb.client.cutlass.qwp.client.QwpVersionMismatchException;
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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * QWP delivery uses Kafka as the replay log. The task retains only frame checkpoints; when a
 * sender dies, Connect rewinds the affected partitions and re-fetches the unacknowledged rows.
 */
class QwpSinkTask extends SinkTask {
    private static final long CLOSE_BOUND_MILLIS = 1_000L;
    private static final String ERRORS_TOLERANCE_CONFIG = "errors.tolerance";
    private static final Logger log = LoggerFactory.getLogger(QwpSinkTask.class);

    private final Set<TopicPartition> assignment = new HashSet<>();
    private final ArrayDeque<Checkpoint> checkpoints = new ArrayDeque<>();
    private final FlushConfig flushConfig = new FlushConfig();
    private final Map<String, Map<Integer, TopicPartition>> partitionCache = new HashMap<>();
    private final Map<TopicPartition, Long> quarantineDisposed = new HashMap<>();
    private final Map<TopicPartition, Long> quarantineUntil = new HashMap<>();
    private final Map<TopicPartition, Long> rewindPending = new HashMap<>();

    private QuestDBSinkConnectorConfig config;
    private Throwable deferredFailure;
    private EnumSet<SenderError.Category> dlqEligibleCategories;
    private boolean dlqUsable;
    // Rows written to the sender since the last checkpoint, one mutable range per partition so
    // the per-row path allocates nothing after a partition's first row.
    private Map<TopicPartition, Range> buffered = new HashMap<>();
    private int bufferedRows;
    // Rows in outstanding checkpoints; kept as a running total so backpressure is O(1).
    private int checkpointedRows;
    private long lastAckedFsn = -1L;
    private long lastProgressNanos;
    private Mode mode = Mode.PIPELINED;
    private long nextFlushNanos;
    private boolean partitionsPaused;
    private String patchedConfString;
    private RecordToRowHandler recordHandler;
    private ErrantRecordReporter reporter;
    private Sender sender;
    private boolean senderDown;
    // A revocation removed rows the live sender still owns. Its acknowledgements and rejections
    // can no longer be attributed to anything the task holds, so the next put() retires it
    // before asking it anything, and rewinds the surviving partitions.
    private boolean senderStale;
    private Map<TopicPartition, Range> awaiting;

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
        try {
            reporter = context.errantRecordReporter();
        } catch (NoSuchMethodError | NoClassDefFoundError e) {
            reporter = null;
        }
        dlqUsable = reporter != null && "all".equalsIgnoreCase(props.get(ERRORS_TOLERANCE_CONFIG));
        // The sender is deliberately null. A temporary startup outage belongs to put(), where
        // Connect can retain and redeliver the batch through RetriableException.
        recordHandler = new RecordToRowHandler(
                config,
                null,
                true,
                RecordToRowHandler.SenderErrorPolicy.PROBE,
                true,
                RecordToRowHandler.NameLimits.QWP
        );
        lastProgressNanos = nanoTime();
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        // A put can only happen after WorkerSinkTask.poll(), which applies offset requests.
        rewindPending.clear();
        try {
            if (senderStale) {
                retireStaleSender(records);
                if (mode == Mode.PIPELINED) {
                    return;
                }
            }
            surfaceDeferredFailure();
            if (senderDown) {
                detectStall();
            }
            ensureSender();
            exitQuarantineIfPast();
            if (mode == Mode.QUARANTINE) {
                // No probe precedes this check: a quarantine chunk observes acknowledgements
                // synchronously in its drain, and progress() already moves the clock on every
                // settled or rejected chunk. Without the check here a server that stops
                // answering mid-quarantine would be retried forever.
                detectStall();
                deliverInQuarantine(records);
                return;
            }

            probe();
            detectStall();
            for (SinkRecord record : records) {
                TopicPartition partition = partitionFor(record);
                if (!assignment.contains(partition) || record.value() == null) {
                    continue;
                }
                boolean hadWork = hasWork();
                try {
                    if (recordHandler.handle(record)) {
                        trackBufferedRecord(partition, record, hadWork);
                    }
                } catch (BatchTooLargeForCapException e) {
                    // QWP commits the row before at()/atNow() auto-flushes. The typed failure
                    // retains that batch, so recovery must own this record even though handle()
                    // did not return normally.
                    trackBufferedRecord(partition, record, hadWork);
                    throw e;
                } catch (InvalidDataException e) {
                    reportRecordFailure(record, e);
                }
            }
            checkpointIfDue(records.isEmpty());
            applyBackpressure();
            requestNextTaskTick();
        } catch (LineSenderServerException | BatchTooLargeForCapException e) {
            onRejection(e, records);
        } catch (LineSenderException | HttpClientException e) {
            Throwable latched = probeLatched();
            if (latched instanceof LineSenderServerException) {
                onRejection(latched, records);
            } else {
                onTransportFailure(latched != null ? latched : e, records);
            }
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        if (deferredFailure == null && sender != null && !senderStale && mode == Mode.PIPELINED) {
            try {
                probe();
                checkpoint();
                if (!checkpoints.isEmpty() && sender.drain(config.getQwpCommitAckTimeoutMs())) {
                    clearCheckpoints();
                    progress();
                    afterSettlement();
                }
            } catch (Throwable e) {
                deferFailure(e);
            }
        }

        quarantineUntil.entrySet().removeIf(entry -> {
            OffsetAndMetadata current = currentOffsets.get(entry.getKey());
            return !rewindPending.containsKey(entry.getKey())
                    && current != null
                    && current.offset() >= entry.getValue();
        });
        exitQuarantineIfPast();
        return clamp(currentOffsets);
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
        if (partitions.isEmpty()) {
            return;
        }
        Set<TopicPartition> revoked = new HashSet<>(partitions);
        if (partitionsPaused) {
            Set<TopicPartition> resumable = new HashSet<>(revoked);
            resumable.retainAll(assignment);
            if (!resumable.isEmpty()) {
                context.resume(resumable.toArray(new TopicPartition[0]));
            }
        }
        assignment.removeAll(revoked);
        boolean removedSenderWork = removeKeys(buffered, revoked);
        removeKeys(rewindPending, revoked);
        removeKeys(quarantineUntil, revoked);
        removeKeys(quarantineDisposed, revoked);
        for (Checkpoint checkpoint : checkpoints) {
            removedSenderWork |= removeKeys(checkpoint.spans, revoked);
        }
        if (awaiting != null) {
            removedSenderWork |= removeKeys(awaiting, revoked);
            if (awaiting.isEmpty()) {
                awaiting = null;
            }
        }
        if (removedSenderWork && sender != null) {
            senderStale = true;
        }
        if (assignment.isEmpty()) {
            partitionsPaused = false;
        }
    }

    @Override
    public void stop() {
        closeSenderBounded(null);
    }

    Sender buildSender(String confString) {
        return Sender.builder(confString)
                .errorHandler(this::onSenderError)
                .build();
    }

    long nanoTime() {
        return System.nanoTime();
    }

    private void afterSettlement() {
        context.requestCommit();
        if (partitionsPaused && inflightRows() < config.getQwpMaxInflightRows()) {
            resumeAssignedPartitions();
        }
    }

    private void applyBackpressure() {
        if (!partitionsPaused && inflightRows() > config.getQwpMaxInflightRows() && !assignment.isEmpty()) {
            context.pause(assignment.toArray(new TopicPartition[0]));
            partitionsPaused = true;
        }
    }

    private Map<TopicPartition, OffsetAndMetadata> clamp(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        Map<TopicPartition, OffsetAndMetadata> clamped = new LinkedHashMap<>(currentOffsets.size());
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : currentOffsets.entrySet()) {
            Long hold = resumeAt(entry.getKey());
            Long pendingRewind = rewindPending.get(entry.getKey());
            if (pendingRewind != null && (hold == null || pendingRewind < hold)) {
                hold = pendingRewind;
            }
            OffsetAndMetadata current = entry.getValue();
            if (hold != null && current.offset() > hold) {
                clamped.put(entry.getKey(), new OffsetAndMetadata(hold, current.metadata()));
            } else {
                clamped.put(entry.getKey(), current);
            }
        }
        return clamped;
    }

    private void checkpoint() {
        if (bufferedRows == 0) {
            return;
        }
        long fsn = sender.flushAndGetSequence();
        if (fsn >= 0L) {
            Iterator<Checkpoint> iterator = checkpoints.descendingIterator();
            while (iterator.hasNext()) {
                Checkpoint checkpoint = iterator.next();
                if (checkpoint.fsn >= 0L) {
                    break;
                }
                checkpoint.fsn = fsn;
            }
        }
        checkpoints.addLast(new Checkpoint(fsn, bufferedRows, buffered));
        checkpointedRows += bufferedRows;
        buffered = new HashMap<>();
        bufferedRows = 0;
        nextFlushNanos = nanoTime() + flushConfig.autoFlushNanos;
    }

    private void clearCheckpoints() {
        checkpoints.clear();
        checkpointedRows = 0;
    }

    private void checkpointIfDue(boolean quiescent) {
        if (bufferedRows == 0) {
            return;
        }
        if (quiescent || bufferedRows >= flushConfig.autoFlushRows) {
            checkpoint();
            return;
        }
        long remainingNanos = nextFlushNanos - nanoTime();
        long remainingMillis = TimeUnit.NANOSECONDS.toMillis(remainingNanos);
        if (remainingMillis <= 0L || config.getAllowedLag() == 0) {
            checkpoint();
        }
    }

    private void closeSenderBounded(Throwable cause) {
        Sender closing = sender;
        sender = null;
        if (closing == null) {
            return;
        }
        Thread closeThread = new Thread(() -> {
            try {
                closing.close();
            } catch (Throwable e) {
                log.warn("Failed to close QWP sender", e);
            }
        }, "questdb-qwp-sender-close");
        closeThread.setDaemon(true);
        closeThread.start();
        try {
            closeThread.join(CLOSE_BOUND_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (closeThread.isAlive()) {
            log.warn("QWP sender close exceeded {} ms and will finish in the background", CLOSE_BOUND_MILLIS);
        }
        if (cause != null) {
            log.warn("Retired QWP sender after failure", cause);
        }
    }

    private void deferFailure(Throwable failure) {
        deferredFailure = failure;
        try {
            context.timeout(1L);
        } catch (Throwable wakeupFailure) {
            if (wakeupFailure != failure) {
                failure.addSuppressed(wakeupFailure);
            }
        }
    }

    private void deliverInQuarantine(Collection<SinkRecord> records) {
        List<SinkRecord> members = new ArrayList<>(records.size());
        for (SinkRecord record : records) {
            TopicPartition partition = partitionFor(record);
            Long disposed = quarantineDisposed.get(partition);
            if (assignment.contains(partition)
                    && (disposed == null || record.originalKafkaOffset() >= disposed)) {
                members.add(record);
            }
        }
        if (members.isEmpty()) {
            if (awaiting != null) {
                awaiting = null;
            }
            exitQuarantineIfPast();
            return;
        }

        // Every chunk is a sublist of this call's members, so identity is the right key.
        Set<SinkRecord> reportedHere = Collections.newSetFromMap(new IdentityHashMap<>());
        int start = 0;
        int width = members.size();
        int end = members.size();
        boolean flushed = false;
        if (awaiting != null) {
            end = leadingAwaitedMembers(members);
            if (end == 0) {
                awaiting = null;
                end = members.size();
            } else {
                width = end;
                flushed = true;
            }
        }

        while (start < members.size()) {
            end = Math.min(end, members.size());
            List<SinkRecord> chunk = members.subList(start, end);
            Map<TopicPartition, Range> chunkSpan = flushed ? awaiting : span(chunk);
            try {
                if (!flushed) {
                    ensureSender();
                    boolean hadWork = hasWork();
                    for (SinkRecord record : chunk) {
                        if (record.value() == null || reportedHere.contains(record)) {
                            continue;
                        }
                        try {
                            // The chunk span, not individual row state, is the disposition unit.
                            recordHandler.handle(record);
                        } catch (InvalidDataException e) {
                            reportRecordFailure(record, e);
                            reportedHere.add(record);
                        }
                    }
                    awaiting = chunkSpan;
                    noteWork(hadWork);
                    sender.flushAndGetSequence();
                }
                if (!sender.drain(config.getQwpQuarantineAckTimeoutMs())) {
                    context.timeout(1L);
                    throw new RetriableException("QWP quarantine step not yet acknowledged");
                }
                dispose(chunkSpan);
                awaiting = null;
                progress();
                afterSettlement();
                flushed = false;
                start = end;
                int remaining = members.size() - start;
                if (remaining == 0) {
                    break;
                }
                width = Math.min(Math.max(1, width * 2), remaining);
                end = start + width;
            } catch (LineSenderServerException | BatchTooLargeForCapException e) {
                awaiting = null;
                flushed = false;
                if (!dlqUsable || !isolable(e)) {
                    throw terminalFailure(e);
                }
                retireSender(e);
                progress();
                if (config.isDlqSendBatchOnError() || chunk.size() == 1) {
                    for (SinkRecord record : chunk) {
                        if (record.value() != null && !reportedHere.contains(record)) {
                            reporter.report(record, e);
                            reportedHere.add(record);
                        }
                    }
                    dispose(chunkSpan);
                    afterSettlement();
                    start = end;
                    int remaining = members.size() - start;
                    if (remaining == 0) {
                        break;
                    }
                    width = config.isDlqSendBatchOnError() ? remaining : 1;
                    end = start + width;
                } else {
                    width = Math.max(1, chunk.size() / 2);
                    end = start + width;
                }
            }
        }
        exitQuarantineIfPast();
    }

    private void detectStall() {
        if (hasWork()
                && nanoTime() - lastProgressNanos >= TimeUnit.MILLISECONDS.toNanos(config.getQwpProgressTimeoutMs())) {
            throw new ConnectException("QWP acknowledgements did not advance for "
                    + config.getQwpProgressTimeoutMs() + " ms");
        }
    }

    private void dispose(Map<TopicPartition, Range> span) {
        for (Map.Entry<TopicPartition, Range> entry : span.entrySet()) {
            long next = entry.getValue().next;
            quarantineDisposed.merge(entry.getKey(), next, Math::max);
            Long until = quarantineUntil.get(entry.getKey());
            if (until != null && next >= until) {
                quarantineUntil.remove(entry.getKey());
            }
        }
    }

    private void ensureSender() {
        if (sender != null) {
            return;
        }
        boolean hadWork = hasWork();
        try {
            sender = buildSender(patchedConfString);
        } catch (LineSenderException | HttpClientException e) {
            if (!isTransientInitialConnectFailure(e)) {
                throw new ConnectException("QWP sender configuration or handshake failed", e);
            }
            senderDown = true;
            noteWork(hadWork);
            context.timeout(config.getRetryBackoffMs());
            throw new RetriableException("QuestDB is unreachable; will retry", e);
        }
        senderDown = false;
        recordHandler.setSender(sender);
        lastAckedFsn = -1L;
        nextFlushNanos = nanoTime() + flushConfig.autoFlushNanos;
    }

    private static boolean isTransientInitialConnectFailure(Throwable failure) {
        if (failure instanceof QwpRoleMismatchException
                || failure instanceof QwpIngressRoleRejectedException) {
            return true;
        }
        if (failure instanceof WebSocketUpgradeException) {
            WebSocketUpgradeException upgrade = (WebSocketUpgradeException) failure;
            int statusCode = upgrade.getStatusCode();
            // 421 is how the client itself recognises a role mismatch: keep walking, retry later.
            return statusCode == WebSocketUpgradeException.STATUS_NONE
                    || upgrade.isRoleMismatch()
                    || statusCode >= 500 && statusCode < 600;
        }
        if (failure instanceof QwpAuthFailedException
                || failure instanceof QwpDurableAckMismatchException
                || failure instanceof QwpVersionMismatchException) {
            return false;
        }
        if (failure != null && failure.getClass() == HttpClientException.class) {
            return true;
        }
        return failure instanceof LineSenderException
                && failure.getCause() != null
                && isTransientInitialConnectFailure(failure.getCause());
    }

    private void exitQuarantineIfPast() {
        if (mode == Mode.QUARANTINE && quarantineUntil.isEmpty() && awaiting == null) {
            mode = Mode.PIPELINED;
            quarantineDisposed.clear();
            log.info("QWP quarantine completed");
        }
    }

    private boolean hasOutstanding() {
        return !buffered.isEmpty() || !checkpoints.isEmpty() || awaiting != null;
    }

    private boolean hasWork() {
        return hasOutstanding() || senderDown;
    }

    private int inflightRows() {
        return bufferedRows + checkpointedRows;
    }

    private boolean isolable(Throwable failure) {
        if (failure instanceof BatchTooLargeForCapException) {
            return true;
        }
        if (failure instanceof LineSenderServerException) {
            SenderError error = ((LineSenderServerException) failure).getServerError();
            return dlqEligibleCategories.contains(error.getCategory());
        }
        return false;
    }

    private int leadingAwaitedMembers(List<SinkRecord> members) {
        int end = 0;
        while (end < members.size()) {
            SinkRecord record = members.get(end);
            Range range = awaiting.get(partitionFor(record));
            if (range == null || record.originalKafkaOffset() >= range.next) {
                break;
            }
            end++;
        }
        return end;
    }

    private void noteWork(boolean hadWork) {
        if (!hadWork) {
            lastProgressNanos = nanoTime();
        }
    }

    private void onRejection(Throwable failure, Collection<SinkRecord> batch) {
        if (mode == Mode.QUARANTINE) {
            throw abortQuarantineStep(failure, 1L, "QWP quarantine step aborted");
        }
        recover(failure, batch);
    }

    /**
     * A failure outside a quarantine step's own handling: retire the sender, keep the batch in
     * Connect's hands and let the redelivery resume from the recorded dispositions.
     */
    private RetriableException abortQuarantineStep(Throwable failure, long timeoutMillis, String message) {
        retireSender(failure);
        context.timeout(timeoutMillis);
        return new RetriableException(message, failure);
    }

    private void onSenderError(SenderError error) {
        if (error.getAppliedPolicy() == SenderError.Policy.TERMINAL) {
            log.warn("QuestDB QWP terminal error: {}", error);
        } else {
            log.warn("QuestDB QWP transient error; the client will retry: {}", error);
        }
    }

    private void onTransportFailure(Throwable failure, Collection<SinkRecord> batch) {
        if (mode == Mode.QUARANTINE) {
            throw abortQuarantineStep(failure, config.getRetryBackoffMs(), "QWP sender lost");
        }
        rewindForResend(batch);
        retireSender(failure);
    }

    /** Original coordinates are interned so the pipelined path allocates no TopicPartition per row. */
    private TopicPartition partitionFor(SinkRecord record) {
        String topic = record.originalTopic();
        Integer partition = record.originalKafkaPartition();
        if (topic == null || partition == null) {
            throw new ConnectException("Kafka Connect did not provide original coordinates for a QWP record");
        }
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

    private void probe() {
        sender.awaitAckedFsn(lastAckedFsn, 0L);
        settle();
    }

    private Throwable probeLatched() {
        if (sender == null) {
            return null;
        }
        try {
            sender.awaitAckedFsn(-1L, 0L);
            return null;
        } catch (LineSenderException | HttpClientException e) {
            return e;
        }
    }

    private void progress() {
        lastProgressNanos = nanoTime();
    }

    private void recover(Throwable failure, Collection<SinkRecord> batch) {
        for (TopicPartition partition : assignment) {
            Long high = quarantineUntil.get(partition);
            for (Checkpoint checkpoint : checkpoints) {
                Range range = checkpoint.spans.get(partition);
                if (range != null && (high == null || range.next > high)) {
                    high = range.next;
                }
            }
            Range range = buffered.get(partition);
            if (range != null && (high == null || range.next > high)) {
                high = range.next;
            }
            if (high != null) {
                quarantineUntil.put(partition, high);
            }
        }
        // A rejection from a frame whose records were all revoked has nothing left to
        // quarantine. It still makes this sender stale, but cannot require a DLQ owned by
        // the task; only the new owner can classify and dispose those records.
        if (!quarantineUntil.isEmpty() && (!dlqUsable || !isolable(failure))) {
            throw terminalFailure(failure);
        }
        rewindForResend(batch);
        retireSender(failure);
        if (!quarantineUntil.isEmpty()) {
            mode = Mode.QUARANTINE;
            log.warn("Entering QWP quarantine [until={}]", quarantineUntil);
        }
    }

    private void reportRecordFailure(SinkRecord record, InvalidDataException failure) {
        if (!dlqUsable) {
            throw failure;
        }
        reporter.report(record, failure);
    }

    private void requestNextTaskTick() {
        if (hasWork()) {
            long timeoutMillis = Math.min(config.getAllowedLag(), config.getQwpProgressTimeoutMs());
            if (bufferedRows > 0) {
                long flushMillis = TimeUnit.NANOSECONDS.toMillis(nextFlushNanos - nanoTime());
                timeoutMillis = Math.min(timeoutMillis, Math.max(1L, flushMillis));
            }
            context.timeout(Math.max(1L, timeoutMillis));
        }
    }

    private void resumeAssignedPartitions() {
        if (!assignment.isEmpty()) {
            context.resume(assignment.toArray(new TopicPartition[0]));
        }
        partitionsPaused = false;
    }

    private Long resumeAt(TopicPartition partition) {
        for (Checkpoint checkpoint : checkpoints) {
            Range range = checkpoint.spans.get(partition);
            if (range != null) {
                return range.first;
            }
        }
        Range range = buffered.get(partition);
        if (range == null && awaiting != null) {
            range = awaiting.get(partition);
        }
        return range == null ? null : range.first;
    }

    private void retireSender(Throwable cause) {
        boolean hadWork = hasWork();
        closeSenderBounded(cause);
        senderDown = true;
        senderStale = false;
        clearCheckpoints();
        buffered.clear();
        bufferedRows = 0;
        awaiting = null;
        lastAckedFsn = -1L;
        noteWork(hadWork);
        // The old sender owned every row counted by backpressure. Once that sender and its
        // ledger are gone, keeping Connect-level pauses would prevent the requested rewind
        // from ever being fetched, so retirement must hand them back immediately.
        if (partitionsPaused) {
            resumeAssignedPartitions();
        }
    }

    /**
     * The sender outlived a revocation that removed rows it still owns. Whatever it reports
     * next - a late acknowledgement, a late rejection in any category, a transport error - may
     * belong to rows the task no longer holds, so nothing it says can be acted on. Retire it
     * unasked. In pipelined mode the surviving partitions are rewound to their unacknowledged
     * rows and the batch in hand is dropped, exactly as after a transport failure; in quarantine
     * the batch stays with Connect and the redelivery resumes from the recorded dispositions.
     */
    private void retireStaleSender(Collection<SinkRecord> batch) {
        senderStale = false;
        if (deferredFailure != null) {
            // Raised by the stale generation, so it cannot be attributed either; the surviving
            // rows are re-sent on a fresh sender and a genuine data problem will resurface there.
            log.warn("Discarding a failure reported by a QWP sender retired after partition revocation", deferredFailure);
            deferredFailure = null;
        }
        log.info("Retiring the QWP sender after a partition revocation removed rows it still owned");
        if (mode == Mode.QUARANTINE) {
            throw abortQuarantineStep(null, 1L, "QWP sender retired after partition revocation");
        }
        rewindForResend(batch);
        retireSender(null);
    }

    private void rewindForResend(Collection<SinkRecord> batch) {
        Map<TopicPartition, Long> targets = new HashMap<>();
        for (TopicPartition partition : assignment) {
            Long resume = resumeAt(partition);
            if (resume != null) {
                targets.put(partition, resume);
            }
        }
        for (SinkRecord record : batch) {
            TopicPartition partition = partitionFor(record);
            if (assignment.contains(partition)) {
                targets.putIfAbsent(partition, record.originalKafkaOffset());
            }
        }
        if (!targets.isEmpty()) {
            context.offset(targets);
            rewindPending.putAll(targets);
        }
    }

    private void settle() {
        long acked = sender.getAckedFsn();
        if (acked > lastAckedFsn) {
            lastAckedFsn = acked;
            progress();
        }
        boolean popped = false;
        while (!checkpoints.isEmpty()) {
            Checkpoint checkpoint = checkpoints.peekFirst();
            if (checkpoint.fsn < 0L || checkpoint.fsn > acked) {
                break;
            }
            checkpoints.removeFirst();
            checkpointedRows -= checkpoint.rows;
            popped = true;
            progress();
        }
        if (!checkpoints.isEmpty()
                && checkpoints.peekFirst().fsn < 0L
                && bufferedRows == 0
                && sender.drain(0L)) {
            clearCheckpoints();
            popped = true;
            progress();
        }
        if (popped) {
            afterSettlement();
        }
    }

    private Map<TopicPartition, Range> span(List<SinkRecord> records) {
        Map<TopicPartition, Range> span = new HashMap<>();
        for (SinkRecord record : records) {
            extend(span, partitionFor(record), record.originalKafkaOffset());
        }
        return span;
    }

    private static void extend(Map<TopicPartition, Range> span, TopicPartition partition, long offset) {
        Range range = span.get(partition);
        if (range == null) {
            span.put(partition, new Range(offset));
        } else {
            range.next = offset + 1L;
        }
    }

    private void surfaceDeferredFailure() {
        if (deferredFailure == null) {
            return;
        }
        Throwable failure = deferredFailure;
        deferredFailure = null;
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        throw new ConnectException("QWP task failed", failure);
    }

    private void trackBufferedRecord(TopicPartition partition, SinkRecord record, boolean hadWork) {
        extend(buffered, partition, record.originalKafkaOffset());
        bufferedRows++;
        noteWork(hadWork);
    }

    private ConnectException terminalFailure(Throwable failure) {
        if (failure instanceof LineSenderServerException) {
            SenderError error = ((LineSenderServerException) failure).getServerError();
            return new ConnectException("QuestDB rejected QWP frames [category=" + error.getCategory()
                    + ", fsn=" + error.getFromFsn() + '-' + error.getToFsn() + ']', failure);
        }
        return new ConnectException("QuestDB rejected a QWP batch that exceeds the server cap", failure);
    }

    private static EnumSet<SenderError.Category> parseDlqEligibleCategories(List<String> configured) {
        EnumSet<SenderError.Category> result = EnumSet.noneOf(SenderError.Category.class);
        for (String value : configured) {
            try {
                result.add(SenderError.Category.valueOf(value.trim().toUpperCase(Locale.ENGLISH)));
            } catch (IllegalArgumentException e) {
                throw new ConfigException(QuestDBSinkConnectorConfig.QWP_DLQ_TERMINAL_CATEGORIES_CONFIG,
                        value,
                        "unknown QWP terminal category");
            }
        }
        return result;
    }

    private static <K, V> boolean removeKeys(Map<K, V> map, Collection<K> keys) {
        return map.keySet().removeAll(keys);
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

    private static final class Checkpoint {
        private long fsn;
        private final int rows;
        private final Map<TopicPartition, Range> spans;

        private Checkpoint(long fsn, int rows, Map<TopicPartition, Range> spans) {
            this.fsn = fsn;
            this.rows = rows;
            this.spans = spans;
        }
    }

    private enum Mode {
        PIPELINED,
        QUARANTINE
    }

    /** The offsets of one partition's rows in a checkpoint or chunk: [first, next). */
    private static final class Range {
        private final long first;
        private long next;

        private Range(long offset) {
            this.first = offset;
            this.next = offset + 1L;
        }
    }
}
