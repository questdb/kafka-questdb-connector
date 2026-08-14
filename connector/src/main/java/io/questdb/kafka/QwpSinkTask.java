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
    private static final long UNFLUSHED = Long.MIN_VALUE;
    private static final Logger log = LoggerFactory.getLogger(QwpSinkTask.class);

    private final List<FlushEntry> flushEntries = new ArrayList<>();
    private final List<PendingRecord> retained = new ArrayList<>();
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
        recordHandler = new RecordToRowHandler(config, sender, true, false);
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
            if (!records.isEmpty()) {
                throw new RetriableException("QWP replay isolation is in progress");
            }
            return;
        }
        boolean batchAdmitted = false;
        try {
            probeTerminalError();
            updateCompletions();
            detectStall();
            if (records.isEmpty()) {
                // An empty poll signals quiescence: publish buffered rows now instead of
                // waiting out the flush timer, so low-volume latency stays close to the
                // legacy HTTP path. Publishing is an async write, so this is cheap.
                publishPendingRows();
                applyBackpressure();
                return;
            }

            boolean hadPendingRecords = hasServerPending();
            int retainedStart = retained.size();
            for (SinkRecord record : records) {
                if (record.value() != null) {
                    retained.add(new PendingRecord(record));
                }
            }
            batchAdmitted = true;
            if (!hadPendingRecords && hasServerPending()) {
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
                        pending.rowWritten = true;
                        pendingRows++;
                    }
                } catch (InvalidDataException e) {
                    if (reporter == null) {
                        throw e;
                    }
                    pending.rowRequired = false;
                    pending.dlqFuture = reporter.report(record, e);
                }
            }
            flushIfDue();
            applyBackpressure();
        } catch (LineSenderServerException e) {
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
            probeTerminalError();
            if (recovery == null) {
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
            if (!beginRecovery(e)) {
                deferredFailure = terminalFailure(e);
            }
            return Collections.emptyMap();
        } catch (Throwable e) {
            deferredFailure = e;
            return Collections.emptyMap();
        }

        Map<TopicPartition, Long> earliestIncomplete = new HashMap<>();
        for (PendingRecord pending : retained) {
            earliestIncomplete.merge(pending.partition, pending.offset, Math::min);
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
        bestEffortDrain();
        if (partitions.isEmpty()) {
            return;
        }
        Set<TopicPartition> revoked = new HashSet<>(partitions);
        assignment.removeAll(revoked);
        retained.removeIf(pending -> revoked.contains(pending.partition));
        for (FlushEntry flushEntry : flushEntries) {
            flushEntry.records.removeIf(pending -> revoked.contains(pending.partition));
        }
        flushEntries.removeIf(entry -> entry.records.isEmpty());
        pendingRows = countUnflushedRows();
        if (recovery != null) {
            recovery.removeRevoked(revoked);
        }
        if (assignment.isEmpty()) {
            partitionsPaused = false;
        }
    }

    @Override
    public void stop() {
        bestEffortDrain();
        closeSenderSilently();
    }

    Sender buildSender(String confString) {
        return Sender.builder(confString)
                .errorHandler(this::onSenderError)
                .build();
    }

    private Sender createSender() {
        Sender rawSender = buildSender(patchedConfString);
        String symbolColumns = config.getSymbolColumns();
        return symbolColumns == null ? rawSender : new SymbolRoutingSender(rawSender, symbolColumns);
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
        List<PendingRecord> records = new ArrayList<>(pendingRows);
        for (PendingRecord pending : retained) {
            if (pending.rowRequired && pending.rowWritten && pending.dependencyFsn == UNFLUSHED) {
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
            long acked = Math.max(0L, sender.getAckedFsn());
            for (PendingRecord pending : records) {
                pending.dependencyFsn = acked;
            }
            pendingRows = 0;
            nextFlushNanos = nanoTime() + flushConfig.autoFlushNanos;
            return;
        }
        long fromFsn = lastPublishedFsn + 1L;
        for (PendingRecord pending : records) {
            pending.dependencyFsn = fsn;
        }
        flushEntries.add(new FlushEntry(fromFsn, fsn, records));
        lastPublishedFsn = fsn;
        pendingRows = 0;
        nextFlushNanos = nanoTime() + flushConfig.autoFlushNanos;
    }

    private void probeTerminalError() {
        long acked = Math.max(sender.getAckedFsn(), lastAckedFsn);
        sender.awaitAckedFsn(acked, 0L);
    }

    private void updateCompletions() {
        long acked = Math.max(sender.getAckedFsn(), lastAckedFsn);
        if (acked > lastAckedFsn) {
            lastAckedFsn = acked;
            lastProgressNanos = nanoTime();
        }

        int write = 0;
        boolean completedRecord = false;
        for (int read = 0, n = retained.size(); read < n; read++) {
            PendingRecord pending = retained.get(read);
            boolean complete = pending.dependencyFsn >= 0L && pending.dependencyFsn <= acked;
            if (!complete && pending.dlqFuture != null && pending.dlqFuture.isDone()) {
                completeDlqFuture(pending.dlqFuture);
                complete = true;
            }
            if (!complete) {
                retained.set(write++, pending);
            } else {
                completedRecord = true;
            }
        }
        retained.subList(write, retained.size()).clear();
        flushEntries.removeIf(entry -> entry.toFsn <= acked || entry.records.isEmpty());
        if (completedRecord) {
            context.requestCommit();
        }
        if (recovery == null && partitionsPaused && retained.size() < config.getQwpMaxInflightRows()) {
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
        for (PendingRecord pending : retained) {
            if (pending.rowRequired) {
                return true;
            }
        }
        return false;
    }

    private void applyBackpressure() {
        if (!partitionsPaused && retained.size() > config.getQwpMaxInflightRows() && !assignment.isEmpty()) {
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

    private int countUnflushedRows() {
        int count = 0;
        for (PendingRecord pending : retained) {
            if (pending.rowRequired && pending.rowWritten && pending.dependencyFsn == UNFLUSHED) {
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
            for (PendingRecord pending : retained) {
                if (pending.rowRequired) {
                    pending.rowRequired = false;
                    pending.rowWritten = false;
                    pending.dependencyFsn = UNFLUSHED;
                    pending.dlqFuture = reporter.report(pending.record, exception);
                }
            }
            resetSenderForRecovery();
            resumeAfterRecovery();
            return true;
        }

        FlushEntry suspect = null;
        for (FlushEntry entry : flushEntries) {
            if (entry.fromFsn <= error.getToFsn() && error.getFromFsn() <= entry.toFsn) {
                suspect = entry;
                break;
            }
        }
        if (suspect == null) {
            return false;
        }

        List<RecoveryStep> steps = new ArrayList<>();
        Set<PendingRecord> scheduled = Collections.newSetFromMap(new IdentityHashMap<>());
        for (FlushEntry entry : flushEntries) {
            List<PendingRecord> eligible = new ArrayList<>(entry.records.size());
            for (PendingRecord pending : entry.records) {
                if (pending.rowRequired) {
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
        for (PendingRecord pending : retained) {
            if (pending.rowRequired && !scheduled.contains(pending)) {
                tail.add(pending);
            }
        }
        if (!tail.isEmpty()) {
            steps.add(new RecoveryStep(tail, false));
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
        long now = nanoTime();
        if (now - lastProgressNanos >= TimeUnit.MILLISECONDS.toNanos(config.getQwpProgressTimeoutMs())) {
            recovery = null;
            throw new ConnectException("QWP replay isolation made no progress for " + config.getQwpProgressTimeoutMs() + " ms");
        }
        long deadline = now + TimeUnit.MILLISECONDS.toNanos(config.getQwpIsolationSliceMs());

        while (recovery.index < recovery.steps.size()) {
            RecoveryStep step = recovery.steps.get(recovery.index);
            step.records.removeIf(pending -> !pending.rowRequired);
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
                lastAckedFsn = Math.max(lastAckedFsn, step.fsn);
                lastProgressNanos = nanoTime();
                updateCompletions();
                recovery.index++;
            } catch (LineSenderServerException e) {
                SenderError error = e.getServerError();
                if (!dlqEligibleCategories.contains(error.getCategory())) {
                    recovery = null;
                    throw terminalFailure(e);
                }
                if (step.isolated && step.records.size() == 1) {
                    PendingRecord rejected = step.records.get(0);
                    rejected.rowRequired = false;
                    rejected.rowWritten = false;
                    rejected.dependencyFsn = UNFLUSHED;
                    rejected.dlqFuture = reporter.report(rejected.record, e);
                    recovery.index++;
                    lastProgressNanos = nanoTime();
                    resetSenderForRecovery();
                } else {
                    recovery.splitCurrentStep();
                    resetSenderForRecovery();
                }
            } catch (InvalidDataException e) {
                if (step.records.size() != 1) {
                    recovery.splitCurrentStep();
                    resetSenderForRecovery();
                    continue;
                }
                PendingRecord rejected = step.records.get(0);
                rejected.rowRequired = false;
                rejected.rowWritten = false;
                rejected.dlqFuture = reporter.report(rejected.record, e);
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
                pending.rowWritten = true;
            }
        }
        long fsn = sender.flushAndGetSequence();
        if (fsn < 0L) {
            throw new ConnectException("QWP recovery flush did not return a frame sequence number");
        }
        for (PendingRecord pending : step.records) {
            pending.dependencyFsn = fsn;
        }
        step.fsn = fsn;
        step.published = true;
        flushEntries.add(new FlushEntry(lastPublishedFsn + 1L, fsn, new ArrayList<>(step.records)));
        lastPublishedFsn = fsn;
    }

    private void resetSenderForRecovery() {
        closeSenderSilently();
        sender = createSender();
        recordHandler.setSender(sender);
        flushEntries.clear();
        pendingRows = 0;
        lastAckedFsn = -1L;
        lastPublishedFsn = -1L;
        for (PendingRecord pending : retained) {
            if (pending.rowRequired) {
                pending.dependencyFsn = UNFLUSHED;
                pending.rowWritten = false;
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
        if (partitionsPaused && retained.size() < config.getQwpMaxInflightRows()) {
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

    private void bestEffortDrain() {
        if (sender == null) {
            return;
        }
        try {
            if (!sender.drain(config.getQwpDrainTimeoutMs())) {
                log.warn("Timed out while draining QWP sender during task cleanup");
            }
        } catch (Exception e) {
            log.warn("Failed to drain QWP sender during task cleanup", e);
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

    private static final class PendingRecord {
        private final SinkRecord record;
        private final TopicPartition partition;
        private final long offset;
        private long dependencyFsn = UNFLUSHED;
        private Future<Void> dlqFuture;
        private boolean rowRequired = true;
        private boolean rowWritten;

        private PendingRecord(SinkRecord record) {
            this.record = record;
            String originalTopic = record.originalTopic();
            Integer originalPartition = record.originalKafkaPartition();
            if (originalTopic == null || originalPartition == null) {
                throw new ConnectException("Kafka Connect did not provide original coordinates for a QWP record");
            }
            this.partition = new TopicPartition(originalTopic, originalPartition);
            this.offset = record.originalKafkaOffset();
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

        private void splitCurrentStep() {
            RecoveryStep current = steps.remove(index);
            for (int i = current.records.size() - 1; i >= 0; i--) {
                steps.add(index, new RecoveryStep(Collections.singletonList(current.records.get(i)), true));
            }
        }
    }
}
