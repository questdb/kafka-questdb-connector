package io.questdb.kafka;

import io.questdb.client.Sender;
import io.questdb.client.cutlass.http.client.HttpClientException;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.std.NumericException;
import io.questdb.client.std.ObjList;
import io.questdb.kafka.compat.datetime.DateFormat;
import io.questdb.kafka.compat.datetime.DateLocaleFactory;
import io.questdb.kafka.compat.datetime.microtime.Micros;
import io.questdb.client.std.str.StringSink;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

final class LegacyQuestDBSinkTask extends SinkTask {
    private static final char STRUCT_FIELD_SEPARATOR = '_';
    private static final String PRIMITIVE_KEY_FALLBACK_NAME = "key";
    private static final String PRIMITIVE_VALUE_FALLBACK_NAME = "value";

    private static final Logger log = LoggerFactory.getLogger(LegacyQuestDBSinkTask.class);
    private Sender sender;
    private QuestDBSinkConnectorConfig config;
    private int remainingRetries;
    private long batchesSinceLastError = 0;
    private boolean httpTransport;
    private int allowedLag;
    private long nextFlushNanos;
    private int pendingRows;
    private final FlushConfig flushConfig = new FlushConfig();
    private final ObjList<SinkRecord> inflightSinkRecords = new ObjList<>();
    private ErrantRecordReporter reporter;
    private boolean dlqSendBatchOnError;
    private RecordToRowHandler recordHandler;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        log.info("Starting QuestDB sink task [version={}, commit={}]", VersionUtil.getVersion(), VersionUtil.getGitHash());
        this.config = new QuestDBSinkConnectorConfig(map);
        this.sender = createSender();
        this.recordHandler = new RecordToRowHandler(config, sender, httpTransport, true);
        this.remainingRetries = config.getMaxRetries();
        this.allowedLag = config.getAllowedLag();
        this.nextFlushNanos = System.nanoTime() + flushConfig.autoFlushNanos;


        try {
            reporter = context.errantRecordReporter();
        } catch (NoSuchMethodError | NoClassDefFoundError e) {
            // Kafka older than 2.6
            reporter = null;
        }
        this.dlqSendBatchOnError = config.isDlqSendBatchOnError();
    }

    private Sender createRawSender() {
        log.debug("Creating a new sender");
        String confStr = ClientConfUtils.resolveConfString(config);
        if (confStr != null && !confStr.isEmpty()) {
            log.debug("Using client configuration string");
            StringSink sink = new StringSink();
            httpTransport = ClientConfUtils.patchConfStr(confStr, sink, flushConfig);
            if (!httpTransport) {
                log.info("Using TCP transport, consider using HTTP transport for improved fault tolerance and error handling");
            }
            return Sender.fromConfig(sink);
        }
        log.warn("Configuration options 'host', 'tsl', 'token' and 'username' are deprecated and will be removed in the future. Use 'client.conf.string' instead. See: https://questdb.com/docs/third-party-tools/kafka/#configuration-manual");
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.TCP).address(config.getHost());
        if (config.isTls()) {
            builder.enableTls();
            if ("insecure".equals(config.getTlsValidationMode())) {
                builder.advancedTls().disableCertificateValidation();
            }
        }
        if (config.getToken() != null) {
            String username = config.getUsername();
            if (username == null || username.isEmpty()) {
                throw new ConnectException("Username cannot be empty when using ILP authentication");
            }
            builder.enableAuth(username).authToken(config.getToken().value());
        }
        return builder.build();
    }

    private Sender createSender() {
        Sender rawSender = createRawSender();
        String symbolColumns = config.getSymbolColumns();
        if (symbolColumns == null) {
            log.debug("No symbol columns configured. Using raw sender");
            return rawSender;
        }
        log.debug("Symbol columns configured. Using buffering sender");
        return new BufferingSender(rawSender, symbolColumns);
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        if (collection.isEmpty()) {
            if (httpTransport) {
                log.debug("Received empty collection, let's flush the buffer");
                // Ok, there are no new records to send. Let's flush! Why?
                // We do not want locally buffered row to be stuck in the buffer for too long. It increases
                // latency between the time the record is produced and the time it is visible in QuestDB.
                // If the local buffer is empty then flushing is a cheap no-op.
                flushAndResetCounters();
            } else {
                log.debug("Received empty collection, nothing to do");
            }
            return;
        }

        if (log.isDebugEnabled()) {
            SinkRecord record = collection.iterator().next();
            log.debug("Received {} records. First record kafka coordinates:({}-{}-{}). ",
                    collection.size(), record.topic(), record.kafkaPartition(), record.kafkaOffset());
        }
        try {
            if (sender == null) {
                sender = createSender();
                recordHandler.setSender(sender);
            }
            for (SinkRecord record : collection) {
                if (httpTransport) {
                    inflightSinkRecords.add(record);
                }
                try {
                    if (recordHandler.handle(record)) {
                        pendingRows++;
                    }
                } catch (InvalidDataException ex) {
                    // data format error generated on client-side

                    if (httpTransport && reporter != null) {
                        // we have DLQ set, let's report this single object

                        // remove the last item from in-flight records
                        inflightSinkRecords.setPos(inflightSinkRecords.size() - 1);
                        context.errantRecordReporter().report(record, ex);
                    } else {
                        // ok, no DQL, let's error the connector
                        throw ex;
                    }
                }
            }

            if (httpTransport) {
                if (pendingRows >= flushConfig.autoFlushRows) {
                    log.debug("Flushing data to QuestDB due to auto_flush_rows limit [pending-rows={}, max-pending-rows={}]",
                            pendingRows, flushConfig.autoFlushRows);
                    flushAndResetCounters();
                } else {
                    long remainingNanos = nextFlushNanos - System.nanoTime();
                    long remainingMs = TimeUnit.NANOSECONDS.toMillis(remainingNanos);
                    if (remainingMs <= 0) {
                        log.debug("Flushing data to QuestDB due to auto_flush_interval timeout");
                        flushAndResetCounters();
                    } else if (allowedLag == 0) {
                        log.debug("Flushing data to QuestDB due to zero allowed lag");
                        flushAndResetCounters();
                    } else {
                        log.debug("Flushing data to QuestDB in {} ms", remainingMs);
                        long maxWaitTime = Math.min(remainingMs, allowedLag);
                        context.timeout(maxWaitTime);
                    }
                }
            } else {
                log.debug("Sending {} records", collection.size());
                sender.flush();
                log.debug("Successfully sent {} records", collection.size());
                if (++batchesSinceLastError == 10) {
                    // why 10? why not to reset the retry counter immediately upon a successful flush()?
                    // there are two reasons for server disconnections:
                    // 1. infrastructure: the server is down / unreachable / other_infrastructure_issues
                    // 2. structural: the client is sending bad data (e.g. pushing a string to a double column)
                    // errors in the latter case are not recoverable. upon receiving bad data the server will *eventually* close the connection,
                    // after a while, the client will notice that the connection is closed and will try to reconnect
                    // if we reset the retry counter immediately upon first successful flush() then we end-up in a loop where we flush bad data,
                    // the server closes the connection, the client reconnects, reset the retry counter, and sends bad data again, etc.
                    // to avoid this, we only reset the retry counter after a few successful flushes.
                    log.debug("Successfully sent 10 batches in a row. Resetting retry counter");
                    remainingRetries = config.getMaxRetries();
                }
            }
        } catch (LineSenderException | HttpClientException e) {
            onSenderException(e);
        }
    }

    private void flushAndResetCounters() {
        log.debug("Flushing data to QuestDB");
        try {
            if (sender != null) {
                sender.flush();
            }
            context.requestCommit();
            nextFlushNanos = System.nanoTime() + flushConfig.autoFlushNanos;
            pendingRows = 0;
        } catch (LineSenderException | HttpClientException e) {
            onSenderException(e);
        } finally {
            inflightSinkRecords.clear();
        }
    }

    private void onSenderException(Exception e) {
        if (httpTransport) {
            onHttpSenderException(e);
        } else {
            onTcpSenderException(e);
        }
    }

    private void onTcpSenderException(Exception e) {
        batchesSinceLastError = 0;
        if (--remainingRetries > 0) {
            closeSenderSilently();
            log.debug("Sender exception, retrying in {} ms", config.getRetryBackoffMs());
            context.timeout(config.getRetryBackoffMs());
            throw new RetriableException(e);
        } else {
            throw new ConnectException("Failed to send data to QuestDB after " + config.getMaxRetries() + " retries");
        }
    }

    private void onHttpSenderException(Exception e) {
        closeSenderSilently();
        if (
                (reporter != null && e.getMessage() != null) // hack to detect data parsing errors originating at server-side
                && (e.getMessage().contains("error in line") || e.getMessage().contains("failed to parse line protocol"))
        ) {
            if (dlqSendBatchOnError) {
                // Send all records directly to DLQ without trying to send them to database
                log.warn("Sender exception, sending entire batch to DLQ. Inflight record size = {}", inflightSinkRecords.size(), e);
                for (int i = 0; i < inflightSinkRecords.size(); i++) {
                    SinkRecord sinkRecord = inflightSinkRecords.get(i);
                    log.debug("Reporting record to Kafka Connect error handler (DLQ)...");
                    context.errantRecordReporter().report(sinkRecord, e);
                }
            } else {
                // ok, we have a parsing error, let's try to send records one by one to find the problematic record
                // and we will report it to the error handler. the rest of the records will make it to QuestDB
                log.warn("Sender exception, trying to send problematic record one by one. Inflight record size = {}", inflightSinkRecords.size(), e);
                sender = createSender();
                recordHandler.setSender(sender);
                for (int i = 0; i < inflightSinkRecords.size(); i++) {
                    SinkRecord sinkRecord = inflightSinkRecords.get(i);
                    try {
                        if (recordHandler.handle(sinkRecord)) {
                            pendingRows++;
                        }
                        sender.flush();
                    } catch (Exception ex) {
                        log.warn("Failed to send problematic record to QuestDB. Reporting to Kafka Connect error handler (DQL)...", ex);
                        context.errantRecordReporter().report(sinkRecord, ex);
                        closeSenderSilently();
                        sender = createSender();
                        recordHandler.setSender(sender);
                    }
                }
            }
            nextFlushNanos = System.nanoTime() + flushConfig.autoFlushNanos;
            pendingRows = 0;
        } else {
            // ok, this is not a parsing error, let's just close the sender and rethrow the exception
            nextFlushNanos = System.nanoTime() + flushConfig.autoFlushNanos;
            pendingRows = 0;
            throw new ConnectException("Failed to send data to QuestDB", e);
        }
    }

    private void closeSenderSilently() {
        if (sender != null) {
            try {
                sender.close();
            } catch (Exception ex) {
                log.warn("Failed to close sender", ex);
            } finally {
                sender = null;
            }
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        if (sender != null) {
            flush(currentOffsets);
            return currentOffsets;
        } else {
            // null sender indicates there was an error and we cannot guarantee that the data was actually sent
            // returning empty map will cause the task to avoid committing offsets to Kafka
            return Collections.emptyMap();
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
        if (httpTransport) {
            flushAndResetCounters();
        }
        // TCP transport flushes after each batch so no need to flush here
    }

    @Override
    public void stop() {
        closeSenderSilently();
    }
}
