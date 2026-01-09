package io.questdb.kafka;

import io.questdb.client.Sender;
import io.questdb.cutlass.http.client.HttpClientException;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.str.StringSink;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.types.Password;
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

public final class QuestDBSinkTask extends SinkTask {
    private static final char STRUCT_FIELD_SEPARATOR = '_';
    private static final String PRIMITIVE_KEY_FALLBACK_NAME = "key";
    private static final String PRIMITIVE_VALUE_FALLBACK_NAME = "value";

    private Function<SinkRecord, ? extends CharSequence> recordToTable;
    private static final Logger log = LoggerFactory.getLogger(QuestDBSinkTask.class);
    private Sender sender;
    private QuestDBSinkConnectorConfig config;
    private String timestampColumnName;
    private long timestampColumnValue = Long.MIN_VALUE;
    private TimeUnit timestampUnits;
    private Set<CharSequence> doubleColumns;
    private Set<String> stringTimestampColumns;
    private int remainingRetries;
    private long batchesSinceLastError = 0;
    private DateFormat dataFormat;
    private boolean kafkaTimestampsEnabled;
    private boolean httpTransport;
    private int allowedLag;
    private long nextFlushNanos;
    private int pendingRows;
    private final FlushConfig flushConfig = new FlushConfig();
    private final ObjList<SinkRecord> inflightSinkRecords = new ObjList<>();
    private ErrantRecordReporter reporter;
    private boolean dlqSendBatchOnError;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        log.info("Starting QuestDB sink task [version={}, commit={}]", VersionUtil.getVersion(), VersionUtil.getGitHash());
        this.config = new QuestDBSinkConnectorConfig(map);
        String timestampStringFields = config.getTimestampStringFields();
        if (timestampStringFields != null) {
            stringTimestampColumns = new HashSet<>();
            for (String symbolColumn : timestampStringFields.split(",")) {
                stringTimestampColumns.add(symbolColumn.trim());
            }
        } else {
            stringTimestampColumns = Collections.emptySet();
        }
        dataFormat = TimestampParserCompiler.compilePattern(config.getTimestampFormat());

        String doubleColumnsConfig = config.getDoubleColumns();
        if (doubleColumnsConfig == null) {
            doubleColumns = Collections.emptySet();
        } else {
            doubleColumns = new HashSet<>();
            for (String symbolColumn : doubleColumnsConfig.split(",")) {
                doubleColumns.add(symbolColumn.trim());
            }
        }
        this.sender = createSender();
        this.remainingRetries = config.getMaxRetries();
        this.timestampColumnName = config.getDesignatedTimestampColumnName();
        this.kafkaTimestampsEnabled = config.isDesignatedTimestampKafkaNative();
        this.timestampUnits = config.getTimestampUnitsOrNull();
        this.allowedLag = config.getAllowedLag();
        this.nextFlushNanos = System.nanoTime() + flushConfig.autoFlushNanos;
        this.recordToTable = Templating.newTableTableFn(config.getTable());
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
        Password confStrSecret = config.getConfigurationString();
        String confStr = confStrSecret == null ? null : confStrSecret.value();
        if (confStr == null || confStr.isEmpty()) {
            confStr = System.getenv("QDB_CLIENT_CONF");
        }
        if (confStr != null && !confStr.isEmpty()) {
            confStr = ConfStringEnvInterpolator.expand(confStr);
        }
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
            }
            for (SinkRecord record : collection) {
                if (httpTransport) {
                    inflightSinkRecords.add(record);
                }
                try {
                    handleSingleRecord(record);
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
                for (int i = 0; i < inflightSinkRecords.size(); i++) {
                    SinkRecord sinkRecord = inflightSinkRecords.get(i);
                    try {
                        handleSingleRecord(sinkRecord);
                        sender.flush();
                    } catch (Exception ex) {
                        log.warn("Failed to send problematic record to QuestDB. Reporting to Kafka Connect error handler (DQL)...", ex);
                        context.errantRecordReporter().report(sinkRecord, ex);
                        closeSenderSilently();
                        sender = createSender();
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

    private void handleSingleRecord(SinkRecord record) {
        assert timestampColumnValue == Long.MIN_VALUE;

        Object recordValue = record.value();
        if (recordValue == null) {
            // ignore tombstones
            return;
        }

        CharSequence tableName = recordToTable.apply(record);
        if (tableName == null || tableName.equals("")) {
            throw new InvalidDataException("Table name cannot be empty");
        }

        boolean partialRecord = false;
        try {
            sender.table(tableName);
            partialRecord = true;
            if (config.isIncludeKey()) {
                handleObject(config.getKeyPrefix(), record.keySchema(), record.key(), PRIMITIVE_KEY_FALLBACK_NAME);
            }
            handleObject(config.getValuePrefix(), record.valueSchema(), recordValue, PRIMITIVE_VALUE_FALLBACK_NAME);
        } catch (InvalidDataException ex) {
            if (httpTransport && partialRecord) {
                sender.cancelRow();
            }
            throw ex;
        } catch (LineSenderException ex) {
            if (httpTransport && partialRecord) {
                sender.cancelRow();
            }
            throw new InvalidDataException("object contains invalid data", ex);
        }

        if (kafkaTimestampsEnabled) {
            timestampColumnValue = TimeUnit.MILLISECONDS.toNanos(record.timestamp());
        }

        if (timestampColumnValue == Long.MIN_VALUE) {
            sender.atNow();
        } else {
            try {
                sender.at(timestampColumnValue, ChronoUnit.NANOS);
            } finally {
                timestampColumnValue = Long.MIN_VALUE;
            }
        }
        pendingRows++;
    }

    private void handleStruct(String parentName, Struct value, Schema schema) {
        List<Field> valueFields = schema.fields();
        for (Field field : valueFields) {
            String fieldName = field.name();
            Schema fieldSchema = field.schema();
            Object fieldValue = value.get(fieldName);

            String name = parentName.isEmpty() ? fieldName : parentName + STRUCT_FIELD_SEPARATOR + fieldName;
            handleObject(name, fieldSchema, fieldValue, "");
        }
    }

    private void handleMap(String name, Map<?, ?> value, String fallbackName) {
        for (Map.Entry<?, ?> entry : value.entrySet()) {
            Object mapKey = entry.getKey();
            if (!(mapKey instanceof String)) {
                throw new InvalidDataException("Map keys must be strings");
            }
            String mapKeyName = (String) mapKey;
            String entryName = name.isEmpty() ? mapKeyName : name + STRUCT_FIELD_SEPARATOR + mapKeyName;
            handleObject(entryName, null, entry.getValue(), fallbackName);
        }
    }

    private boolean isDesignatedColumnName(String name, String fallbackName) {
        if (timestampColumnName == null) {
            return false;
        }
        if (timestampColumnName.equals(name)) {
            return true;
        }
        if (name != null) {
            return false;
        }
        return timestampColumnName.equals(fallbackName);
    }

    private void handleObject(String name, Schema schema, Object value, String fallbackName) {
        assert !name.isEmpty() || !fallbackName.isEmpty();

        if (isDesignatedColumnName(name, fallbackName)) {
            assert timestampColumnValue == Long.MIN_VALUE;
            if (value == null) {
                throw new InvalidDataException("Timestamp column value cannot be null");
            }
            timestampColumnValue = resolveDesignatedTimestampColumnValue(value, schema);
            return;
        }
        if (value == null) {
            return;
        }
        if (tryWriteLogicalType(name.isEmpty() ? fallbackName : name, schema, value)) {
            return;
        }
        // ok, not a known logical type, try primitive types
        if (tryWritePhysicalTypeFromSchema(name, schema, value, fallbackName)) {
            return;
        }
        writePhysicalTypeWithoutSchema(name, value, fallbackName);
    }

    private long resolveDesignatedTimestampColumnValue(Object value, Schema schema) {
        if (value instanceof java.util.Date) {
            log.debug("Timestamp column value is a java.util.Date");
            return TimeUnit.MILLISECONDS.toNanos(((java.util.Date) value).getTime());
        }
        if (value instanceof String) {
            log.debug("Timestamp column value is a string");
            return parseToMicros((String) value) * 1000;
        }
        if (!(value instanceof Long)) {
            throw new InvalidDataException("Unsupported timestamp column type: " + value.getClass());
        }
        long longValue = (Long) value;
        TimeUnit inputUnit;
        if (schema == null || !"io.debezium.time.MicroTimestamp".equals(schema.name())) {
            inputUnit = TimestampHelper.getTimestampUnits(timestampUnits, longValue);
            log.debug("Detected {} as timestamp units", inputUnit);
        } else {
            // special case: Debezium micros since epoch
            inputUnit = TimeUnit.MICROSECONDS;
            log.debug("Detected Debezium micros as timestamp units");
        }
        return inputUnit.toNanos(longValue);
    }

    private void writePhysicalTypeWithoutSchema(String name, Object value, String fallbackName) {
        if (value == null) {
            return;
        }
        String actualName = name.isEmpty() ? fallbackName : sanitizeName(name);
        if (value instanceof String) {
            String stringVal = (String) value;
            if (stringTimestampColumns.contains(actualName)) {
                long timestamp = parseToMicros(stringVal);
                sender.timestampColumn(actualName, timestamp, ChronoUnit.MICROS);
            } else {
                sender.stringColumn(actualName, stringVal);
            }
        } else if (value instanceof Long) {
            Long longValue = (Long) value;
            if (doubleColumns.contains(actualName)) {
                sender.doubleColumn(actualName, longValue.doubleValue());
            } else {
                sender.longColumn(actualName, longValue);
            }
        } else if (value instanceof Integer) {
            Integer intValue = (Integer) value;
            if (doubleColumns.contains(actualName)) {
                sender.doubleColumn(actualName, intValue.doubleValue());
            } else {
                sender.longColumn(actualName, intValue);
            }
        } else if (value instanceof Boolean) {
            sender.boolColumn(actualName, (Boolean) value);
        } else if (value instanceof Double) {
            sender.doubleColumn(actualName, (Double) value);
        } else if (value instanceof Map) {
            handleMap(name, (Map<?, ?>) value, fallbackName);
        } else if (value instanceof java.util.Date) {
            long epochMillis = ((java.util.Date) value).getTime();
            sender.timestampColumn(actualName, TimeUnit.MILLISECONDS.toMicros(epochMillis), ChronoUnit.MICROS);
        } else if (value instanceof List) {
            handleArrayWithoutSchema(actualName, (List<?>) value);
        } else {
            onUnsupportedType(actualName, value.getClass().getName());
        }
    }

    private long parseToMicros(String timestamp) {
        try {
            return dataFormat.parse(timestamp, DateFormatUtils.EN_LOCALE);
        } catch (NumericException e) {
            throw new InvalidDataException("Cannot parse timestamp: " + timestamp + " with the configured format '" + config.getTimestampFormat() +"' use '"
                    + QuestDBSinkConnectorConfig.TIMESTAMP_FORMAT + "' to configure the right timestamp format. " +
                    "See https://questdb.io/docs/reference/function/date-time/#date-and-timestamp-format for timestamp parser documentation. ", e);
        }
    }

    private static String sanitizeName(String name) {
        // todo: proper implementation
        return name.replace('.', '_');
    }

    private boolean tryWritePhysicalTypeFromSchema(String name, Schema schema, Object value, String fallbackName) {
        if (schema == null) {
            return false;
        }
        Schema.Type type = schema.type();
        String primitiveTypesName = name.isEmpty() ? fallbackName : name;
        String sanitizedName = sanitizeName(primitiveTypesName);
        switch (type) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
                Number l = (Number) value;
                sender.longColumn(sanitizedName, l.longValue());
                break;
            case FLOAT32:
            case FLOAT64:
                Number d = (Number) value;
                sender.doubleColumn(sanitizedName, d.doubleValue());
                break;
            case BOOLEAN:
                Boolean b = (Boolean) value;
                sender.boolColumn(sanitizedName, b);
                break;
            case STRING:
                String s = (String) value;
                if (stringTimestampColumns.contains(primitiveTypesName)) {
                    long timestamp = parseToMicros(s);
                    sender.timestampColumn(sanitizedName, timestamp, ChronoUnit.MICROS);
                } else {
                    sender.stringColumn(sanitizedName, s);
                }
                break;
            case STRUCT:
                handleStruct(name, (Struct) value, schema);
                break;
            case ARRAY:
                handleArray(sanitizedName, value, schema);
                break;
            case BYTES:
            case MAP:
            default:
                onUnsupportedType(name, type);
        }
        return true;
    }

    private void handleArray(String name, Object value, Schema schema) {
        if (value == null) {
            return;
        }
        
        Schema valueSchema = schema.valueSchema();
        if (valueSchema == null) {
            throw new InvalidDataException("Array schema must have a value schema");
        }
        
        Schema.Type elementType = valueSchema.type();
        
        if (elementType == Schema.Type.FLOAT32 || elementType == Schema.Type.FLOAT64) {
            List<?> list = (List<?>) value;
            // todo: do not allocate new arrays, depends on https://github.com/questdb/questdb/pull/5996
            double[] doubleArray = new double[list.size()];
            for (int i = 0; i < list.size(); i++) {
                Object element = list.get(i);
                if (element == null) {
                    throw new InvalidDataException("Array elements cannot be null for QuestDB double arrays");
                }
                doubleArray[i] = ((Number) element).doubleValue();
            }
            sender.doubleArray(name, doubleArray);
        } else if (elementType == Schema.Type.ARRAY) {
            Schema nestedValueSchema = valueSchema.valueSchema();
            if (nestedValueSchema != null && (nestedValueSchema.type() == Schema.Type.FLOAT32 || nestedValueSchema.type() == Schema.Type.FLOAT64)) {
                List<?> list = (List<?>) value;
                
                // First, validate that all rows have the same length (no jagged arrays)
                if (!list.isEmpty()) {
                    int expectedRowLength = ((List<?>) list.get(0)).size();
                    for (int i = 0; i < list.size(); i++) {
                        Object row = list.get(i);
                        if (row == null) {
                            throw new InvalidDataException("Array elements cannot be null for QuestDB double arrays");
                        }
                        List<?> rowList = (List<?>) row;
                        if (rowList.size() != expectedRowLength) {
                            throw new InvalidDataException("QuestDB does not support jagged arrays. All rows must have the same length. Expected: " + expectedRowLength + ", but row " + i + " has length: " + rowList.size());
                        }
                    }
                }
                
                double[][] doubleArray2D = new double[list.size()][];
                for (int i = 0; i < list.size(); i++) {
                    Object row = list.get(i);
                    List<?> rowList = (List<?>) row;
                    doubleArray2D[i] = new double[rowList.size()];
                    for (int j = 0; j < rowList.size(); j++) {
                        Object element = rowList.get(j);
                        if (element == null) {
                            throw new InvalidDataException("Array elements cannot be null for QuestDB double arrays");
                        }
                        doubleArray2D[i][j] = ((Number) element).doubleValue();
                    }
                }
                sender.doubleArray(name, doubleArray2D);
            } else if (nestedValueSchema != null && nestedValueSchema.type() == Schema.Type.ARRAY) {
                Schema nestedNestedValueSchema = nestedValueSchema.valueSchema();
                if (nestedNestedValueSchema != null && (nestedNestedValueSchema.type() == Schema.Type.FLOAT32 || nestedNestedValueSchema.type() == Schema.Type.FLOAT64)) {
                    List<?> list = (List<?>) value;
                    
                    // First, validate dimensions for 3D array (no jagged arrays)
                    if (!list.isEmpty()) {
                        List<?> firstMatrix = (List<?>) list.get(0);
                        int expectedMatrixHeight = firstMatrix.size();
                        int expectedRowLength = firstMatrix.isEmpty() ? 0 : ((List<?>) firstMatrix.get(0)).size();
                        
                        for (int i = 0; i < list.size(); i++) {
                            Object matrix = list.get(i);
                            if (matrix == null) {
                                throw new InvalidDataException("Array elements cannot be null for QuestDB double arrays");
                            }
                            List<?> matrixList = (List<?>) matrix;
                            if (matrixList.size() != expectedMatrixHeight) {
                                throw new InvalidDataException("QuestDB does not support jagged arrays. All matrices must have the same height. Expected: " + expectedMatrixHeight + ", but matrix " + i + " has height: " + matrixList.size());
                            }
                            
                            for (int j = 0; j < matrixList.size(); j++) {
                                Object row = matrixList.get(j);
                                if (row == null) {
                                    throw new InvalidDataException("Array elements cannot be null for QuestDB double arrays");
                                }
                                List<?> rowList = (List<?>) row;
                                if (rowList.size() != expectedRowLength) {
                                    throw new InvalidDataException("QuestDB does not support jagged arrays. All rows must have the same length. Expected: " + expectedRowLength + ", but matrix " + i + " row " + j + " has length: " + rowList.size());
                                }
                            }
                        }
                    }
                    
                    double[][][] doubleArray3D = new double[list.size()][][];
                    for (int i = 0; i < list.size(); i++) {
                        Object matrix = list.get(i);
                        List<?> matrixList = (List<?>) matrix;
                        doubleArray3D[i] = new double[matrixList.size()][];
                        for (int j = 0; j < matrixList.size(); j++) {
                            Object row = matrixList.get(j);
                            List<?> rowList = (List<?>) row;
                            doubleArray3D[i][j] = new double[rowList.size()];
                            for (int k = 0; k < rowList.size(); k++) {
                                Object element = rowList.get(k);
                                if (element == null) {
                                    throw new InvalidDataException("Array elements cannot be null for QuestDB double arrays");
                                }
                                doubleArray3D[i][j][k] = ((Number) element).doubleValue();
                            }
                        }
                    }
                    sender.doubleArray(name, doubleArray3D);
                } else {
                    onUnsupportedType(name, "Multidimensional ARRAY with unsupported element type");
                }
            } else {
                onUnsupportedType(name, "Multidimensional ARRAY with unsupported element type");
            }
        } else {
            onUnsupportedType(name, "ARRAY<" + elementType + ">");
        }
    }

    private void handleArrayWithoutSchema(String name, List<?> list) {
        if (list == null || list.isEmpty()) {
            return;
        }
        
        Object firstElement = list.get(0);
        if (firstElement == null) {
            throw new InvalidDataException("QuestDB array elements cannot be null");
        }
        
        if (firstElement instanceof Number) {
            // todo: do not allocate new arrays
            double[] doubleArray = new double[list.size()];
            for (int i = 0; i < list.size(); i++) {
                Object element = list.get(i);
                if (element == null) {
                    onUnsupportedType(name, "null element in ARRAY");
                } else if (!(element instanceof Number)) {
                    onUnsupportedType(name, "ARRAY<" + element.getClass().getSimpleName() + ">");
                } else {
                    doubleArray[i] = ((Number) element).doubleValue();
                }
            }
            sender.doubleArray(name, doubleArray);
        } else if (firstElement instanceof List) {
            List<?> firstList = (List<?>) firstElement;
            if (firstList.isEmpty()) {
                throw new InvalidDataException("QuestDB 2D array cannot contain empty rows");
            }
            Object firstNestedElement = firstList.get(0);
            if (firstNestedElement == null) {
                throw new InvalidDataException("QuestDB 2D array elements cannot be null");
            }
            
            if (firstNestedElement instanceof Number) {
                // First, validate that all rows have the same length (no jagged arrays)
                int expectedRowLength = firstList.size();
                for (int i = 0; i < list.size(); i++) {
                    Object row = list.get(i);
                    if (row == null) {
                        throw new InvalidDataException("QuestDB 2D array rows cannot be null");
                    }
                    if (!(row instanceof List)) {
                        throw new InvalidDataException("QuestDB 2D array rows must be Lists");
                    }
                    List<?> rowList = (List<?>) row;
                    if (rowList.size() != expectedRowLength) {
                        throw new InvalidDataException("QuestDB does not support jagged arrays. All rows must have the same length. Expected: " + expectedRowLength + ", but row " + i + " has length: " + rowList.size());
                    }
                }
                
                double[][] doubleArray2D = new double[list.size()][];
                for (int i = 0; i < list.size(); i++) {
                    Object row = list.get(i);
                    List<?> rowList = (List<?>) row;
                    doubleArray2D[i] = new double[rowList.size()];
                    for (int j = 0; j < rowList.size(); j++) {
                        Object element = rowList.get(j);
                        if (element == null) {
                            throw new InvalidDataException("QuestDB 2D array elements cannot be null");
                        }
                        if (!(element instanceof Number)) {
                            throw new InvalidDataException("QuestDB 2D array elements must be Numbers");
                        }
                        doubleArray2D[i][j] = ((Number) element).doubleValue();
                    }
                }
                sender.doubleArray(name, doubleArray2D);
            } else if (firstNestedElement instanceof List) {
                List<?> firstNestedList = (List<?>) firstNestedElement;
                if (firstNestedList.isEmpty()) {
                    throw new InvalidDataException("QuestDB 3D array cannot contain empty matrices");
                }
                Object firstNestedNestedElement = firstNestedList.get(0);
                if (firstNestedNestedElement == null) {
                    throw new InvalidDataException("QuestDB 3D array elements cannot be null");
                }
                
                if (firstNestedNestedElement instanceof Number) {
                    // First, validate dimensions for 3D array (no jagged arrays)
                    int expectedMatrixHeight = firstList.size();
                    int expectedRowLength = firstNestedList.size();
                    
                    for (int i = 0; i < list.size(); i++) {
                        Object matrix = list.get(i);
                        if (matrix == null) {
                            throw new InvalidDataException("QuestDB 3D array matrices cannot be null");
                        }
                        if (!(matrix instanceof List)) {
                            throw new InvalidDataException("QuestDB 3D array matrices must be Lists");
                        }
                        List<?> matrixList = (List<?>) matrix;
                        if (matrixList.size() != expectedMatrixHeight) {
                            throw new InvalidDataException("QuestDB does not support jagged arrays. All matrices must have the same height. Expected: " + expectedMatrixHeight + ", but matrix " + i + " has height: " + matrixList.size());
                        }
                        
                        for (int j = 0; j < matrixList.size(); j++) {
                            Object row = matrixList.get(j);
                            if (row == null) {
                                throw new InvalidDataException("QuestDB 3D array rows cannot be null");
                            }
                            if (!(row instanceof List)) {
                                throw new InvalidDataException("QuestDB 3D array rows must be Lists");
                            }
                            List<?> rowList = (List<?>) row;
                            if (rowList.size() != expectedRowLength) {
                                throw new InvalidDataException("QuestDB does not support jagged arrays. All rows must have the same length. Expected: " + expectedRowLength + ", but matrix " + i + " row " + j + " has length: " + rowList.size());
                            }
                        }
                    }
                    
                    double[][][] doubleArray3D = new double[list.size()][][];
                    for (int i = 0; i < list.size(); i++) {
                        Object matrix = list.get(i);
                        List<?> matrixList = (List<?>) matrix;
                        doubleArray3D[i] = new double[matrixList.size()][];
                        for (int j = 0; j < matrixList.size(); j++) {
                            Object row = matrixList.get(j);
                            List<?> rowList = (List<?>) row;
                            doubleArray3D[i][j] = new double[rowList.size()];
                            for (int k = 0; k < rowList.size(); k++) {
                                Object element = rowList.get(k);
                                if (element == null) {
                                    throw new InvalidDataException("QuestDB 3D array elements cannot be null");
                                }
                                if (!(element instanceof Number)) {
                                    throw new InvalidDataException("QuestDB 3D array elements must be Numbers");
                                }
                                doubleArray3D[i][j][k] = ((Number) element).doubleValue();
                            }
                        }
                    }
                    sender.doubleArray(name, doubleArray3D);
                } else {
                    onUnsupportedType(name, "3D ARRAY with unsupported element type: " + firstNestedNestedElement.getClass().getSimpleName());
                }
            } else {
                onUnsupportedType(name, "2D ARRAY with unsupported element type: " + firstNestedElement.getClass().getSimpleName());
            }
        } else {
            onUnsupportedType(name, "ARRAY<" + firstElement.getClass().getSimpleName() + ">");
        }
    }

    private void onUnsupportedType(String name, Object type) {
        if (config.isSkipUnsupportedTypes()) {
            log.debug("Skipping unsupported type: {}, name: {}", type, name);
        } else {
            throw new InvalidDataException("Unsupported type: " + type + ", name: " + name);
        }
    }

    private boolean tryWriteLogicalType(String name, Schema schema, Object value) {
        if (schema == null || schema.name() == null) {
            return false;
        }
        switch (schema.name()) {
            case "io.debezium.time.MicroTimestamp":
                long l = (Long) value;
                sender.timestampColumn(name, l, ChronoUnit.MICROS);
                return true;
            case "io.debezium.time.Date":
                int i = (Integer) value;
                long micros = Timestamps.addDays(0, i);
                sender.timestampColumn(name, micros, ChronoUnit.MICROS);
                return true;
            case Timestamp.LOGICAL_NAME:
            case Date.LOGICAL_NAME:
                java.util.Date d = (java.util.Date) value;
                long epochMillis = d.getTime();
                sender.timestampColumn(name, epochMillis, ChronoUnit.MILLIS);
                return true;
            case Time.LOGICAL_NAME:
                java.util.Date timeValue = (java.util.Date) value;
                long dayMillis = timeValue.getTime();
                sender.longColumn(name, dayMillis);
                return true;
            case Decimal.LOGICAL_NAME:
                onUnsupportedType(name, schema.name());
        }
        return false;
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
