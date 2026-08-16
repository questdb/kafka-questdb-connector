package io.questdb.kafka;

import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.std.NumericException;
import io.questdb.kafka.compat.datetime.DateFormat;
import io.questdb.kafka.compat.datetime.DateLocaleFactory;
import io.questdb.kafka.compat.datetime.microtime.Micros;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

final class RecordToRowHandler {
    private static final char STRUCT_FIELD_SEPARATOR = '_';
    private static final String PRIMITIVE_KEY_FALLBACK_NAME = "key";
    private static final String PRIMITIVE_VALUE_FALLBACK_NAME = "value";
    private static final Logger log = LoggerFactory.getLogger(RecordToRowHandler.class);

    private final QuestDBSinkConnectorConfig config;
    private final boolean cancelPartialRow;
    private final boolean wrapSenderErrors;
    private final boolean rawJson;
    private final boolean rawJsonEnvelope;
    private final Function<SinkRecord, ? extends CharSequence> recordToTable;
    private final String timestampColumnName;
    private final TimeUnit timestampUnits;
    private final Set<CharSequence> doubleColumns;
    // Non-empty only on QWP: ILP needs symbols written before any other column, so the
    // legacy transports route them through BufferingSender instead. QWP accepts symbols
    // interleaved with fields, so they can go straight to the wire from here.
    private final Set<String> symbolColumns;
    private final Set<String> stringTimestampColumns;
    private final DateFormat dataFormat;
    private final boolean kafkaTimestampsEnabled;
    private final String[] composedTimestampFields;
    private final String[] composedTimestampValues;
    private final MultiPartCharSequence composedBuffer;
    private long timestampColumnValue = Long.MIN_VALUE;
    private static final com.fasterxml.jackson.core.JsonFactory JSON_FACTORY = new com.fasterxml.jackson.core.JsonFactory();
    // Jackson 2.13 (what Connect provides here) predates StreamReadConstraints, so nesting is
    // unbounded. Without a cap a deeply nested payload is a StackOverflowError - an Error, which
    // Kafka Connect never routes to the DLQ - so the record kills the task on every restart.
    private static final int MAX_JSON_DEPTH = 64;
    private Sender sender;

    RecordToRowHandler(QuestDBSinkConnectorConfig config, Sender sender, boolean cancelPartialRow, boolean wrapSenderErrors) {
        this(config, sender, cancelPartialRow, wrapSenderErrors, false);
    }

    RecordToRowHandler(QuestDBSinkConnectorConfig config, Sender sender, boolean cancelPartialRow, boolean wrapSenderErrors,
                       boolean routeSymbolsDirectly) {
        this.config = config;
        this.sender = sender;
        this.cancelPartialRow = cancelPartialRow;
        this.wrapSenderErrors = wrapSenderErrors;
        this.rawJson = config.isRawJsonFormat();
        this.rawJsonEnvelope = config.isRawJsonEnvelope();

        String symbolColumnsConfig = routeSymbolsDirectly ? config.getSymbolColumns() : null;
        if (symbolColumnsConfig == null) {
            symbolColumns = Collections.emptySet();
        } else {
            symbolColumns = new HashSet<>();
            for (String column : symbolColumnsConfig.split(",")) {
                symbolColumns.add(column.trim());
            }
        }

        String timestampStringFields = config.getTimestampStringFields();
        if (timestampStringFields != null) {
            stringTimestampColumns = new HashSet<>();
            for (String column : timestampStringFields.split(",")) {
                stringTimestampColumns.add(column.trim());
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
            for (String column : doubleColumnsConfig.split(",")) {
                doubleColumns.add(column.trim());
            }
        }
        kafkaTimestampsEnabled = config.isDesignatedTimestampKafkaNative();
        timestampUnits = config.getTimestampUnitsOrNull();
        recordToTable = Templating.newTableTableFn(config.getTable());

        String timestampFieldName = config.getDesignatedTimestampColumnName();
        if (timestampFieldName != null && timestampFieldName.contains(",")) {
            String[] fields = timestampFieldName.split(",");
            composedTimestampFields = new String[fields.length];
            for (int i = 0; i < fields.length; i++) {
                String field = fields[i].trim();
                if (field.isEmpty()) {
                    throw new ConnectException("Empty field name in '" + QuestDBSinkConnectorConfig.DESIGNATED_TIMESTAMP_COLUMN_NAME_CONFIG
                            + "': '" + timestampFieldName + "'");
                }
                composedTimestampFields[i] = field;
            }
            timestampColumnName = null;
            composedTimestampValues = new String[composedTimestampFields.length];
            composedBuffer = composedTimestampFields.length == 2
                    ? new TwoPartCharSequence()
                    : new CompositeCharSequence(composedTimestampFields.length);
        } else {
            timestampColumnName = timestampFieldName;
            composedTimestampFields = null;
            composedTimestampValues = null;
            composedBuffer = null;
        }
        if (rawJson && composedTimestampFields != null) {
            throw new ConnectException("value.format=json does not support composed timestamps ("
                    + QuestDBSinkConnectorConfig.DESIGNATED_TIMESTAMP_COLUMN_NAME_CONFIG
                    + " naming several fields)");
        }
    }

    /**
     * Fast path for raw JSON payloads (value.converter=ByteArrayConverter): the bytes are
     * parsed once, directly into Sender calls. The standard path costs two object graphs
     * per record - JsonConverter builds a JsonNode tree and then converts it into a Map of
     * boxed values - and both are garbage immediately.
     */
    boolean handleRawJson(SinkRecord record, byte[] payload) {
        assert timestampColumnValue == Long.MIN_VALUE;
        if (payload == null) {
            return false; // tombstone
        }
        if (payload.length == 0) {
            throw new InvalidDataException("Empty payload cannot be parsed as JSON");
        }
        CharSequence tableName = recordToTable.apply(record);
        if (tableName == null || tableName.length() == 0) {
            throw new InvalidDataException("Table name cannot be empty");
        }
        boolean partialRecord = false;
        try {
            sender.table(tableName);
            partialRecord = true;
            if (config.isIncludeKey()) {
                // The key does not come from the JSON payload, so it is still whatever the
                // key converter produced. Reuse the standard handling rather than a second
                // type dispatch: structs, maps and logical types then behave identically.
                handleObject(config.getKeyPrefix(), record.keySchema(), record.key(), PRIMITIVE_KEY_FALLBACK_NAME);
            }
            try (com.fasterxml.jackson.core.JsonParser parser = JSON_FACTORY.createParser(payload)) {
                if (parser.nextToken() != com.fasterxml.jackson.core.JsonToken.START_OBJECT) {
                    throw new InvalidDataException("JSON payload must be an object");
                }
                if (rawJsonEnvelope) {
                    writeJsonEnvelope(parser);
                } else {
                    writeJsonObject(parser, config.getValuePrefix(), 1);
                }
            } catch (java.io.IOException e) {
                throw new InvalidDataException("Cannot parse JSON payload", e);
            }
        } catch (InvalidDataException | LineSenderException ex) {
            // a half-written row must not leak its timestamp into the next record
            timestampColumnValue = Long.MIN_VALUE;
            if (cancelPartialRow && partialRecord) {
                sender.cancelRow();
            }
            if (ex instanceof LineSenderException && wrapSenderErrors) {
                throw new InvalidDataException("object contains invalid data", ex);
            }
            throw ex;
        }

        if (kafkaTimestampsEnabled) {
            timestampColumnValue = TimeUnit.MILLISECONDS.toMicros(record.timestamp());
        }
        if (timestampColumnValue == Long.MIN_VALUE) {
            sender.atNow();
        } else {
            try {
                sender.at(timestampColumnValue, ChronoUnit.MICROS);
            } finally {
                timestampColumnValue = Long.MIN_VALUE;
            }
        }
        return true;
    }

    private void writeJsonObject(com.fasterxml.jackson.core.JsonParser parser, String prefix, int depth) throws java.io.IOException {
        if (depth > MAX_JSON_DEPTH) {
            throw new InvalidDataException("JSON nesting deeper than " + MAX_JSON_DEPTH + " is not supported");
        }
        while (parser.nextToken() != com.fasterxml.jackson.core.JsonToken.END_OBJECT) {
            String rawName = parser.currentName();

            String name = prefix.isEmpty() ? rawName : prefix + STRUCT_FIELD_SEPARATOR + rawName;
            com.fasterxml.jackson.core.JsonToken token = parser.nextToken();
            if (isDesignatedRawField(name)) {
                // Every token type must be considered here. Leaving it to the per-type
                // writers meant a null, fractional, boolean or structured timestamp was
                // written as an ordinary column and the row silently got wall-clock time.
                readDesignatedTimestamp(parser, token);
                continue;
            }
            switch (token) {
                case VALUE_NULL:
                    break;
                case START_OBJECT:
                    writeJsonObject(parser, name, depth + 1);
                    break;
                case START_ARRAY:
                    // Arrays are rare, and their validation (jagged rows, null elements,
                    // element types, skip.unsupported.types) is intricate. Materialise the
                    // array in the same shape the converter produces and reuse that code
                    // rather than reimplementing the rules here.
                    handleArrayWithoutSchema(sanitizeName(name), readJsonList(parser, depth + 1));
                    break;
                case VALUE_STRING:
                    writeJsonString(name, parser.getText());
                    break;
                case VALUE_NUMBER_INT:
                    if (parser.getNumberType() == com.fasterxml.jackson.core.JsonParser.NumberType.BIG_INTEGER) {
                        writeJsonDouble(name, parser.getDoubleValue());
                    } else {
                        writeJsonLong(name, parser.getLongValue());
                    }
                    break;
                case VALUE_NUMBER_FLOAT:
                    writeJsonDouble(name, parser.getDoubleValue());
                    break;
                case VALUE_TRUE:
                case VALUE_FALSE:
                    writeJsonBool(name, parser.getBooleanValue());
                    break;
                default:
                    throw new InvalidDataException("Unsupported JSON token " + token + " for field " + name);
            }
        }
    }

    /**
     * Unwraps the envelope JsonConverter produces with schemas.enable=true:
     * {"schema": {...}, "payload": {...}}. The schema is ignored - the fast path infers
     * types from the JSON itself, exactly as it does for schemaless payloads.
     */
    private void writeJsonEnvelope(com.fasterxml.jackson.core.JsonParser parser) throws java.io.IOException {
        boolean payloadSeen = false;
        while (parser.nextToken() != com.fasterxml.jackson.core.JsonToken.END_OBJECT) {
            String field = parser.currentName();
            com.fasterxml.jackson.core.JsonToken token = parser.nextToken();
            if ("payload".equals(field)) {
                if (token != com.fasterxml.jackson.core.JsonToken.START_OBJECT) {
                    throw new InvalidDataException("Envelope payload must be an object");
                }
                writeJsonObject(parser, config.getValuePrefix(), 1);
                payloadSeen = true;
            } else {
                parser.skipChildren();
            }
        }
        if (!payloadSeen) {
            throw new InvalidDataException("value.format=json_envelope but the record has no payload field");
        }
    }

    private java.util.List<Object> readJsonList(com.fasterxml.jackson.core.JsonParser parser, int depth) throws java.io.IOException {
        if (depth > MAX_JSON_DEPTH) {
            throw new InvalidDataException("JSON nesting deeper than " + MAX_JSON_DEPTH + " is not supported");
        }
        java.util.List<Object> list = new ArrayList<>();
        for (;;) {
            com.fasterxml.jackson.core.JsonToken token = parser.nextToken();
            if (token == null) {
                throw new InvalidDataException("Unterminated JSON array");
            }
            switch (token) {
                case END_ARRAY:
                    return list;
                case START_ARRAY:
                    list.add(readJsonList(parser, depth + 1));
                    break;
                case START_OBJECT:
                    // Objects are not valid array elements, but the decision to skip or fail
                    // belongs to skip.unsupported.types. Hand a map to the shared array code
                    // so it applies exactly the same rule as the converted path.
                    parser.skipChildren();
                    list.add(new java.util.HashMap<>());
                    break;
                case VALUE_NULL:
                    list.add(null);
                    break;
                case VALUE_STRING:
                    list.add(parser.getText());
                    break;
                case VALUE_NUMBER_INT:
                    if (parser.getNumberType() == com.fasterxml.jackson.core.JsonParser.NumberType.BIG_INTEGER) {
                        list.add(parser.getDoubleValue());
                    } else {
                        list.add(parser.getLongValue());
                    }
                    break;
                case VALUE_NUMBER_FLOAT:
                    list.add(parser.getDoubleValue());
                    break;
                case VALUE_TRUE:
                case VALUE_FALSE:
                    list.add(parser.getBooleanValue());
                    break;
                default:
                    throw new InvalidDataException("Unsupported JSON token in array: " + token);
            }
        }
    }

    private void readDesignatedTimestamp(com.fasterxml.jackson.core.JsonParser parser,
                                        com.fasterxml.jackson.core.JsonToken token) throws java.io.IOException {
        switch (token) {
            case VALUE_STRING:
                timestampColumnValue = parseToMicros(parser.getText());
                return;
            case VALUE_NUMBER_INT:
                long raw = parser.getLongValue();
                timestampColumnValue = TimestampHelper.getTimestampUnits(timestampUnits, raw).toMicros(raw);
                return;
            case VALUE_NULL:
                throw new InvalidDataException("Timestamp column value cannot be null");
            default:
                throw new InvalidDataException("Unsupported timestamp column type: " + token);
        }
    }

    private boolean isDesignatedRawField(String name) {
        return timestampColumnName != null && timestampColumnName.equals(name);
    }

    private void writeJsonString(String name, String value) {
        if (isDesignatedRawField(name)) {
            timestampColumnValue = parseToMicros(value);
            return;
        }
        String actualName = sanitizeName(name);
        if (symbolColumns.contains(actualName)) {
            sender.symbol(actualName, value);
        } else if (stringTimestampColumns.contains(actualName)) {
            sender.timestampColumn(actualName, parseToMicros(value), ChronoUnit.MICROS);
        } else {
            sender.stringColumn(actualName, value);
        }
    }

    private void writeJsonLong(String name, long value) {
        if (isDesignatedRawField(name)) {
            timestampColumnValue = TimestampHelper.getTimestampUnits(timestampUnits, value).toMicros(value);
            return;
        }
        String actualName = sanitizeName(name);
        if (symbolColumns.contains(actualName)) {
            sender.symbol(actualName, String.valueOf(value));
        } else if (doubleColumns.contains(actualName)) {
            sender.doubleColumn(actualName, (double) value);
        } else {
            sender.longColumn(actualName, value);
        }
    }

    private void writeJsonDouble(String name, double value) {
        String actualName = sanitizeName(name);
        if (symbolColumns.contains(actualName)) {
            sender.symbol(actualName, String.valueOf(value));
        } else {
            sender.doubleColumn(actualName, value);
        }
    }

    private void writeJsonBool(String name, boolean value) {
        String actualName = sanitizeName(name);
        if (symbolColumns.contains(actualName)) {
            sender.symbol(actualName, String.valueOf(value));
        } else {
            sender.boolColumn(actualName, value);
        }
    }

    void setSender(Sender sender) {
        this.sender = sender;
    }

    boolean handle(SinkRecord record) {
        if (rawJson) {
            Object value = record.value();
            if (value == null) {
                return false; // tombstone
            }
            if (!(value instanceof byte[])) {
                throw new InvalidDataException("value.format=json requires value.converter="
                        + "org.apache.kafka.connect.converters.ByteArrayConverter, got " + value.getClass().getName());
            }
            return handleRawJson(record, (byte[]) value);
        }
        assert timestampColumnValue == Long.MIN_VALUE;

        // Clear composed timestamp values from any previous failed record
        if (composedTimestampValues != null) {
            Arrays.fill(composedTimestampValues, null);
        }

        Object recordValue = record.value();
        if (recordValue == null) {
            // ignore tombstones
            return false;
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

            if (composedTimestampFields != null) {
                composedBuffer.reset();
                for (int i = 0; i < composedTimestampValues.length; i++) {
                    if (composedTimestampValues[i] == null) {
                        throw new InvalidDataException("Missing composed timestamp field: " + composedTimestampFields[i]);
                    }
                    composedBuffer.add(composedTimestampValues[i]);
                    composedTimestampValues[i] = null;
                }
                try {
                    timestampColumnValue = dataFormat.parse(composedBuffer, DateLocaleFactory.EN_LOCALE);
                } catch (NumericException e) {
                    throw new InvalidDataException("Cannot parse composed timestamp: " + composedBuffer
                            + " with the configured format '" + config.getTimestampFormat() + "'", e);
                }
            }
        } catch (InvalidDataException | LineSenderException ex) {
            // the handler is reused for every record: a row that failed after its
            // designated timestamp was parsed must not hand it to the next record
            timestampColumnValue = Long.MIN_VALUE;
            if (cancelPartialRow && partialRecord) {
                sender.cancelRow();
            }
            if (ex instanceof LineSenderException && wrapSenderErrors) {
                throw new InvalidDataException("object contains invalid data", ex);
            }
            throw ex;
        }

        if (kafkaTimestampsEnabled) {
            timestampColumnValue = TimeUnit.MILLISECONDS.toMicros(record.timestamp());
        }

        if (timestampColumnValue == Long.MIN_VALUE) {
            sender.atNow();
        } else {
            try {
                sender.at(timestampColumnValue, ChronoUnit.MICROS);
            } finally {
                timestampColumnValue = Long.MIN_VALUE;
            }
        }
        return true;
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

        if (composedTimestampFields != null) {
            for (int i = 0; i < composedTimestampFields.length; i++) {
                if (composedTimestampFields[i].equals(name)) {
                    if (value == null) {
                        throw new InvalidDataException("Composed timestamp field '" + name + "' cannot be null");
                    }
                    composedTimestampValues[i] = value.toString();
                    return;
                }
            }
        }

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
        if (!symbolColumns.isEmpty()) {
            String symbolName = sanitizeName(name.isEmpty() ? fallbackName : name);
            if (symbolColumns.contains(symbolName)) {
                CharSequence symbolValue = symbolText(schema, value);
                if (symbolValue != null) {
                    sender.symbol(symbolName, symbolValue);
                    return;
                }
                // Not a scalar the sender could have written: fall through and let the normal
                // path flatten it or reject it, exactly as if it were not named in `symbols`.
            }
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
            return TimeUnit.MILLISECONDS.toMicros(((java.util.Date) value).getTime());
        }
        if (value instanceof String) {
            log.debug("Timestamp column value is a string");
            return parseToMicros((String) value);
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
        return inputUnit.toMicros(longValue);
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
            return dataFormat.parse(timestamp, DateLocaleFactory.EN_LOCALE);
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

    /**
     * The legacy transports route symbols inside the sender - that is, after a value has been
     * converted to the physical type the column would otherwise hold - and stringify that.
     * QWP routes symbols here instead, so it has to perform the same conversion to store the
     * same text. It matters most for logical date/time types: stringifying the raw
     * {@link java.util.Date} would record whatever the worker's default timezone and locale
     * happen to render, so the same record would land differently on different workers.
     * <p>
     * Returns null when the value is not a scalar the sender could have written, so the caller
     * falls back to the normal path rather than storing something like an array's identity
     * hash as a symbol.
     */
    private CharSequence symbolText(Schema schema, Object value) {
        if (schema != null && schema.name() != null) {
            switch (schema.name()) {
                case "io.debezium.time.MicroTimestamp":
                    return String.valueOf((Long) value);
                case "io.debezium.time.Date":
                    return String.valueOf(Micros.addDays(0, (Integer) value));
                case Timestamp.LOGICAL_NAME:
                case org.apache.kafka.connect.data.Date.LOGICAL_NAME:
                case Time.LOGICAL_NAME:
                    return String.valueOf(((java.util.Date) value).getTime());
                case Decimal.LOGICAL_NAME:
                    return null; // unsupported either way; let onUnsupportedType decide
                default:
                    break;
            }
        }
        if (value instanceof CharSequence) {
            return (CharSequence) value;
        }
        if (value instanceof Float) {
            // the physical path widens to double, and 1.1f prints differently once widened
            return String.valueOf(((Float) value).doubleValue());
        }
        if (value instanceof Number || value instanceof Boolean) {
            return String.valueOf(value);
        }
        if (value instanceof java.util.Date) {
            return String.valueOf(TimeUnit.MILLISECONDS.toMicros(((java.util.Date) value).getTime()));
        }
        return null;
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
                long micros = Micros.addDays(0, i);
                sender.timestampColumn(name, micros, ChronoUnit.MICROS);
                return true;
            case Timestamp.LOGICAL_NAME:
            case org.apache.kafka.connect.data.Date.LOGICAL_NAME:
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

}
