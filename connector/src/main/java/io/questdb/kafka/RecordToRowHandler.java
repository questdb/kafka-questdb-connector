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
    }

    void setSender(Sender sender) {
        this.sender = sender;
    }

    boolean handle(SinkRecord record) {
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
        } catch (InvalidDataException ex) {
            if (cancelPartialRow && partialRecord) {
                sender.cancelRow();
            }
            throw ex;
        } catch (LineSenderException ex) {
            if (cancelPartialRow && partialRecord) {
                sender.cancelRow();
            }
            if (wrapSenderErrors) {
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
                sender.symbol(symbolName, value instanceof CharSequence ? (CharSequence) value : String.valueOf(value));
                return;
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
