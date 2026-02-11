package io.questdb.kafka;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class StructArrayExplode<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String MAPPINGS_CONFIG = "mappings";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(MAPPINGS_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    "Semicolon-separated mappings: sourceField:targetCol1,targetCol2:structField1,structField2");

    private List<SourceMapping> mappings;
    private Map<String, SourceMapping> sourceFieldMap;
    private Set<String> targetFieldNames;
    private final IdentityHashMap<Schema, Schema> schemaCache = new IdentityHashMap<>();

    protected abstract Schema operatingSchema(R record);
    protected abstract Object operatingValue(R record);
    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    @Override
    public void configure(Map<String, ?> configs) {
        String raw = (String) configs.get(MAPPINGS_CONFIG);
        if (raw == null || raw.trim().isEmpty()) {
            throw new ConfigException(MAPPINGS_CONFIG, raw, "At least one mapping is required");
        }
        mappings = new ArrayList<>();
        sourceFieldMap = new HashMap<>();
        targetFieldNames = new HashSet<>();
        for (String entry : raw.split(";")) {
            entry = entry.trim();
            if (entry.isEmpty()) {
                continue;
            }
            String[] parts = entry.split(":");
            if (parts.length != 3) {
                throw new IllegalArgumentException("Invalid mapping entry: '" + entry
                        + "'. Expected format: sourceField:targetCol1,targetCol2:structField1,structField2");
            }
            String sourceField = parts[0].trim();
            String[] targetColumns = parts[1].trim().split(",");
            String[] structFields = parts[2].trim().split(",");
            for (int i = 0; i < targetColumns.length; i++) {
                targetColumns[i] = targetColumns[i].trim();
            }
            for (int i = 0; i < structFields.length; i++) {
                structFields[i] = structFields[i].trim();
            }
            if (targetColumns.length != structFields.length) {
                throw new IllegalArgumentException("Invalid mapping entry: '" + entry
                        + "'. Number of target columns (" + targetColumns.length
                        + ") must match number of struct fields (" + structFields.length + ")");
            }
            List<ColumnMapping> columnMappings = new ArrayList<>(targetColumns.length);
            for (int i = 0; i < targetColumns.length; i++) {
                if (!targetFieldNames.add(targetColumns[i])) {
                    throw new IllegalArgumentException("Duplicate target column name: '" + targetColumns[i] + "'");
                }
                columnMappings.add(new ColumnMapping(structFields[i], targetColumns[i]));
            }
            SourceMapping mapping = new SourceMapping(sourceField, columnMappings);
            mappings.add(mapping);
            if (sourceFieldMap.put(sourceField, mapping) != null) {
                throw new IllegalArgumentException("Duplicate source field: '" + sourceField
                        + "'. Combine all target columns into a single mapping entry");
            }
        }
        if (mappings.isEmpty()) {
            throw new ConfigException(MAPPINGS_CONFIG, raw, "At least one mapping is required");
        }
        for (SourceMapping mapping : mappings) {
            for (ColumnMapping cm : mapping.columnMappings) {
                SourceMapping conflicting = sourceFieldMap.get(cm.targetColumn);
                if (conflicting != null && conflicting != mapping) {
                    throw new IllegalArgumentException("Target column '" + cm.targetColumn
                            + "' conflicts with source field of another mapping");
                }
            }
        }
    }

    @Override
    public R apply(R record) {
        Object value = operatingValue(record);
        if (value == null) {
            return record;
        }
        Schema schema = operatingSchema(record);
        if (schema != null) {
            return applyWithSchema(record, schema, (Struct) value);
        } else {
            return applySchemaless(record, value);
        }
    }

    private R applyWithSchema(R record, Schema originalSchema, Struct originalStruct) {
        Schema newSchema = getOrBuildSchema(originalSchema);
        Struct newStruct = new Struct(newSchema);

        // Copy non-mapped fields (skip source fields and target fields that will be replaced)
        for (Field field : originalSchema.fields()) {
            if (!sourceFieldMap.containsKey(field.name()) && !targetFieldNames.contains(field.name())) {
                newStruct.put(field.name(), originalStruct.get(field));
            }
        }

        // Explode mapped fields into separate 1D columns
        for (SourceMapping mapping : mappings) {
            if (originalSchema.field(mapping.sourceField) == null) {
                continue;
            }
            List<Struct> entries = originalStruct.getArray(mapping.sourceField);
            if (entries == null || entries.isEmpty()) {
                continue;
            }
            for (ColumnMapping cm : mapping.columnMappings) {
                List<Double> column = new ArrayList<>(entries.size());
                for (Struct entry : entries) {
                    Object val = entry.get(cm.structField);
                    column.add(toDouble(val, cm.structField, mapping.sourceField));
                }
                newStruct.put(cm.targetColumn, column);
            }
        }

        return newRecord(record, newSchema, newStruct);
    }

    private Schema getOrBuildSchema(Schema originalSchema) {
        Schema cached = schemaCache.get(originalSchema);
        if (cached != null) {
            return cached;
        }

        SchemaBuilder builder = SchemaBuilder.struct();
        if (originalSchema.name() != null) {
            builder.name(originalSchema.name());
        }
        if (originalSchema.version() != null) {
            builder.version(originalSchema.version());
        }
        if (originalSchema.doc() != null) {
            builder.doc(originalSchema.doc());
        }
        if (originalSchema.parameters() != null) {
            builder.parameters(originalSchema.parameters());
        }
        if (originalSchema.defaultValue() != null) {
            builder.defaultValue(originalSchema.defaultValue());
        }
        if (originalSchema.isOptional()) {
            builder.optional();
        }

        for (Field field : originalSchema.fields()) {
            if (!sourceFieldMap.containsKey(field.name()) && !targetFieldNames.contains(field.name())) {
                builder.field(field.name(), field.schema());
            }
        }

        // Add target fields as optional ARRAY(FLOAT64) (1D), only if source field exists.
        // Target is always optional: null/empty source arrays are skipped at runtime
        // (QuestDB does not support zero-length arrays).
        Schema targetArraySchema = SchemaBuilder.array(Schema.FLOAT64_SCHEMA).optional().build();
        for (SourceMapping mapping : mappings) {
            if (originalSchema.field(mapping.sourceField) != null) {
                for (ColumnMapping cm : mapping.columnMappings) {
                    builder.field(cm.targetColumn, targetArraySchema);
                }
            }
        }

        Schema newSchema = builder.build();
        schemaCache.put(originalSchema, newSchema);
        return newSchema;
    }

    private static double toDouble(Object val, String structField, String sourceField) {
        if (val == null) {
            throw new ConnectException("Null value for field '" + structField + "' in source array '" + sourceField + "'");
        }
        if (val instanceof Number) {
            return ((Number) val).doubleValue();
        }
        if (val instanceof String) {
            try {
                return Double.parseDouble((String) val);
            } catch (NumberFormatException e) {
                throw new ConnectException("Cannot parse '" + val + "' as double for field '"
                        + structField + "' in source array '" + sourceField + "'");
            }
        }
        throw new ConnectException("Unsupported type " + val.getClass().getName() + " for field '"
                + structField + "' in source array '" + sourceField + "'");
    }

    @SuppressWarnings("unchecked")
    private R applySchemaless(R record, Object value) {
        Map<String, Object> original = (Map<String, Object>) value;
        Map<String, Object> result = new LinkedHashMap<>(original);

        for (SourceMapping mapping : mappings) {
            List<Map<String, Object>> entries = (List<Map<String, Object>>) result.remove(mapping.sourceField);
            // Remove colliding target keys before the empty-check so pre-existing
            // scalar values never leak through when the source array is empty/null.
            for (ColumnMapping cm : mapping.columnMappings) {
                result.remove(cm.targetColumn);
            }
            if (entries == null || entries.isEmpty()) {
                continue;
            }
            for (ColumnMapping cm : mapping.columnMappings) {
                List<Double> column = new ArrayList<>(entries.size());
                for (Map<String, Object> entry : entries) {
                    Object val = entry.get(cm.structField);
                    column.add(toDouble(val, cm.structField, mapping.sourceField));
                }
                result.put(cm.targetColumn, column);
            }
        }

        return newRecord(record, null, result);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        schemaCache.clear();
    }

    private static final class ColumnMapping {
        final String structField;
        final String targetColumn;

        ColumnMapping(String structField, String targetColumn) {
            this.structField = structField;
            this.targetColumn = targetColumn;
        }
    }

    private static final class SourceMapping {
        final String sourceField;
        final List<ColumnMapping> columnMappings;

        SourceMapping(String sourceField, List<ColumnMapping> columnMappings) {
            this.sourceField = sourceField;
            this.columnMappings = columnMappings;
        }
    }

    public static class Key<R extends ConnectRecord<R>> extends StructArrayExplode<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(),
                    updatedSchema, updatedValue,
                    record.valueSchema(), record.value(),
                    record.timestamp());
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends StructArrayExplode<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(),
                    record.keySchema(), record.key(),
                    updatedSchema, updatedValue,
                    record.timestamp());
        }
    }
}
