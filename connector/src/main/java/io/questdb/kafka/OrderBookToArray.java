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

public abstract class OrderBookToArray<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String MAPPINGS_CONFIG = "mappings";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(MAPPINGS_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    "Semicolon-separated mappings: sourceField:targetField:structField1,structField2");

    private List<FieldMapping> mappings;
    private Map<String, FieldMapping> sourceFieldMap;
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
                        + "'. Expected format: sourceField:targetField:structField1,structField2");
            }
            String sourceField = parts[0].trim();
            String targetField = parts[1].trim();
            String[] structFields = parts[2].trim().split(",");
            for (int i = 0; i < structFields.length; i++) {
                structFields[i] = structFields[i].trim();
            }
            FieldMapping mapping = new FieldMapping(sourceField, targetField, structFields);
            mappings.add(mapping);
            sourceFieldMap.put(sourceField, mapping);
            targetFieldNames.add(targetField);
        }
        if (mappings.isEmpty()) {
            throw new ConfigException(MAPPINGS_CONFIG, raw, "At least one mapping is required");
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

        // Transpose mapped fields
        for (FieldMapping mapping : mappings) {
            if (originalSchema.field(mapping.sourceField) == null) {
                continue;
            }
            List<Struct> entries = originalStruct.getArray(mapping.sourceField);
            if (entries == null || entries.isEmpty()) {
                continue;
            }
            List<List<Double>> transposed = transpose(entries, mapping.structFields, mapping.sourceField);
            newStruct.put(mapping.targetField, transposed);
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

        // Add target fields as optional ARRAY(ARRAY(FLOAT64)), only if source field exists.
        // Target is always optional: null/empty source arrays are skipped at runtime
        // (QuestDB does not support zero-length arrays).
        Schema targetArraySchema = SchemaBuilder.array(
                SchemaBuilder.array(Schema.FLOAT64_SCHEMA).build()
        ).optional().build();
        for (FieldMapping mapping : mappings) {
            if (originalSchema.field(mapping.sourceField) != null) {
                builder.field(mapping.targetField, targetArraySchema);
            }
        }

        Schema newSchema = builder.build();
        schemaCache.put(originalSchema, newSchema);
        return newSchema;
    }

    private List<List<Double>> transpose(List<Struct> entries, String[] structFields, String sourceField) {
        List<List<Double>> result = new ArrayList<>(structFields.length);
        for (String structField : structFields) {
            List<Double> row = new ArrayList<>(entries.size());
            for (Struct entry : entries) {
                Object val = entry.get(structField);
                row.add(toDouble(val, structField, sourceField));
            }
            result.add(row);
        }
        return result;
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

        for (FieldMapping mapping : mappings) {
            List<Map<String, Object>> entries = (List<Map<String, Object>>) result.remove(mapping.sourceField);
            if (entries == null || entries.isEmpty()) {
                continue;
            }
            List<List<Double>> transposed = new ArrayList<>(mapping.structFields.length);
            for (String structField : mapping.structFields) {
                List<Double> row = new ArrayList<>(entries.size());
                for (Map<String, Object> entry : entries) {
                    Object val = entry.get(structField);
                    row.add(toDouble(val, structField, mapping.sourceField));
                }
                transposed.add(row);
            }
            result.put(mapping.targetField, transposed);
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

    private static final class FieldMapping {
        final String sourceField;
        final String targetField;
        final String[] structFields;

        FieldMapping(String sourceField, String targetField, String[] structFields) {
            this.sourceField = sourceField;
            this.targetField = targetField;
            this.structFields = structFields;
        }
    }

    public static class Key<R extends ConnectRecord<R>> extends OrderBookToArray<R> {
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

    public static class Value<R extends ConnectRecord<R>> extends OrderBookToArray<R> {
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
