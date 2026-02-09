package io.questdb.kafka;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class OrderBookToArrayTest {

    @Test
    public void testWithSchema_singleMapping() {
        OrderBookToArray<SinkRecord> smt = new OrderBookToArray.Value<>();
        smt.configure(Collections.singletonMap("mappings", "buy_entries:bids:px,size"));

        Schema entrySchema = SchemaBuilder.struct()
                .field("px", Schema.FLOAT64_SCHEMA)
                .field("size", Schema.FLOAT64_SCHEMA)
                .build();
        Schema schema = SchemaBuilder.struct()
                .name("com.example.OrderBook")
                .field("symbol", Schema.STRING_SCHEMA)
                .field("buy_entries", SchemaBuilder.array(entrySchema).build())
                .build();

        Struct struct = new Struct(schema)
                .put("symbol", "AAPL")
                .put("buy_entries", Arrays.asList(
                        new Struct(entrySchema).put("px", 10.0).put("size", 1123.0),
                        new Struct(entrySchema).put("px", 5.1).put("size", 92.0)
                ));

        SinkRecord original = new SinkRecord("topic", 0, null, null, schema, struct, 0);
        SinkRecord transformed = smt.apply(original);

        Struct result = (Struct) transformed.value();
        Assert.assertEquals("AAPL", result.get("symbol"));

        @SuppressWarnings("unchecked")
        List<List<Double>> bids = (List<List<Double>>) result.get("bids");
        Assert.assertEquals(Arrays.asList(10.0, 5.1), bids.get(0));
        Assert.assertEquals(Arrays.asList(1123.0, 92.0), bids.get(1));

        // source field should be gone from the new schema
        Assert.assertNull(transformed.valueSchema().field("buy_entries"));
        Assert.assertNotNull(transformed.valueSchema().field("bids"));
        Assert.assertNotNull(transformed.valueSchema().field("symbol"));

        smt.close();
    }

    @Test
    public void testWithSchema_multipleMappings() {
        OrderBookToArray<SinkRecord> smt = new OrderBookToArray.Value<>();
        smt.configure(Collections.singletonMap("mappings", "buy_entries:bids:px,size;sell_entries:asks:px,size"));

        Schema entrySchema = SchemaBuilder.struct()
                .field("px", Schema.FLOAT64_SCHEMA)
                .field("size", Schema.FLOAT64_SCHEMA)
                .build();
        Schema schema = SchemaBuilder.struct()
                .field("symbol", Schema.STRING_SCHEMA)
                .field("buy_entries", SchemaBuilder.array(entrySchema).build())
                .field("sell_entries", SchemaBuilder.array(entrySchema).build())
                .build();

        Struct struct = new Struct(schema)
                .put("symbol", "AAPL")
                .put("buy_entries", Arrays.asList(
                        new Struct(entrySchema).put("px", 10.0).put("size", 1123.0),
                        new Struct(entrySchema).put("px", 5.1).put("size", 92.0)
                ))
                .put("sell_entries", Arrays.asList(
                        new Struct(entrySchema).put("px", 11.0).put("size", 500.0)
                ));

        SinkRecord original = new SinkRecord("topic", 0, null, null, schema, struct, 0);
        SinkRecord transformed = smt.apply(original);

        Struct result = (Struct) transformed.value();
        Assert.assertEquals("AAPL", result.get("symbol"));

        @SuppressWarnings("unchecked")
        List<List<Double>> bids = (List<List<Double>>) result.get("bids");
        Assert.assertEquals(Arrays.asList(10.0, 5.1), bids.get(0));
        Assert.assertEquals(Arrays.asList(1123.0, 92.0), bids.get(1));

        @SuppressWarnings("unchecked")
        List<List<Double>> asks = (List<List<Double>>) result.get("asks");
        Assert.assertEquals(Collections.singletonList(11.0), asks.get(0));
        Assert.assertEquals(Collections.singletonList(500.0), asks.get(1));

        smt.close();
    }

    @Test
    public void testSchemaless() {
        OrderBookToArray<SinkRecord> smt = new OrderBookToArray.Value<>();
        smt.configure(Collections.singletonMap("mappings", "buy_entries:bids:px,size;sell_entries:asks:px,size"));

        Map<String, Object> value = new LinkedHashMap<>();
        value.put("symbol", "AAPL");
        value.put("buy_entries", Arrays.asList(
                mapOf("px", 10.0, "size", 1123.0),
                mapOf("px", 5.1, "size", 92.0)
        ));
        value.put("sell_entries", Arrays.asList(
                mapOf("px", 11.0, "size", 500.0),
                mapOf("px", 12.5, "size", 300.0)
        ));

        SinkRecord original = new SinkRecord("topic", 0, null, null, null, value, 0);
        SinkRecord transformed = smt.apply(original);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) transformed.value();
        Assert.assertEquals("AAPL", result.get("symbol"));
        Assert.assertNull(result.get("buy_entries"));
        Assert.assertNull(result.get("sell_entries"));

        @SuppressWarnings("unchecked")
        List<List<Double>> bids = (List<List<Double>>) result.get("bids");
        Assert.assertEquals(Arrays.asList(10.0, 5.1), bids.get(0));
        Assert.assertEquals(Arrays.asList(1123.0, 92.0), bids.get(1));

        @SuppressWarnings("unchecked")
        List<List<Double>> asks = (List<List<Double>>) result.get("asks");
        Assert.assertEquals(Arrays.asList(11.0, 12.5), asks.get(0));
        Assert.assertEquals(Arrays.asList(500.0, 300.0), asks.get(1));

        smt.close();
    }

    @Test
    public void testNullValuePassthrough() {
        OrderBookToArray<SinkRecord> smt = new OrderBookToArray.Value<>();
        smt.configure(Collections.singletonMap("mappings", "buy_entries:bids:px,size"));

        SinkRecord original = new SinkRecord("topic", 0, null, null, null, null, 0);
        SinkRecord transformed = smt.apply(original);
        Assert.assertNull(transformed.value());

        smt.close();
    }

    @Test
    public void testKeyVariant_operatesOnKey() {
        OrderBookToArray<SinkRecord> smt = new OrderBookToArray.Key<>();
        smt.configure(Collections.singletonMap("mappings", "entries:data:px,size"));

        Schema entrySchema = SchemaBuilder.struct()
                .field("px", Schema.FLOAT64_SCHEMA)
                .field("size", Schema.FLOAT64_SCHEMA)
                .build();
        Schema keySchema = SchemaBuilder.struct()
                .field("id", Schema.STRING_SCHEMA)
                .field("entries", SchemaBuilder.array(entrySchema).build())
                .build();
        Schema valueSchema = SchemaBuilder.struct()
                .field("other", Schema.STRING_SCHEMA)
                .build();

        Struct key = new Struct(keySchema)
                .put("id", "k1")
                .put("entries", Arrays.asList(
                        new Struct(entrySchema).put("px", 1.0).put("size", 2.0)
                ));
        Struct value = new Struct(valueSchema).put("other", "untouched");

        SinkRecord original = new SinkRecord("topic", 0, keySchema, key, valueSchema, value, 0);
        SinkRecord transformed = smt.apply(original);

        // Key should be transformed
        Struct resultKey = (Struct) transformed.key();
        Assert.assertEquals("k1", resultKey.get("id"));
        Assert.assertNotNull(transformed.keySchema().field("data"));
        Assert.assertNull(transformed.keySchema().field("entries"));

        @SuppressWarnings("unchecked")
        List<List<Double>> data = (List<List<Double>>) resultKey.get("data");
        Assert.assertEquals(Collections.singletonList(1.0), data.get(0));
        Assert.assertEquals(Collections.singletonList(2.0), data.get(1));

        // Value should be untouched
        Struct resultValue = (Struct) transformed.value();
        Assert.assertEquals("untouched", resultValue.get("other"));
        Assert.assertSame(valueSchema, transformed.valueSchema());

        smt.close();
    }

    @Test
    public void testNumberCoercion_intAndFloat() {
        OrderBookToArray<SinkRecord> smt = new OrderBookToArray.Value<>();
        smt.configure(Collections.singletonMap("mappings", "entries:data:px,size"));

        // Use INT32 and FLOAT32 fields instead of FLOAT64
        Schema entrySchema = SchemaBuilder.struct()
                .field("px", Schema.FLOAT32_SCHEMA)
                .field("size", Schema.INT32_SCHEMA)
                .build();
        Schema schema = SchemaBuilder.struct()
                .field("entries", SchemaBuilder.array(entrySchema).build())
                .build();

        Struct struct = new Struct(schema)
                .put("entries", Arrays.asList(
                        new Struct(entrySchema).put("px", 10.5f).put("size", 100),
                        new Struct(entrySchema).put("px", 5.1f).put("size", 200)
                ));

        SinkRecord original = new SinkRecord("topic", 0, null, null, schema, struct, 0);
        SinkRecord transformed = smt.apply(original);

        Struct result = (Struct) transformed.value();
        @SuppressWarnings("unchecked")
        List<List<Double>> data = (List<List<Double>>) result.get("data");
        // float -> double
        Assert.assertEquals(10.5, data.get(0).get(0), 0.001);
        Assert.assertEquals(5.1, data.get(0).get(1), 0.001);
        // int -> double
        Assert.assertEquals(100.0, data.get(1).get(0), 0.001);
        Assert.assertEquals(200.0, data.get(1).get(1), 0.001);

        smt.close();
    }

    @Test
    public void testSchemaless_missingSourceFieldSkipped() {
        OrderBookToArray<SinkRecord> smt = new OrderBookToArray.Value<>();
        smt.configure(Collections.singletonMap("mappings", "buy_entries:bids:px,size;sell_entries:asks:px,size"));

        Map<String, Object> value = new LinkedHashMap<>();
        value.put("symbol", "AAPL");
        // only buy_entries present, sell_entries missing
        value.put("buy_entries", Arrays.asList(
                mapOf("px", 10.0, "size", 1123.0)
        ));

        SinkRecord original = new SinkRecord("topic", 0, null, null, null, value, 0);
        SinkRecord transformed = smt.apply(original);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) transformed.value();
        Assert.assertEquals("AAPL", result.get("symbol"));
        Assert.assertNotNull(result.get("bids"));
        // sell_entries was missing, so asks should not be in the output
        Assert.assertNull(result.get("asks"));

        smt.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidMappingFormat() {
        OrderBookToArray<SinkRecord> smt = new OrderBookToArray.Value<>();
        smt.configure(Collections.singletonMap("mappings", "only_two_parts:target"));
    }

    @Test(expected = ConfigException.class)
    public void testNullMappingsConfig() {
        OrderBookToArray<SinkRecord> smt = new OrderBookToArray.Value<>();
        smt.configure(Collections.singletonMap("mappings", null));
    }

    @Test(expected = ConfigException.class)
    public void testBlankMappingsConfig() {
        OrderBookToArray<SinkRecord> smt = new OrderBookToArray.Value<>();
        smt.configure(Collections.singletonMap("mappings", ""));
    }

    @Test(expected = ConfigException.class)
    public void testOnlySemicolonsMappingsConfig() {
        OrderBookToArray<SinkRecord> smt = new OrderBookToArray.Value<>();
        smt.configure(Collections.singletonMap("mappings", ";;;"));
    }

    @Test
    public void testSchemaCaching() {
        OrderBookToArray<SinkRecord> smt = new OrderBookToArray.Value<>();
        smt.configure(Collections.singletonMap("mappings", "entries:data:px"));

        Schema entrySchema = SchemaBuilder.struct()
                .field("px", Schema.FLOAT64_SCHEMA)
                .build();
        Schema schema = SchemaBuilder.struct()
                .field("entries", SchemaBuilder.array(entrySchema).build())
                .build();

        Struct struct1 = new Struct(schema)
                .put("entries", Arrays.asList(new Struct(entrySchema).put("px", 1.0)));
        Struct struct2 = new Struct(schema)
                .put("entries", Arrays.asList(new Struct(entrySchema).put("px", 2.0)));

        SinkRecord r1 = new SinkRecord("topic", 0, null, null, schema, struct1, 0);
        SinkRecord r2 = new SinkRecord("topic", 0, null, null, schema, struct2, 1);

        SinkRecord t1 = smt.apply(r1);
        SinkRecord t2 = smt.apply(r2);

        // Same input schema identity should produce same output schema identity
        Assert.assertSame(t1.valueSchema(), t2.valueSchema());

        smt.close();
    }

    @Test(expected = ConnectException.class)
    public void testWithSchema_nullStructFieldValue() {
        OrderBookToArray<SinkRecord> smt = new OrderBookToArray.Value<>();
        smt.configure(Collections.singletonMap("mappings", "buy_entries:bids:px,size"));

        Schema entrySchema = SchemaBuilder.struct()
                .field("px", Schema.OPTIONAL_FLOAT64_SCHEMA)
                .field("size", Schema.OPTIONAL_FLOAT64_SCHEMA)
                .build();
        Schema schema = SchemaBuilder.struct()
                .field("symbol", Schema.STRING_SCHEMA)
                .field("buy_entries", SchemaBuilder.array(entrySchema).build())
                .build();

        Struct struct = new Struct(schema)
                .put("symbol", "AAPL")
                .put("buy_entries", Arrays.asList(
                        new Struct(entrySchema).put("px", null).put("size", 1123.0)
                ));

        SinkRecord original = new SinkRecord("topic", 0, null, null, schema, struct, 0);
        smt.apply(original);
    }

    @Test(expected = ConnectException.class)
    public void testSchemaless_nullStructFieldValue() {
        OrderBookToArray<SinkRecord> smt = new OrderBookToArray.Value<>();
        smt.configure(Collections.singletonMap("mappings", "buy_entries:bids:px,size"));

        Map<String, Object> value = new LinkedHashMap<>();
        value.put("symbol", "AAPL");
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("px", null);
        entry.put("size", 1123.0);
        value.put("buy_entries", Collections.singletonList(entry));

        SinkRecord original = new SinkRecord("topic", 0, null, null, null, value, 0);
        smt.apply(original);
    }

    @Test
    public void testWithSchema_missingSourceField() {
        OrderBookToArray<SinkRecord> smt = new OrderBookToArray.Value<>();
        smt.configure(Collections.singletonMap("mappings", "buy_entries:bids:px,size;sell_entries:asks:px,size"));

        Schema entrySchema = SchemaBuilder.struct()
                .field("px", Schema.FLOAT64_SCHEMA)
                .field("size", Schema.FLOAT64_SCHEMA)
                .build();
        // Schema only has buy_entries, not sell_entries
        Schema schema = SchemaBuilder.struct()
                .field("symbol", Schema.STRING_SCHEMA)
                .field("buy_entries", SchemaBuilder.array(entrySchema).build())
                .build();

        Struct struct = new Struct(schema)
                .put("symbol", "AAPL")
                .put("buy_entries", Arrays.asList(
                        new Struct(entrySchema).put("px", 10.0).put("size", 1123.0)
                ));

        SinkRecord original = new SinkRecord("topic", 0, null, null, schema, struct, 0);
        SinkRecord transformed = smt.apply(original);

        Struct result = (Struct) transformed.value();
        Assert.assertEquals("AAPL", result.get("symbol"));
        Assert.assertNotNull(transformed.valueSchema().field("bids"));
        // sell_entries was missing from schema, so asks should not be in output schema
        Assert.assertNull(transformed.valueSchema().field("asks"));

        @SuppressWarnings("unchecked")
        List<List<Double>> bids = (List<List<Double>>) result.get("bids");
        Assert.assertEquals(Arrays.asList(10.0), bids.get(0));
        Assert.assertEquals(Arrays.asList(1123.0), bids.get(1));

        smt.close();
    }

    @Test
    public void testWithSchema_targetCollidesWithExisting() {
        OrderBookToArray<SinkRecord> smt = new OrderBookToArray.Value<>();
        // Target field "bids" collides with an existing field in the schema
        smt.configure(Collections.singletonMap("mappings", "buy_entries:bids:px,size"));

        Schema entrySchema = SchemaBuilder.struct()
                .field("px", Schema.FLOAT64_SCHEMA)
                .field("size", Schema.FLOAT64_SCHEMA)
                .build();
        Schema schema = SchemaBuilder.struct()
                .field("symbol", Schema.STRING_SCHEMA)
                .field("bids", Schema.STRING_SCHEMA) // existing field with same name as target
                .field("buy_entries", SchemaBuilder.array(entrySchema).build())
                .build();

        Struct struct = new Struct(schema)
                .put("symbol", "AAPL")
                .put("bids", "old_value")
                .put("buy_entries", Arrays.asList(
                        new Struct(entrySchema).put("px", 10.0).put("size", 1123.0)
                ));

        SinkRecord original = new SinkRecord("topic", 0, null, null, schema, struct, 0);
        SinkRecord transformed = smt.apply(original);

        Struct result = (Struct) transformed.value();
        Assert.assertEquals("AAPL", result.get("symbol"));

        // bids should now be the transposed array, not the old string value
        @SuppressWarnings("unchecked")
        List<List<Double>> bids = (List<List<Double>>) result.get("bids");
        Assert.assertEquals(Arrays.asList(10.0), bids.get(0));
        Assert.assertEquals(Arrays.asList(1123.0), bids.get(1));

        // Verify the schema: bids field should be ARRAY(ARRAY(FLOAT64)), not STRING
        Schema bidsSchema = transformed.valueSchema().field("bids").schema();
        Assert.assertEquals(Schema.Type.ARRAY, bidsSchema.type());
        Assert.assertEquals(Schema.Type.ARRAY, bidsSchema.valueSchema().type());
        Assert.assertEquals(Schema.Type.FLOAT64, bidsSchema.valueSchema().valueSchema().type());

        smt.close();
    }

    @Test
    public void testWithSchema_nullSourceArrayValue() {
        OrderBookToArray<SinkRecord> smt = new OrderBookToArray.Value<>();
        smt.configure(Collections.singletonMap("mappings", "buy_entries:bids:px,size"));

        Schema entrySchema = SchemaBuilder.struct()
                .field("px", Schema.FLOAT64_SCHEMA)
                .field("size", Schema.FLOAT64_SCHEMA)
                .build();
        Schema schema = SchemaBuilder.struct()
                .field("symbol", Schema.STRING_SCHEMA)
                .field("buy_entries", SchemaBuilder.array(entrySchema).optional().build())
                .build();

        Struct struct = new Struct(schema)
                .put("symbol", "AAPL")
                .put("buy_entries", null);

        SinkRecord original = new SinkRecord("topic", 0, null, null, schema, struct, 0);
        SinkRecord transformed = smt.apply(original);

        Struct result = (Struct) transformed.value();
        Assert.assertEquals("AAPL", result.get("symbol"));
        Assert.assertNull(result.get("bids"));
        // Target field should exist in schema but be optional
        Assert.assertNotNull(transformed.valueSchema().field("bids"));
        Assert.assertTrue(transformed.valueSchema().field("bids").schema().isOptional());

        smt.close();
    }

    @Test
    public void testWithSchema_emptySourceArray() {
        OrderBookToArray<SinkRecord> smt = new OrderBookToArray.Value<>();
        smt.configure(Collections.singletonMap("mappings", "buy_entries:bids:px,size"));

        Schema entrySchema = SchemaBuilder.struct()
                .field("px", Schema.FLOAT64_SCHEMA)
                .field("size", Schema.FLOAT64_SCHEMA)
                .build();
        Schema schema = SchemaBuilder.struct()
                .field("symbol", Schema.STRING_SCHEMA)
                .field("buy_entries", SchemaBuilder.array(entrySchema).build())
                .build();

        Struct struct = new Struct(schema)
                .put("symbol", "AAPL")
                .put("buy_entries", Collections.emptyList());

        SinkRecord original = new SinkRecord("topic", 0, null, null, schema, struct, 0);
        SinkRecord transformed = smt.apply(original);

        Struct result = (Struct) transformed.value();
        Assert.assertEquals("AAPL", result.get("symbol"));
        // Empty source array is skipped — target field is null
        Assert.assertNull(result.get("bids"));
        // Target field should still exist in schema as optional
        Assert.assertNotNull(transformed.valueSchema().field("bids"));
        Assert.assertTrue(transformed.valueSchema().field("bids").schema().isOptional());

        smt.close();
    }

    @Test
    public void testWithSchema_schemaMetadataPreserved() {
        OrderBookToArray<SinkRecord> smt = new OrderBookToArray.Value<>();
        smt.configure(Collections.singletonMap("mappings", "buy_entries:bids:px,size"));

        Schema entrySchema = SchemaBuilder.struct()
                .field("px", Schema.FLOAT64_SCHEMA)
                .field("size", Schema.FLOAT64_SCHEMA)
                .build();
        Schema schema = SchemaBuilder.struct()
                .name("com.example.OrderBook")
                .version(3)
                .doc("An order book snapshot")
                .parameter("custom.key", "custom.value")
                .field("symbol", Schema.STRING_SCHEMA)
                .field("buy_entries", SchemaBuilder.array(entrySchema).build())
                .build();

        Struct struct = new Struct(schema)
                .put("symbol", "AAPL")
                .put("buy_entries", Arrays.asList(
                        new Struct(entrySchema).put("px", 10.0).put("size", 1123.0)
                ));

        SinkRecord original = new SinkRecord("topic", 0, null, null, schema, struct, 0);
        SinkRecord transformed = smt.apply(original);

        Schema resultSchema = transformed.valueSchema();
        Assert.assertEquals("com.example.OrderBook", resultSchema.name());
        Assert.assertEquals(Integer.valueOf(3), resultSchema.version());
        Assert.assertEquals("An order book snapshot", resultSchema.doc());
        Assert.assertEquals("custom.value", resultSchema.parameters().get("custom.key"));

        smt.close();
    }

    @Test
    public void testWithSchema_nestedStructAsNonMappedField() {
        OrderBookToArray<SinkRecord> smt = new OrderBookToArray.Value<>();
        smt.configure(Collections.singletonMap("mappings", "buy_entries:bids:px,size"));

        Schema metadataSchema = SchemaBuilder.struct()
                .field("source", Schema.STRING_SCHEMA)
                .field("seq", Schema.INT64_SCHEMA)
                .build();
        Schema entrySchema = SchemaBuilder.struct()
                .field("px", Schema.FLOAT64_SCHEMA)
                .field("size", Schema.FLOAT64_SCHEMA)
                .build();
        Schema schema = SchemaBuilder.struct()
                .field("symbol", Schema.STRING_SCHEMA)
                .field("metadata", metadataSchema)
                .field("buy_entries", SchemaBuilder.array(entrySchema).build())
                .build();

        Struct metadata = new Struct(metadataSchema)
                .put("source", "exchange1")
                .put("seq", 42L);
        Struct struct = new Struct(schema)
                .put("symbol", "AAPL")
                .put("metadata", metadata)
                .put("buy_entries", Arrays.asList(
                        new Struct(entrySchema).put("px", 10.0).put("size", 1123.0),
                        new Struct(entrySchema).put("px", 5.1).put("size", 92.0)
                ));

        SinkRecord original = new SinkRecord("topic", 0, null, null, schema, struct, 0);
        SinkRecord transformed = smt.apply(original);

        Struct result = (Struct) transformed.value();
        Assert.assertEquals("AAPL", result.get("symbol"));

        // Nested struct should be preserved
        Struct resultMetadata = (Struct) result.get("metadata");
        Assert.assertEquals("exchange1", resultMetadata.get("source"));
        Assert.assertEquals(42L, resultMetadata.get("seq"));

        // Transposed field should be correct
        @SuppressWarnings("unchecked")
        List<List<Double>> bids = (List<List<Double>>) result.get("bids");
        Assert.assertEquals(Arrays.asList(10.0, 5.1), bids.get(0));
        Assert.assertEquals(Arrays.asList(1123.0, 92.0), bids.get(1));

        // Schema should have metadata as a struct field
        Assert.assertNotNull(transformed.valueSchema().field("metadata"));
        Assert.assertEquals(Schema.Type.STRUCT, transformed.valueSchema().field("metadata").schema().type());

        smt.close();
    }

    @Test
    public void testSchemaless_stringValues() {
        OrderBookToArray<SinkRecord> smt = new OrderBookToArray.Value<>();
        smt.configure(Collections.singletonMap("mappings", "buy_entries:bids:px,size"));

        Map<String, Object> value = new LinkedHashMap<>();
        value.put("symbol", "AAPL");
        value.put("buy_entries", Arrays.asList(
                mapOf("px", "10.5", "size", "1123.0"),
                mapOf("px", "5.1", "size", "92")
        ));

        SinkRecord original = new SinkRecord("topic", 0, null, null, null, value, 0);
        SinkRecord transformed = smt.apply(original);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) transformed.value();
        @SuppressWarnings("unchecked")
        List<List<Double>> bids = (List<List<Double>>) result.get("bids");
        Assert.assertEquals(Arrays.asList(10.5, 5.1), bids.get(0));
        Assert.assertEquals(Arrays.asList(1123.0, 92.0), bids.get(1));

        smt.close();
    }

    @Test
    public void testSchemaless_mixedNumericAndStringValues() {
        OrderBookToArray<SinkRecord> smt = new OrderBookToArray.Value<>();
        smt.configure(Collections.singletonMap("mappings", "entries:data:px,size"));

        Map<String, Object> value = new LinkedHashMap<>();
        value.put("entries", Arrays.asList(
                mapOf("px", 10.5, "size", "1123.0"),   // numeric px, string size
                mapOf("px", "5.1", "size", 92.0)       // string px, numeric size
        ));

        SinkRecord original = new SinkRecord("topic", 0, null, null, null, value, 0);
        SinkRecord transformed = smt.apply(original);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) transformed.value();
        @SuppressWarnings("unchecked")
        List<List<Double>> data = (List<List<Double>>) result.get("data");
        Assert.assertEquals(Arrays.asList(10.5, 5.1), data.get(0));
        Assert.assertEquals(Arrays.asList(1123.0, 92.0), data.get(1));

        smt.close();
    }

    @Test(expected = ConnectException.class)
    public void testSchemaless_nonNumericStringThrows() {
        OrderBookToArray<SinkRecord> smt = new OrderBookToArray.Value<>();
        smt.configure(Collections.singletonMap("mappings", "entries:data:px"));

        Map<String, Object> value = new LinkedHashMap<>();
        value.put("entries", Collections.singletonList(
                Collections.singletonMap("px", "not_a_number")
        ));

        SinkRecord original = new SinkRecord("topic", 0, null, null, null, value, 0);
        smt.apply(original);
    }

    @Test
    public void testWithSchema_stringFieldsCoerced() {
        OrderBookToArray<SinkRecord> smt = new OrderBookToArray.Value<>();
        smt.configure(Collections.singletonMap("mappings", "buy_entries:bids:px,size"));

        Schema entrySchema = SchemaBuilder.struct()
                .field("px", Schema.STRING_SCHEMA)
                .field("size", Schema.STRING_SCHEMA)
                .build();
        Schema schema = SchemaBuilder.struct()
                .field("symbol", Schema.STRING_SCHEMA)
                .field("buy_entries", SchemaBuilder.array(entrySchema).build())
                .build();

        Struct struct = new Struct(schema)
                .put("symbol", "AAPL")
                .put("buy_entries", Arrays.asList(
                        new Struct(entrySchema).put("px", "10.5").put("size", "1123.0"),
                        new Struct(entrySchema).put("px", "5.1").put("size", "92")
                ));

        SinkRecord original = new SinkRecord("topic", 0, null, null, schema, struct, 0);
        SinkRecord transformed = smt.apply(original);

        Struct result = (Struct) transformed.value();
        @SuppressWarnings("unchecked")
        List<List<Double>> bids = (List<List<Double>>) result.get("bids");
        Assert.assertEquals(Arrays.asList(10.5, 5.1), bids.get(0));
        Assert.assertEquals(Arrays.asList(1123.0, 92.0), bids.get(1));

        smt.close();
    }

    private static Map<String, Object> mapOf(String k1, Object v1, String k2, Object v2) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(k1, v1);
        map.put(k2, v2);
        return map;
    }
}