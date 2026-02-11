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

public class StructArrayExplodeTest {

    @Test
    public void testWithSchema_singleMapping() {
        StructArrayExplode<SinkRecord> smt = new StructArrayExplode.Value<>();
        smt.configure(Collections.singletonMap("mappings", "vols:strikes,ivols:strike,ivol"));

        Schema entrySchema = SchemaBuilder.struct()
                .field("strike", Schema.FLOAT64_SCHEMA)
                .field("ivol", Schema.FLOAT64_SCHEMA)
                .build();
        Schema schema = SchemaBuilder.struct()
                .name("com.example.VolSurface")
                .field("symbol", Schema.STRING_SCHEMA)
                .field("vols", SchemaBuilder.array(entrySchema).build())
                .build();

        Struct struct = new Struct(schema)
                .put("symbol", "AAPL")
                .put("vols", Arrays.asList(
                        new Struct(entrySchema).put("strike", 150.0).put("ivol", 0.25),
                        new Struct(entrySchema).put("strike", 160.0).put("ivol", 0.22)
                ));

        SinkRecord original = new SinkRecord("topic", 0, null, null, schema, struct, 0);
        SinkRecord transformed = smt.apply(original);

        Struct result = (Struct) transformed.value();
        Assert.assertEquals("AAPL", result.get("symbol"));

        @SuppressWarnings("unchecked")
        List<Double> strikes = (List<Double>) result.get("strikes");
        Assert.assertEquals(Arrays.asList(150.0, 160.0), strikes);

        @SuppressWarnings("unchecked")
        List<Double> ivols = (List<Double>) result.get("ivols");
        Assert.assertEquals(Arrays.asList(0.25, 0.22), ivols);

        // source field should be gone from the new schema
        Assert.assertNull(transformed.valueSchema().field("vols"));
        Assert.assertNotNull(transformed.valueSchema().field("strikes"));
        Assert.assertNotNull(transformed.valueSchema().field("ivols"));
        Assert.assertNotNull(transformed.valueSchema().field("symbol"));

        smt.close();
    }

    @Test
    public void testWithSchema_multipleMappings() {
        StructArrayExplode<SinkRecord> smt = new StructArrayExplode.Value<>();
        smt.configure(Collections.singletonMap("mappings",
                "buy_entries:bid_prices,bid_sizes:px,size;sell_entries:ask_prices,ask_sizes:px,size"));

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
        List<Double> bidPrices = (List<Double>) result.get("bid_prices");
        Assert.assertEquals(Arrays.asList(10.0, 5.1), bidPrices);

        @SuppressWarnings("unchecked")
        List<Double> bidSizes = (List<Double>) result.get("bid_sizes");
        Assert.assertEquals(Arrays.asList(1123.0, 92.0), bidSizes);

        @SuppressWarnings("unchecked")
        List<Double> askPrices = (List<Double>) result.get("ask_prices");
        Assert.assertEquals(Collections.singletonList(11.0), askPrices);

        @SuppressWarnings("unchecked")
        List<Double> askSizes = (List<Double>) result.get("ask_sizes");
        Assert.assertEquals(Collections.singletonList(500.0), askSizes);

        smt.close();
    }

    @Test
    public void testWithSchema_singleStructField() {
        StructArrayExplode<SinkRecord> smt = new StructArrayExplode.Value<>();
        smt.configure(Collections.singletonMap("mappings", "vols:strikes:strike"));

        Schema entrySchema = SchemaBuilder.struct()
                .field("strike", Schema.FLOAT64_SCHEMA)
                .build();
        Schema schema = SchemaBuilder.struct()
                .field("vols", SchemaBuilder.array(entrySchema).build())
                .build();

        Struct struct = new Struct(schema)
                .put("vols", Arrays.asList(
                        new Struct(entrySchema).put("strike", 150.0),
                        new Struct(entrySchema).put("strike", 160.0)
                ));

        SinkRecord original = new SinkRecord("topic", 0, null, null, schema, struct, 0);
        SinkRecord transformed = smt.apply(original);

        Struct result = (Struct) transformed.value();
        @SuppressWarnings("unchecked")
        List<Double> strikes = (List<Double>) result.get("strikes");
        Assert.assertEquals(Arrays.asList(150.0, 160.0), strikes);

        Assert.assertNull(transformed.valueSchema().field("vols"));
        Assert.assertNotNull(transformed.valueSchema().field("strikes"));

        smt.close();
    }

    @Test
    public void testWithSchema_manyStructFields() {
        StructArrayExplode<SinkRecord> smt = new StructArrayExplode.Value<>();
        smt.configure(Collections.singletonMap("mappings",
                "vols:strikes,ivols,svols,vol_offsets,deltas,vegas:strike,ivol,svol,volOffset,delta,vega"));

        Schema entrySchema = SchemaBuilder.struct()
                .field("strike", Schema.FLOAT64_SCHEMA)
                .field("ivol", Schema.FLOAT64_SCHEMA)
                .field("svol", Schema.FLOAT64_SCHEMA)
                .field("volOffset", Schema.FLOAT64_SCHEMA)
                .field("delta", Schema.FLOAT64_SCHEMA)
                .field("vega", Schema.FLOAT64_SCHEMA)
                .build();
        Schema schema = SchemaBuilder.struct()
                .field("vols", SchemaBuilder.array(entrySchema).build())
                .build();

        Struct struct = new Struct(schema)
                .put("vols", Arrays.asList(
                        new Struct(entrySchema)
                                .put("strike", 150.0).put("ivol", 0.25).put("svol", 0.20)
                                .put("volOffset", 0.05).put("delta", 0.55).put("vega", 0.10),
                        new Struct(entrySchema)
                                .put("strike", 160.0).put("ivol", 0.22).put("svol", 0.18)
                                .put("volOffset", 0.04).put("delta", 0.45).put("vega", 0.08)
                ));

        SinkRecord original = new SinkRecord("topic", 0, null, null, schema, struct, 0);
        SinkRecord transformed = smt.apply(original);

        Struct result = (Struct) transformed.value();
        Assert.assertEquals(Arrays.asList(150.0, 160.0), result.get("strikes"));
        Assert.assertEquals(Arrays.asList(0.25, 0.22), result.get("ivols"));
        Assert.assertEquals(Arrays.asList(0.20, 0.18), result.get("svols"));
        Assert.assertEquals(Arrays.asList(0.05, 0.04), result.get("vol_offsets"));
        Assert.assertEquals(Arrays.asList(0.55, 0.45), result.get("deltas"));
        Assert.assertEquals(Arrays.asList(0.10, 0.08), result.get("vegas"));

        // All 6 target columns in schema, source field gone
        Assert.assertNull(transformed.valueSchema().field("vols"));
        Assert.assertNotNull(transformed.valueSchema().field("strikes"));
        Assert.assertNotNull(transformed.valueSchema().field("ivols"));
        Assert.assertNotNull(transformed.valueSchema().field("svols"));
        Assert.assertNotNull(transformed.valueSchema().field("vol_offsets"));
        Assert.assertNotNull(transformed.valueSchema().field("deltas"));
        Assert.assertNotNull(transformed.valueSchema().field("vegas"));

        smt.close();
    }

    @Test
    public void testWithSchema_outputSchemaType() {
        StructArrayExplode<SinkRecord> smt = new StructArrayExplode.Value<>();
        smt.configure(Collections.singletonMap("mappings", "vols:strikes,ivols:strike,ivol"));

        Schema entrySchema = SchemaBuilder.struct()
                .field("strike", Schema.FLOAT64_SCHEMA)
                .field("ivol", Schema.FLOAT64_SCHEMA)
                .build();
        Schema schema = SchemaBuilder.struct()
                .field("vols", SchemaBuilder.array(entrySchema).build())
                .build();

        Struct struct = new Struct(schema)
                .put("vols", Arrays.asList(
                        new Struct(entrySchema).put("strike", 150.0).put("ivol", 0.25)
                ));

        SinkRecord original = new SinkRecord("topic", 0, null, null, schema, struct, 0);
        SinkRecord transformed = smt.apply(original);

        // Verify 1D: ARRAY(FLOAT64), not 2D ARRAY(ARRAY(FLOAT64))
        Schema strikesSchema = transformed.valueSchema().field("strikes").schema();
        Assert.assertEquals(Schema.Type.ARRAY, strikesSchema.type());
        Assert.assertEquals(Schema.Type.FLOAT64, strikesSchema.valueSchema().type());

        Schema ivolsSchema = transformed.valueSchema().field("ivols").schema();
        Assert.assertEquals(Schema.Type.ARRAY, ivolsSchema.type());
        Assert.assertEquals(Schema.Type.FLOAT64, ivolsSchema.valueSchema().type());

        smt.close();
    }

    @Test
    public void testWithSchema_missingSourceField() {
        StructArrayExplode<SinkRecord> smt = new StructArrayExplode.Value<>();
        smt.configure(Collections.singletonMap("mappings",
                "buy_entries:bid_prices,bid_sizes:px,size;sell_entries:ask_prices,ask_sizes:px,size"));

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
        Assert.assertNotNull(transformed.valueSchema().field("bid_prices"));
        Assert.assertNotNull(transformed.valueSchema().field("bid_sizes"));
        // sell_entries was missing from schema, so ask columns should not be in output schema
        Assert.assertNull(transformed.valueSchema().field("ask_prices"));
        Assert.assertNull(transformed.valueSchema().field("ask_sizes"));

        @SuppressWarnings("unchecked")
        List<Double> bidPrices = (List<Double>) result.get("bid_prices");
        Assert.assertEquals(Collections.singletonList(10.0), bidPrices);

        @SuppressWarnings("unchecked")
        List<Double> bidSizes = (List<Double>) result.get("bid_sizes");
        Assert.assertEquals(Collections.singletonList(1123.0), bidSizes);

        smt.close();
    }

    @Test
    public void testWithSchema_targetCollidesWithExisting() {
        StructArrayExplode<SinkRecord> smt = new StructArrayExplode.Value<>();
        // Target field "strikes" collides with an existing field in the schema
        smt.configure(Collections.singletonMap("mappings", "vols:strikes,ivols:strike,ivol"));

        Schema entrySchema = SchemaBuilder.struct()
                .field("strike", Schema.FLOAT64_SCHEMA)
                .field("ivol", Schema.FLOAT64_SCHEMA)
                .build();
        Schema schema = SchemaBuilder.struct()
                .field("symbol", Schema.STRING_SCHEMA)
                .field("strikes", Schema.STRING_SCHEMA) // existing field with same name as target
                .field("vols", SchemaBuilder.array(entrySchema).build())
                .build();

        Struct struct = new Struct(schema)
                .put("symbol", "AAPL")
                .put("strikes", "old_value")
                .put("vols", Arrays.asList(
                        new Struct(entrySchema).put("strike", 150.0).put("ivol", 0.25)
                ));

        SinkRecord original = new SinkRecord("topic", 0, null, null, schema, struct, 0);
        SinkRecord transformed = smt.apply(original);

        Struct result = (Struct) transformed.value();
        Assert.assertEquals("AAPL", result.get("symbol"));

        // strikes should now be the 1D array, not the old string value
        @SuppressWarnings("unchecked")
        List<Double> strikes = (List<Double>) result.get("strikes");
        Assert.assertEquals(Collections.singletonList(150.0), strikes);

        // Verify the schema: strikes field should be ARRAY(FLOAT64), not STRING
        Schema strikesSchema = transformed.valueSchema().field("strikes").schema();
        Assert.assertEquals(Schema.Type.ARRAY, strikesSchema.type());
        Assert.assertEquals(Schema.Type.FLOAT64, strikesSchema.valueSchema().type());

        smt.close();
    }

    @Test
    public void testWithSchema_nullSourceArrayValue() {
        StructArrayExplode<SinkRecord> smt = new StructArrayExplode.Value<>();
        smt.configure(Collections.singletonMap("mappings", "vols:strikes,ivols:strike,ivol"));

        Schema entrySchema = SchemaBuilder.struct()
                .field("strike", Schema.FLOAT64_SCHEMA)
                .field("ivol", Schema.FLOAT64_SCHEMA)
                .build();
        Schema schema = SchemaBuilder.struct()
                .field("symbol", Schema.STRING_SCHEMA)
                .field("vols", SchemaBuilder.array(entrySchema).optional().build())
                .build();

        Struct struct = new Struct(schema)
                .put("symbol", "AAPL")
                .put("vols", null);

        SinkRecord original = new SinkRecord("topic", 0, null, null, schema, struct, 0);
        SinkRecord transformed = smt.apply(original);

        Struct result = (Struct) transformed.value();
        Assert.assertEquals("AAPL", result.get("symbol"));
        Assert.assertNull(result.get("strikes"));
        Assert.assertNull(result.get("ivols"));
        // Target fields should exist in schema but be optional
        Assert.assertNotNull(transformed.valueSchema().field("strikes"));
        Assert.assertTrue(transformed.valueSchema().field("strikes").schema().isOptional());
        Assert.assertNotNull(transformed.valueSchema().field("ivols"));
        Assert.assertTrue(transformed.valueSchema().field("ivols").schema().isOptional());

        smt.close();
    }

    @Test
    public void testWithSchema_emptySourceArray() {
        StructArrayExplode<SinkRecord> smt = new StructArrayExplode.Value<>();
        smt.configure(Collections.singletonMap("mappings", "vols:strikes,ivols:strike,ivol"));

        Schema entrySchema = SchemaBuilder.struct()
                .field("strike", Schema.FLOAT64_SCHEMA)
                .field("ivol", Schema.FLOAT64_SCHEMA)
                .build();
        Schema schema = SchemaBuilder.struct()
                .field("symbol", Schema.STRING_SCHEMA)
                .field("vols", SchemaBuilder.array(entrySchema).build())
                .build();

        Struct struct = new Struct(schema)
                .put("symbol", "AAPL")
                .put("vols", Collections.emptyList());

        SinkRecord original = new SinkRecord("topic", 0, null, null, schema, struct, 0);
        SinkRecord transformed = smt.apply(original);

        Struct result = (Struct) transformed.value();
        Assert.assertEquals("AAPL", result.get("symbol"));
        // Empty source array is skipped — target fields are null
        Assert.assertNull(result.get("strikes"));
        Assert.assertNull(result.get("ivols"));
        // Target fields should still exist in schema as optional
        Assert.assertNotNull(transformed.valueSchema().field("strikes"));
        Assert.assertTrue(transformed.valueSchema().field("strikes").schema().isOptional());
        Assert.assertNotNull(transformed.valueSchema().field("ivols"));
        Assert.assertTrue(transformed.valueSchema().field("ivols").schema().isOptional());

        smt.close();
    }

    @Test
    public void testWithSchema_schemaMetadataPreserved() {
        StructArrayExplode<SinkRecord> smt = new StructArrayExplode.Value<>();
        smt.configure(Collections.singletonMap("mappings", "vols:strikes,ivols:strike,ivol"));

        Schema entrySchema = SchemaBuilder.struct()
                .field("strike", Schema.FLOAT64_SCHEMA)
                .field("ivol", Schema.FLOAT64_SCHEMA)
                .build();
        Schema schema = SchemaBuilder.struct()
                .name("com.example.VolSurface")
                .version(3)
                .doc("A vol surface snapshot")
                .parameter("custom.key", "custom.value")
                .field("symbol", Schema.STRING_SCHEMA)
                .field("vols", SchemaBuilder.array(entrySchema).build())
                .build();

        Struct struct = new Struct(schema)
                .put("symbol", "AAPL")
                .put("vols", Arrays.asList(
                        new Struct(entrySchema).put("strike", 150.0).put("ivol", 0.25)
                ));

        SinkRecord original = new SinkRecord("topic", 0, null, null, schema, struct, 0);
        SinkRecord transformed = smt.apply(original);

        Schema resultSchema = transformed.valueSchema();
        Assert.assertEquals("com.example.VolSurface", resultSchema.name());
        Assert.assertEquals(Integer.valueOf(3), resultSchema.version());
        Assert.assertEquals("A vol surface snapshot", resultSchema.doc());
        Assert.assertEquals("custom.value", resultSchema.parameters().get("custom.key"));

        smt.close();
    }

    @Test
    public void testWithSchema_nestedStructAsNonMappedField() {
        StructArrayExplode<SinkRecord> smt = new StructArrayExplode.Value<>();
        smt.configure(Collections.singletonMap("mappings", "vols:strikes,ivols:strike,ivol"));

        Schema metadataSchema = SchemaBuilder.struct()
                .field("source", Schema.STRING_SCHEMA)
                .field("seq", Schema.INT64_SCHEMA)
                .build();
        Schema entrySchema = SchemaBuilder.struct()
                .field("strike", Schema.FLOAT64_SCHEMA)
                .field("ivol", Schema.FLOAT64_SCHEMA)
                .build();
        Schema schema = SchemaBuilder.struct()
                .field("symbol", Schema.STRING_SCHEMA)
                .field("metadata", metadataSchema)
                .field("vols", SchemaBuilder.array(entrySchema).build())
                .build();

        Struct metadata = new Struct(metadataSchema)
                .put("source", "exchange1")
                .put("seq", 42L);
        Struct struct = new Struct(schema)
                .put("symbol", "AAPL")
                .put("metadata", metadata)
                .put("vols", Arrays.asList(
                        new Struct(entrySchema).put("strike", 150.0).put("ivol", 0.25),
                        new Struct(entrySchema).put("strike", 160.0).put("ivol", 0.22)
                ));

        SinkRecord original = new SinkRecord("topic", 0, null, null, schema, struct, 0);
        SinkRecord transformed = smt.apply(original);

        Struct result = (Struct) transformed.value();
        Assert.assertEquals("AAPL", result.get("symbol"));

        // Nested struct should be preserved
        Struct resultMetadata = (Struct) result.get("metadata");
        Assert.assertEquals("exchange1", resultMetadata.get("source"));
        Assert.assertEquals(42L, resultMetadata.get("seq"));

        // Exploded fields should be correct
        @SuppressWarnings("unchecked")
        List<Double> strikes = (List<Double>) result.get("strikes");
        Assert.assertEquals(Arrays.asList(150.0, 160.0), strikes);

        @SuppressWarnings("unchecked")
        List<Double> ivols = (List<Double>) result.get("ivols");
        Assert.assertEquals(Arrays.asList(0.25, 0.22), ivols);

        // Schema should have metadata as a struct field
        Assert.assertNotNull(transformed.valueSchema().field("metadata"));
        Assert.assertEquals(Schema.Type.STRUCT, transformed.valueSchema().field("metadata").schema().type());

        smt.close();
    }

    @Test
    public void testWithSchema_stringFieldsCoerced() {
        StructArrayExplode<SinkRecord> smt = new StructArrayExplode.Value<>();
        smt.configure(Collections.singletonMap("mappings", "vols:strikes,ivols:strike,ivol"));

        Schema entrySchema = SchemaBuilder.struct()
                .field("strike", Schema.STRING_SCHEMA)
                .field("ivol", Schema.STRING_SCHEMA)
                .build();
        Schema schema = SchemaBuilder.struct()
                .field("symbol", Schema.STRING_SCHEMA)
                .field("vols", SchemaBuilder.array(entrySchema).build())
                .build();

        Struct struct = new Struct(schema)
                .put("symbol", "AAPL")
                .put("vols", Arrays.asList(
                        new Struct(entrySchema).put("strike", "150.0").put("ivol", "0.25"),
                        new Struct(entrySchema).put("strike", "160.0").put("ivol", "0.22")
                ));

        SinkRecord original = new SinkRecord("topic", 0, null, null, schema, struct, 0);
        SinkRecord transformed = smt.apply(original);

        Struct result = (Struct) transformed.value();
        @SuppressWarnings("unchecked")
        List<Double> strikes = (List<Double>) result.get("strikes");
        Assert.assertEquals(Arrays.asList(150.0, 160.0), strikes);

        @SuppressWarnings("unchecked")
        List<Double> ivols = (List<Double>) result.get("ivols");
        Assert.assertEquals(Arrays.asList(0.25, 0.22), ivols);

        smt.close();
    }

    // ---- Schemaless mode tests ----

    @Test
    public void testSchemaless() {
        StructArrayExplode<SinkRecord> smt = new StructArrayExplode.Value<>();
        smt.configure(Collections.singletonMap("mappings",
                "buy_entries:bid_prices,bid_sizes:px,size;sell_entries:ask_prices,ask_sizes:px,size"));

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
        List<Double> bidPrices = (List<Double>) result.get("bid_prices");
        Assert.assertEquals(Arrays.asList(10.0, 5.1), bidPrices);

        @SuppressWarnings("unchecked")
        List<Double> bidSizes = (List<Double>) result.get("bid_sizes");
        Assert.assertEquals(Arrays.asList(1123.0, 92.0), bidSizes);

        @SuppressWarnings("unchecked")
        List<Double> askPrices = (List<Double>) result.get("ask_prices");
        Assert.assertEquals(Arrays.asList(11.0, 12.5), askPrices);

        @SuppressWarnings("unchecked")
        List<Double> askSizes = (List<Double>) result.get("ask_sizes");
        Assert.assertEquals(Arrays.asList(500.0, 300.0), askSizes);

        smt.close();
    }

    @Test
    public void testSchemaless_missingSourceFieldSkipped() {
        StructArrayExplode<SinkRecord> smt = new StructArrayExplode.Value<>();
        smt.configure(Collections.singletonMap("mappings",
                "buy_entries:bid_prices,bid_sizes:px,size;sell_entries:ask_prices,ask_sizes:px,size"));

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
        Assert.assertNotNull(result.get("bid_prices"));
        Assert.assertNotNull(result.get("bid_sizes"));
        // sell_entries was missing, so ask columns should not be in the output
        Assert.assertNull(result.get("ask_prices"));
        Assert.assertNull(result.get("ask_sizes"));

        smt.close();
    }

    @Test
    public void testSchemaless_emptySourceRemovesCollidingTarget() {
        StructArrayExplode<SinkRecord> smt = new StructArrayExplode.Value<>();
        smt.configure(Collections.singletonMap("mappings", "vols:strikes,ivols:strike,ivol"));

        Map<String, Object> value = new LinkedHashMap<>();
        value.put("symbol", "AAPL");
        value.put("strikes", "old_scalar"); // collides with target column name
        value.put("vols", Collections.emptyList());

        SinkRecord original = new SinkRecord("topic", 0, null, null, null, value, 0);
        SinkRecord transformed = smt.apply(original);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) transformed.value();
        Assert.assertEquals("AAPL", result.get("symbol"));
        // Colliding scalar must be removed even though source array is empty
        Assert.assertNull(result.get("strikes"));
        Assert.assertNull(result.get("ivols"));
        Assert.assertFalse(result.containsKey("vols"));

        smt.close();
    }

    @Test
    public void testSchemaless_stringValues() {
        StructArrayExplode<SinkRecord> smt = new StructArrayExplode.Value<>();
        smt.configure(Collections.singletonMap("mappings", "vols:strikes,ivols:strike,ivol"));

        Map<String, Object> value = new LinkedHashMap<>();
        value.put("symbol", "AAPL");
        value.put("vols", Arrays.asList(
                mapOf("strike", "150.0", "ivol", "0.25"),
                mapOf("strike", "160.0", "ivol", "0.22")
        ));

        SinkRecord original = new SinkRecord("topic", 0, null, null, null, value, 0);
        SinkRecord transformed = smt.apply(original);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) transformed.value();
        @SuppressWarnings("unchecked")
        List<Double> strikes = (List<Double>) result.get("strikes");
        Assert.assertEquals(Arrays.asList(150.0, 160.0), strikes);

        @SuppressWarnings("unchecked")
        List<Double> ivols = (List<Double>) result.get("ivols");
        Assert.assertEquals(Arrays.asList(0.25, 0.22), ivols);

        smt.close();
    }

    @Test
    public void testSchemaless_mixedNumericAndStringValues() {
        StructArrayExplode<SinkRecord> smt = new StructArrayExplode.Value<>();
        smt.configure(Collections.singletonMap("mappings", "entries:prices,sizes:px,size"));

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
        List<Double> prices = (List<Double>) result.get("prices");
        Assert.assertEquals(Arrays.asList(10.5, 5.1), prices);

        @SuppressWarnings("unchecked")
        List<Double> sizes = (List<Double>) result.get("sizes");
        Assert.assertEquals(Arrays.asList(1123.0, 92.0), sizes);

        smt.close();
    }

    @Test(expected = ConnectException.class)
    public void testSchemaless_nonNumericStringThrows() {
        StructArrayExplode<SinkRecord> smt = new StructArrayExplode.Value<>();
        smt.configure(Collections.singletonMap("mappings", "entries:prices:px"));

        Map<String, Object> value = new LinkedHashMap<>();
        value.put("entries", Collections.singletonList(
                Collections.singletonMap("px", "not_a_number")
        ));

        SinkRecord original = new SinkRecord("topic", 0, null, null, null, value, 0);
        smt.apply(original);
    }

    // ---- Type coercion tests ----

    @Test
    public void testNumberCoercion_intAndFloat() {
        StructArrayExplode<SinkRecord> smt = new StructArrayExplode.Value<>();
        smt.configure(Collections.singletonMap("mappings", "entries:prices,sizes:px,size"));

        // Use FLOAT32 and INT32 fields instead of FLOAT64
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
        List<Double> prices = (List<Double>) result.get("prices");
        // float -> double
        Assert.assertEquals(10.5, prices.get(0), 0.001);
        Assert.assertEquals(5.1, prices.get(1), 0.001);

        @SuppressWarnings("unchecked")
        List<Double> sizes = (List<Double>) result.get("sizes");
        // int -> double
        Assert.assertEquals(100.0, sizes.get(0), 0.001);
        Assert.assertEquals(200.0, sizes.get(1), 0.001);

        smt.close();
    }

    // ---- Error/validation tests ----

    @Test(expected = ConnectException.class)
    public void testWithSchema_nullStructFieldValue() {
        StructArrayExplode<SinkRecord> smt = new StructArrayExplode.Value<>();
        smt.configure(Collections.singletonMap("mappings", "vols:strikes,ivols:strike,ivol"));

        Schema entrySchema = SchemaBuilder.struct()
                .field("strike", Schema.OPTIONAL_FLOAT64_SCHEMA)
                .field("ivol", Schema.OPTIONAL_FLOAT64_SCHEMA)
                .build();
        Schema schema = SchemaBuilder.struct()
                .field("symbol", Schema.STRING_SCHEMA)
                .field("vols", SchemaBuilder.array(entrySchema).build())
                .build();

        Struct struct = new Struct(schema)
                .put("symbol", "AAPL")
                .put("vols", Arrays.asList(
                        new Struct(entrySchema).put("strike", null).put("ivol", 0.25)
                ));

        SinkRecord original = new SinkRecord("topic", 0, null, null, schema, struct, 0);
        smt.apply(original);
    }

    @Test(expected = ConnectException.class)
    public void testSchemaless_nullStructFieldValue() {
        StructArrayExplode<SinkRecord> smt = new StructArrayExplode.Value<>();
        smt.configure(Collections.singletonMap("mappings", "vols:strikes,ivols:strike,ivol"));

        Map<String, Object> value = new LinkedHashMap<>();
        value.put("symbol", "AAPL");
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("strike", null);
        entry.put("ivol", 0.25);
        value.put("vols", Collections.singletonList(entry));

        SinkRecord original = new SinkRecord("topic", 0, null, null, null, value, 0);
        smt.apply(original);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidMappingFormat_noColon() {
        StructArrayExplode<SinkRecord> smt = new StructArrayExplode.Value<>();
        smt.configure(Collections.singletonMap("mappings", "only_two_parts:target"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidMappingFormat_mismatchedLengths() {
        StructArrayExplode<SinkRecord> smt = new StructArrayExplode.Value<>();
        // 2 targets but 3 struct fields
        smt.configure(Collections.singletonMap("mappings", "vols:strikes,ivols:strike,ivol,svol"));
    }

    @Test(expected = ConfigException.class)
    public void testNullMappingsConfig() {
        StructArrayExplode<SinkRecord> smt = new StructArrayExplode.Value<>();
        smt.configure(Collections.singletonMap("mappings", null));
    }

    @Test(expected = ConfigException.class)
    public void testBlankMappingsConfig() {
        StructArrayExplode<SinkRecord> smt = new StructArrayExplode.Value<>();
        smt.configure(Collections.singletonMap("mappings", ""));
    }

    @Test(expected = ConfigException.class)
    public void testOnlySemicolonsMappingsConfig() {
        StructArrayExplode<SinkRecord> smt = new StructArrayExplode.Value<>();
        smt.configure(Collections.singletonMap("mappings", ";;;"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDuplicateTargetColumnNames() {
        StructArrayExplode<SinkRecord> smt = new StructArrayExplode.Value<>();
        // "strikes" appears as target in both mappings
        smt.configure(Collections.singletonMap("mappings",
                "vols:strikes,ivols:strike,ivol;vols2:strikes,svols:strike,svol"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDuplicateSourceField() {
        StructArrayExplode<SinkRecord> smt = new StructArrayExplode.Value<>();
        smt.configure(Collections.singletonMap("mappings",
                "entries:prices:px;entries:sizes:size"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTargetCollidesWithSourceField() {
        StructArrayExplode<SinkRecord> smt = new StructArrayExplode.Value<>();
        // target "bids" in mapping 1 equals source "bids" in mapping 2
        smt.configure(Collections.singletonMap("mappings",
                "raw_bids:bids,bid_sizes:price,size;bids:ask_prices,ask_sizes:price,size"));
    }

    @Test
    public void testSelfReplacementAllowed() {
        // target "entries" replaces its own source "entries" — valid pattern
        StructArrayExplode<SinkRecord> smt = new StructArrayExplode.Value<>();
        smt.configure(Collections.singletonMap("mappings", "entries:entries:px"));

        Schema entrySchema = SchemaBuilder.struct()
                .field("px", Schema.FLOAT64_SCHEMA)
                .build();
        Schema schema = SchemaBuilder.struct()
                .field("entries", SchemaBuilder.array(entrySchema).build())
                .build();

        Struct struct = new Struct(schema)
                .put("entries", Arrays.asList(
                        new Struct(entrySchema).put("px", 1.0),
                        new Struct(entrySchema).put("px", 2.0)
                ));

        SinkRecord original = new SinkRecord("topic", 0, null, null, schema, struct, 0);
        SinkRecord transformed = smt.apply(original);

        Struct result = (Struct) transformed.value();
        @SuppressWarnings("unchecked")
        List<Double> entries = (List<Double>) result.get("entries");
        Assert.assertEquals(Arrays.asList(1.0, 2.0), entries);

        // Schema should be ARRAY(FLOAT64), not the original ARRAY(STRUCT)
        Schema entriesSchema = transformed.valueSchema().field("entries").schema();
        Assert.assertEquals(Schema.Type.ARRAY, entriesSchema.type());
        Assert.assertEquals(Schema.Type.FLOAT64, entriesSchema.valueSchema().type());

        smt.close();
    }

    // ---- Structural tests ----

    @Test
    public void testNullValuePassthrough() {
        StructArrayExplode<SinkRecord> smt = new StructArrayExplode.Value<>();
        smt.configure(Collections.singletonMap("mappings", "vols:strikes,ivols:strike,ivol"));

        SinkRecord original = new SinkRecord("topic", 0, null, null, null, null, 0);
        SinkRecord transformed = smt.apply(original);
        Assert.assertNull(transformed.value());

        smt.close();
    }

    @Test
    public void testKeyVariant_operatesOnKey() {
        StructArrayExplode<SinkRecord> smt = new StructArrayExplode.Key<>();
        smt.configure(Collections.singletonMap("mappings", "entries:prices,sizes:px,size"));

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
        Assert.assertNotNull(transformed.keySchema().field("prices"));
        Assert.assertNotNull(transformed.keySchema().field("sizes"));
        Assert.assertNull(transformed.keySchema().field("entries"));

        @SuppressWarnings("unchecked")
        List<Double> prices = (List<Double>) resultKey.get("prices");
        Assert.assertEquals(Collections.singletonList(1.0), prices);

        @SuppressWarnings("unchecked")
        List<Double> sizes = (List<Double>) resultKey.get("sizes");
        Assert.assertEquals(Collections.singletonList(2.0), sizes);

        // Value should be untouched
        Struct resultValue = (Struct) transformed.value();
        Assert.assertEquals("untouched", resultValue.get("other"));
        Assert.assertSame(valueSchema, transformed.valueSchema());

        smt.close();
    }

    @Test
    public void testSchemaCaching() {
        StructArrayExplode<SinkRecord> smt = new StructArrayExplode.Value<>();
        smt.configure(Collections.singletonMap("mappings", "entries:prices:px"));

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

    private static Map<String, Object> mapOf(String k1, Object v1, String k2, Object v2) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(k1, v1);
        map.put(k2, v2);
        return map;
    }
}
