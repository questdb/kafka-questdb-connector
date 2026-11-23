package io.questdb.kafka;

import io.questdb.client.Sender;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * QWP accepts symbols interleaved with other columns, so the configured symbol
 * columns are written straight to the sender from here. ILP does not allow that
 * ("symbols must be written before any other column types"), which is why the
 * legacy transports keep routing them through {@link BufferingSender} and this
 * handler must leave them alone.
 */
class RecordToRowHandlerSymbolTest {

    @Test
    void writesConfiguredSymbolColumnsDirectlyAndInPlace() {
        Recorder recorder = new Recorder();
        RecordToRowHandler handler = newHandler(recorder, true);

        handler.handle(record());

        assertEquals(
                List.of("table(tab)",
                        "doubleColumn(px,1.5)",   // not configured as a symbol
                        "symbol(sym,abc)",        // configured: string value passed through
                        "symbol(num,42)",         // configured: stringified
                        "stringColumn(note,plain)",
                        "atNow()"),
                recorder.calls,
                "symbols must be emitted in place, without reordering the row");
    }

    @Test
    void leavesSymbolRoutingToBufferingSenderOnLegacyTransports() {
        Recorder recorder = new Recorder();
        RecordToRowHandler handler = newHandler(recorder, false);

        handler.handle(record());

        assertEquals(
                List.of("table(tab)",
                        "doubleColumn(px,1.5)",
                        "stringColumn(sym,abc)",  // BufferingSender turns these into symbols
                        "longColumn(num,42)",
                        "stringColumn(note,plain)",
                        "atNow()"),
                recorder.calls);
    }

    /**
     * A symbol must store the text of the value the column would otherwise have held, which is
     * what the legacy transports store because they route symbols after the type conversion.
     * Stringifying the raw Java object would write {@code Date.toString()} - the worker's
     * timezone and locale, not the timestamp - and would print a float at float precision.
     */
    @Test
    void symbolsStoreTheConvertedValueRatherThanTheRawJavaObject() {
        Recorder recorder = new Recorder();
        RecordToRowHandler handler = newHandler(recorder, true, "ts,f,n");

        Schema schema = SchemaBuilder.struct()
                .field("ts", Timestamp.SCHEMA)
                .field("f", Schema.FLOAT32_SCHEMA)
                .field("n", Schema.INT64_SCHEMA)
                .build();
        Struct struct = new Struct(schema)
                .put("ts", new java.util.Date(1_700_000_000_000L))
                .put("f", 1.1f)
                .put("n", 42L);
        handler.handle(new SinkRecord("tab", 0, null, null, schema, struct, 0L));

        assertEquals(
                List.of("table(tab)",
                        "symbol(ts,1700000000000)",     // epoch millis, not "Tue Nov 14 ..."
                        "symbol(f,1.100000023841858)",  // widened to double first
                        "symbol(n,42)",
                        "atNow()"),
                recorder.calls);
    }

    @Test
    void nonScalarValuesNamedAsSymbolsAreNotStringified() {
        Recorder recorder = new Recorder();
        RecordToRowHandler handler = newHandler(recorder, true, "blob");

        Schema schema = SchemaBuilder.struct().field("blob", Schema.BYTES_SCHEMA).build();
        Struct struct = new Struct(schema).put("blob", new byte[]{1, 2, 3});

        // Falls through to the unsupported-type path instead of storing an identity hash,
        // which would make every record its own symbol value.
        assertThrows(InvalidDataException.class,
                () -> handler.handle(new SinkRecord("tab", 0, null, null, schema, struct, 0L)));
    }

    /**
     * Lookup enrichment: the key is read from the record, the looked-up value is stamped as a
     * symbol after the row's own columns, and a record without a mapping is written as is.
     */
    @ParameterizedTest
    @EnumSource(Shape.class)
    void enrichmentStampsLookedUpValueAfterTheRowsOwnColumns(Shape shape) {
        Recorder recorder = new Recorder();
        Map<String, String> props = new HashMap<>();
        props.put("client.conf.string", "ws::addr=localhost:9000;");
        props.put("table", "tab");
        props.put("include.key", "false");
        props.put("enrichment.sql", "select sec_id, ticker from sec_list");
        props.put("enrichment.key.field", "sec_id");
        if (shape == Shape.RAW_JSON) {
            props.put("value.format", "json");
        }
        RecordToRowHandler handler = new RecordToRowHandler(new QuestDBSinkConnectorConfig(props), recorder.proxy(), true, false, true);
        Map<String, String[]> lookup = new HashMap<>();
        lookup.put("42", new String[]{"AAPL"});
        lookup.put("abc", new String[]{"MSFT"});
        lookup.put("42.5", new String[]{"IBM"});
        lookup.put("true", new String[]{"ORCL"});
        handler.setEnrichment(new String[]{"ticker"}, lookup);

        handler.handle(shape.record(42L)); // numeric key: rendered as plain decimal on both sides
        assertEquals(List.of("table(tab)", "longColumn(sec_id,42)", "doubleColumn(px,1.5)", "symbol(ticker,AAPL)", "atNow()"),
                recorder.calls);

        recorder.calls.clear();
        handler.handle(shape.record("abc"));
        assertEquals(List.of("table(tab)", "stringColumn(sec_id,abc)", "doubleColumn(px,1.5)", "symbol(ticker,MSFT)", "atNow()"),
                recorder.calls);

        // every scalar key is captured as its text on every path, so the miss count agrees too
        recorder.calls.clear();
        handler.handle(shape.record(42.5d));
        assertEquals(List.of("table(tab)", "doubleColumn(sec_id,42.5)", "doubleColumn(px,1.5)", "symbol(ticker,IBM)", "atNow()"),
                recorder.calls);
        recorder.calls.clear();
        handler.handle(shape.record(true));
        assertEquals(List.of("table(tab)", "boolColumn(sec_id,true)", "doubleColumn(px,1.5)", "symbol(ticker,ORCL)", "atNow()"),
                recorder.calls);

        recorder.calls.clear();
        handler.handle(shape.record(7L)); // no mapping: the row is written, the column stays null
        assertEquals(List.of("table(tab)", "longColumn(sec_id,7)", "doubleColumn(px,1.5)", "atNow()"), recorder.calls);

        recorder.calls.clear();
        handler.handle(shape.record(null)); // key field absent: neither a hit nor a miss, counted as absent
        assertEquals(List.of("table(tab)", "doubleColumn(px,1.5)", "atNow()"), recorder.calls);

        recorder.calls.clear();
        handler.handle(shape.record(NESTED)); // not a scalar: written as usual, reported as unusable
        assertEquals(List.of("table(tab)", "longColumn(sec_id_a,1)", "doubleColumn(px,1.5)", "atNow()"), recorder.calls);

        handler.handle(shape.record(null)); // absent again: the unusable key before it does not carry over

        try (LogCapture logs = new LogCapture(EnrichmentMisses.class)) {
            handler.reportEnrichmentMisses(true);
            handler.reportEnrichmentMisses(true); // nothing changed since the first report
            assertEquals(List.of(
                    "INFO Enrichment lookup: 1 key(s) have no mapping (1 new, 0 mapped since the previous report)",
                    "DEBUG Enrichment keys newly without a mapping (at most 20 shown): [7]",
                    "WARN 1 record(s) had a value in enrichment.key.field 'sec_id' that cannot be a lookup key;"
                            + " only text, integers, floating-point numbers and booleans can. Those records are not"
                            + " enriched; later ones are logged at DEBUG",
                    "WARN 2 record(s) had no value in enrichment.key.field 'sec_id'; those records are not enriched."
                            + " If every record has that field, check the name: nested names are joined with '_' and"
                            + " start with the value or key prefix. Later ones are logged at DEBUG"), logs.lines);
        }
        handler.handle(shape.record(null));
        try (LogCapture logs = new LogCapture(EnrichmentMisses.class)) {
            handler.reportEnrichmentMisses(true);
            assertEquals(List.of("DEBUG 1 more record(s) had no value in enrichment.key.field 'sec_id'"), logs.lines);
        }
    }

    /**
     * A primitive key or value under an empty prefix is written to a column named 'key' or
     * 'value', and the key field names it the same way. Matching only the traversal name, which
     * is empty there, meant such records were never enriched and never counted as a miss.
     */
    @Test
    void enrichmentKeyFieldMatchesThePrimitiveKeyOrValueByItsColumnName() {
        Map<String, String[]> lookup = new HashMap<>();
        lookup.put("42", new String[]{"AAPL"});
        lookup.put("abc", new String[]{"MSFT"});

        Recorder recorder = new Recorder();
        RecordToRowHandler byValue = enrichedHandler(recorder, "value", Map.of()); // value.prefix defaults to ""
        byValue.setEnrichment(new String[]{"ticker"}, lookup);
        byValue.handle(new SinkRecord("tab", 0, null, null, Schema.INT64_SCHEMA, 42L, 0L));
        assertEquals(List.of("table(tab)", "longColumn(value,42)", "symbol(ticker,AAPL)", "atNow()"), recorder.calls);

        recorder.calls.clear();
        byValue.handle(new SinkRecord("tab", 0, null, null, null, "abc", 0L)); // schemaless
        assertEquals(List.of("table(tab)", "stringColumn(value,abc)", "symbol(ticker,MSFT)", "atNow()"), recorder.calls);

        recorder.calls.clear();
        byValue.handle(new SinkRecord("tab", 0, null, null, Schema.INT64_SCHEMA, 7L, 0L));
        assertEquals(List.of("table(tab)", "longColumn(value,7)", "atNow()"), recorder.calls);
        try (LogCapture logs = new LogCapture(EnrichmentMisses.class)) {
            byValue.reportEnrichmentMisses(true);
            assertEquals(List.of(
                    "INFO Enrichment lookup: 1 key(s) have no mapping (1 new, 0 mapped since the previous report)",
                    "DEBUG Enrichment keys newly without a mapping (at most 20 shown): [7]"), logs.lines);
        }

        // A struct or map value is flattened: no column is named 'value', so the key field is
        // absent - neither a hit nor a miss, and not a key that cannot be looked up either.
        Schema schema = SchemaBuilder.struct().field("px", Schema.FLOAT64_SCHEMA).build();
        recorder.calls.clear();
        byValue.handle(new SinkRecord("tab", 0, null, null, schema, new Struct(schema).put("px", 1.5d), 0L));
        byValue.handle(new SinkRecord("tab", 0, null, null, null, Map.of("px", 1.5d), 0L));
        // a field that is named 'value' is still the key
        byValue.handle(new SinkRecord("tab", 0, null, null, null, Map.of("value", "abc"), 0L));
        assertEquals(List.of("table(tab)", "doubleColumn(px,1.5)", "atNow()",
                "table(tab)", "doubleColumn(px,1.5)", "atNow()",
                "table(tab)", "stringColumn(value,abc)", "symbol(ticker,MSFT)", "atNow()"), recorder.calls);
        try (LogCapture logs = new LogCapture(EnrichmentMisses.class)) {
            byValue.reportEnrichmentMisses(true);
            assertEquals(List.of("WARN 2 record(s) had no value in enrichment.key.field 'value'; those records are"
                    + " not enriched. If every record has that field, check the name: nested names are joined with '_'"
                    + " and start with the value or key prefix. Later ones are logged at DEBUG"), logs.lines);
        }

        // the key path takes the same fallback when key.prefix is emptied, on both value paths
        for (boolean rawJson : new boolean[]{false, true}) {
            recorder.calls.clear();
            Map<String, String> props = new HashMap<>();
            props.put("include.key", "true");
            props.put("key.prefix", "");
            if (rawJson) {
                props.put("value.format", "json");
            }
            RecordToRowHandler byKey = enrichedHandler(recorder, "key", props);
            byKey.setEnrichment(new String[]{"ticker"}, lookup);
            Object value = rawJson ? "{\"px\":1.5}".getBytes(StandardCharsets.UTF_8) : Map.of("px", 1.5d);
            byKey.handle(new SinkRecord("tab", 0, Schema.STRING_SCHEMA, "abc", null, value, 0L));
            assertEquals(List.of("table(tab)", "stringColumn(key,abc)", "doubleColumn(px,1.5)", "symbol(ticker,MSFT)", "atNow()"),
                    recorder.calls, rawJson ? "raw JSON" : "converted");
        }
    }

    /** A record that fails after its key was captured must not hand the key to the next record. */
    @ParameterizedTest
    @EnumSource(Shape.class)
    void aFailedRecordDoesNotLendItsKeyToTheNext(Shape shape) {
        Recorder recorder = new Recorder();
        RecordToRowHandler handler = enrichedHandler(recorder, "sec_id", shape.props());
        handler.setEnrichment(new String[]{"ticker"}, Map.of("42", new String[]{"AAPL"}));

        // sec_id is captured, then a later field is rejected
        assertThrows(InvalidDataException.class, () -> handler.handle(shape.failingAfterKey(42L)));
        recorder.calls.clear();
        handler.handle(shape.record(null)); // no key field

        assertEquals(List.of("table(tab)", "doubleColumn(px,1.5)", "atNow()"), recorder.calls);
    }

    /** The symbols branch writes the column and returns, so the key must be captured before it. */
    @ParameterizedTest
    @EnumSource(Shape.class)
    void aKeyFieldListedInSymbolsIsStillLookedUp(Shape shape) {
        Recorder recorder = new Recorder();
        Map<String, String> props = new HashMap<>(shape.props());
        props.put("symbols", "sec_id");
        RecordToRowHandler handler = enrichedHandler(recorder, "sec_id", props);
        handler.setEnrichment(new String[]{"ticker"}, Map.of("42", new String[]{"AAPL"}, "abc", new String[]{"MSFT"}));

        handler.handle(shape.record("abc"));
        handler.handle(shape.record(42L));

        assertEquals(List.of(
                "table(tab)", "symbol(sec_id,abc)", "doubleColumn(px,1.5)", "symbol(ticker,MSFT)", "atNow()",
                "table(tab)", "symbol(sec_id,42)", "doubleColumn(px,1.5)", "symbol(ticker,AAPL)", "atNow()"),
                recorder.calls);
    }

    /**
     * Each value column the key's lookup row has a value for is stamped; a row with only some
     * values is still a hit. Each lookup brings its own columns, as SELECT * does after ALTER TABLE.
     */
    @Test
    void enrichmentStampsEveryValueColumnTheLookupRowHasAValueFor() {
        Recorder recorder = new Recorder();
        RecordToRowHandler handler = enrichedHandler(recorder, "sec_id", Map.of());
        handler.setEnrichment(new String[]{"ticker"}, Map.of("42", new String[]{"AAPL"}));
        handler.handle(Shape.MAP.record(42L));

        Map<String, String[]> lookup = new HashMap<>();
        lookup.put("42", new String[]{"AAPL", "EQ"});
        lookup.put("7", new String[]{null, "FX"});
        handler.setEnrichment(new String[]{"ticker", "mkt_type"}, lookup);
        handler.handle(Shape.MAP.record(42L));
        handler.handle(Shape.MAP.record(7L));

        assertEquals(List.of(
                "table(tab)", "longColumn(sec_id,42)", "doubleColumn(px,1.5)", "symbol(ticker,AAPL)", "atNow()",
                "table(tab)", "longColumn(sec_id,42)", "doubleColumn(px,1.5)", "symbol(ticker,AAPL)", "symbol(mkt_type,EQ)", "atNow()",
                "table(tab)", "longColumn(sec_id,7)", "doubleColumn(px,1.5)", "symbol(mkt_type,FX)", "atNow()"),
                recorder.calls);
        try (LogCapture logs = new LogCapture(EnrichmentMisses.class)) {
            handler.reportEnrichmentMisses(true);
            assertEquals(List.of(), logs.lines);
        }
    }

    /** The server names an expression without an alias after its text, which is no column name. */
    @Test
    void enrichmentRejectsAValueColumnWithoutAUsableNameAskingForAnAlias() {
        RecordToRowHandler handler = enrichedHandler(new Recorder(), "sec_id", Map.of());

        ConnectException failure = assertThrows(ConnectException.class, () -> handler.setEnrichment(
                new String[]{"price::varchar"}, Map.of("42", new String[]{"1.5"})));

        assertEquals("enrichment.sql value column 'price::varchar' cannot be used as a column name;"
                + " give it an alias in the SQL (... AS another_name)", failure.getMessage());
    }

    /** QuestDB column names ignore case, and a '.' is written as '_'. */
    @Test
    void enrichmentRejectsTwoValueColumnsNamingTheSameColumn() {
        Recorder recorder = new Recorder();
        RecordToRowHandler handler = enrichedHandler(recorder, "sec_id", Map.of());
        handler.setEnrichment(new String[]{"ticker"}, Map.of("42", new String[]{"AAPL"}));

        ConnectException failure = assertThrows(ConnectException.class, () -> handler.setEnrichment(
                new String[]{"mkt.type", "ticker", "MKT_TYPE"}, Map.of("42", new String[]{"EQ", "IBM", "FX"})));

        assertEquals("enrichment.sql value column 'MKT_TYPE' names the same column as value column 'mkt_type';"
                + " rename it in the SQL with an alias (... AS another_name)", failure.getMessage());
        handler.handle(Shape.MAP.record(42L)); // the previous lookup stays
        assertEquals(List.of("table(tab)", "longColumn(sec_id,42)", "doubleColumn(px,1.5)", "symbol(ticker,AAPL)", "atNow()"),
                recorder.calls);
    }

    private static RecordToRowHandler enrichedHandler(Recorder recorder, String keyField, Map<String, String> extra) {
        Map<String, String> props = new HashMap<>();
        props.put("client.conf.string", "ws::addr=localhost:9000;");
        props.put("table", "tab");
        props.put("include.key", "false");
        props.put("enrichment.sql", "select sec_id, ticker from sec_list");
        props.put("enrichment.key.field", keyField);
        props.putAll(extra);
        return new RecordToRowHandler(new QuestDBSinkConnectorConfig(props), recorder.proxy(), true, false, true);
    }

    /** Stands for a key field holding an object, {"a": 1}. */
    private static final Map<String, Object> NESTED = Map.of("a", 1L);

    private enum Shape {
        STRUCT, MAP, RAW_JSON;

        private Map<String, String> props() {
            return this == RAW_JSON ? Map.of("value.format", "json") : Map.of();
        }

        /** A record {sec_id, px} whose px is of a type the connector rejects. */
        private SinkRecord failingAfterKey(long id) {
            switch (this) {
                case STRUCT:
                    Schema schema = SchemaBuilder.struct()
                            .field("sec_id", Schema.INT64_SCHEMA)
                            .field("px", Schema.BYTES_SCHEMA)
                            .build();
                    Struct struct = new Struct(schema).put("sec_id", id).put("px", new byte[]{1});
                    return new SinkRecord("tab", 0, null, null, schema, struct, 0L);
                case MAP:
                    Map<String, Object> value = new LinkedHashMap<>();
                    value.put("sec_id", id);
                    value.put("px", new byte[]{1});
                    return new SinkRecord("tab", 0, null, null, null, value, 0L);
                default:
                    return new SinkRecord("tab", 0, null, null, null,
                            ("{\"sec_id\":" + id + ",\"px\":[1,\"a\"]}").getBytes(StandardCharsets.UTF_8), 0L);
            }
        }

        /** A record {sec_id, px}; a null id leaves the field out. */
        private SinkRecord record(Object id) {
            switch (this) {
                case STRUCT:
                    SchemaBuilder builder = SchemaBuilder.struct();
                    Schema nested = SchemaBuilder.struct().field("a", Schema.INT64_SCHEMA).build();
                    if (id != null) {
                        builder.field("sec_id", id instanceof Long ? Schema.INT64_SCHEMA
                                : id instanceof Double ? Schema.FLOAT64_SCHEMA
                                : id instanceof Boolean ? Schema.BOOLEAN_SCHEMA
                                : id == NESTED ? nested
                                : Schema.STRING_SCHEMA);
                    }
                    Schema schema = builder.field("px", Schema.FLOAT64_SCHEMA).build();
                    Struct struct = new Struct(schema).put("px", 1.5d);
                    if (id != null) {
                        struct.put("sec_id", id == NESTED ? new Struct(nested).put("a", 1L) : id);
                    }
                    return new SinkRecord("tab", 0, null, null, schema, struct, 0L);
                case MAP:
                    Map<String, Object> value = new LinkedHashMap<>();
                    if (id != null) {
                        value.put("sec_id", id);
                    }
                    value.put("px", 1.5d);
                    return new SinkRecord("tab", 0, null, null, null, value, 0L);
                default:
                    String idJson = id == null ? ""
                            : "\"sec_id\":" + (id instanceof String ? "\"" + id + "\"" : id == NESTED ? "{\"a\":1}" : id.toString()) + ",";
                    return new SinkRecord("tab", 0, null, null, null,
                            ("{" + idJson + "\"px\":1.5}").getBytes(StandardCharsets.UTF_8), 0L);
            }
        }
    }

    private static RecordToRowHandler newHandler(Recorder recorder, boolean routeSymbolsDirectly) {
        return newHandler(recorder, routeSymbolsDirectly, "sym,num");
    }

    private static RecordToRowHandler newHandler(Recorder recorder, boolean routeSymbolsDirectly, String symbols) {
        Map<String, String> props = new HashMap<>();
        props.put("client.conf.string", "ws::addr=localhost:9000;");
        props.put("topics", "tab");
        props.put("table", "tab");
        props.put("symbols", symbols);
        props.put("include.key", "false");
        QuestDBSinkConnectorConfig config = new QuestDBSinkConnectorConfig(props);
        return new RecordToRowHandler(config, recorder.proxy(), true, false, routeSymbolsDirectly);
    }

    private static SinkRecord record() {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("px", 1.5d);
        value.put("sym", "abc");
        value.put("num", 42L);
        value.put("note", "plain");
        return new SinkRecord("tab", 0, null, null, null, value, 0L);
    }

    private static final class Recorder {
        private final List<String> calls = new ArrayList<>();

        private Sender proxy() {
            return (Sender) Proxy.newProxyInstance(
                    Sender.class.getClassLoader(),
                    new Class<?>[]{Sender.class},
                    (proxy, method, args) -> {
                        switch (method.getName()) {
                            case "table":
                                calls.add("table(" + args[0] + ")");
                                return proxy;
                            case "symbol":
                            case "stringColumn":
                            case "doubleColumn":
                            case "longColumn":
                            case "boolColumn":
                                calls.add(method.getName() + "(" + args[0] + "," + args[1] + ")");
                                return proxy;
                            case "atNow":
                                calls.add("atNow()");
                                return null;
                            default:
                                return method.getReturnType() == Sender.class ? proxy : null;
                        }
                    });
        }
    }
}
