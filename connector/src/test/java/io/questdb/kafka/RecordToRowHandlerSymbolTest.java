package io.questdb.kafka;

import io.questdb.client.Sender;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
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
        Map<String, String> lookup = new HashMap<>();
        lookup.put("42", "AAPL");
        lookup.put("abc", "MSFT");
        lookup.put("42.5", "IBM");
        lookup.put("true", "ORCL");
        handler.setEnrichment("ticker", lookup);

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
        handler.handle(shape.record(null)); // key field absent: neither a hit nor a miss
        assertEquals(List.of("table(tab)", "doubleColumn(px,1.5)", "atNow()"), recorder.calls);

        recorder.calls.clear();
        handler.handle(shape.record(NESTED)); // not a scalar: written as usual, reported as unusable
        assertEquals(List.of("table(tab)", "longColumn(sec_id_a,1)", "doubleColumn(px,1.5)", "atNow()"), recorder.calls);

        try (LogCapture logs = new LogCapture(EnrichmentMisses.class)) {
            handler.reportEnrichmentMisses();
            handler.reportEnrichmentMisses(); // nothing changed since the first report
            assertEquals(List.of(
                    "INFO Enrichment lookup: 1 key(s) have no mapping (1 new, 0 mapped since the previous report)",
                    "DEBUG Enrichment keys newly without a mapping (at most 20 shown): [7]",
                    "WARN 1 record(s) had a value in enrichment.key.field 'sec_id' that cannot be a lookup key;"
                            + " only text, integers, floating-point numbers and booleans can. Those records are not"
                            + " enriched; later ones are logged at DEBUG"), logs.lines);
        }
    }

    /** Stands for a key field holding an object, {"a": 1}. */
    private static final Map<String, Object> NESTED = Map.of("a", 1L);

    private enum Shape {
        STRUCT, MAP, RAW_JSON;

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
