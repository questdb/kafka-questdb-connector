package io.questdb.kafka;

import io.questdb.client.Sender;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * value.format=json parses the payload itself instead of consuming what the Connect
 * converter produced, which means the mapping rules exist in two places. These tests
 * pin the two implementations together: the same payload must produce the same
 * sequence of Sender calls whether it arrives as raw bytes or as a converted Map.
 */
class RawJsonEquivalenceTest {

    private static void assertSameRow(List<String> expected, List<String> actual, String context) {
        assertEquals(expected.get(0), actual.get(0), context);
        assertEquals(expected.get(expected.size() - 1), actual.get(actual.size() - 1), context);
        List<String> e = new ArrayList<>(expected.subList(1, expected.size() - 1));
        List<String> a = new ArrayList<>(actual.subList(1, actual.size() - 1));
        Collections.sort(e);
        Collections.sort(a);
        assertEquals(e, a, context);
    }

    private static void assertSameRow(List<String> expected, List<String> actual) {
        assertEquals(expected.get(0), actual.get(0), "row must start with table()");
        assertEquals(expected.get(expected.size() - 1), actual.get(actual.size() - 1), "row must end the same way");
        List<String> expectedCols = new ArrayList<>(expected.subList(1, expected.size() - 1));
        List<String> actualCols = new ArrayList<>(actual.subList(1, actual.size() - 1));
        Collections.sort(expectedCols);
        Collections.sort(actualCols);
        // field order differs by design (HashMap iteration vs JSON document order)
        assertEquals(expectedCols, actualCols, "same columns with the same values");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{\"sym\":\"abc\",\"px\":1.5,\"seq\":42,\"flag\":true}",
            "{\"sym\":\"abc\",\"nested\":{\"a\":1,\"b\":\"x\"}}",
            "{\"only_string\":\"v\"}",
            "{\"neg\":-17,\"zero\":0,\"frac\":-0.25}",
            "{\"nullable\":null,\"kept\":7}",
            "{\"dotted.name\":3}",
            "{\"unicode\":\"héllo\",\"empty\":\"\"}",
    })
    void rawJsonMatchesConvertedMap(String json) {
        Map<String, String> props = baseProps();
        assertSameRow(callsForConverted(props, json), callsForRaw(props, json));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{\"sym\":\"abc\",\"num\":5,\"px\":1.5}",
            "{\"sym\":\"abc\",\"nested\":{\"num\":9}}",
    })
    void rawJsonMatchesWithSymbolsAndDoubles(String json) {
        Map<String, String> props = baseProps();
        props.put("symbols", "sym,num");
        props.put("doubles", "px");
        assertSameRow(callsForConverted(props, json), callsForRaw(props, json));
    }

    @Test
    void rawJsonMatchesWithDesignatedTimestampAndPrefix() {
        Map<String, String> props = baseProps();
        props.put("timestamp.field.name", "ts");
        props.put("timestamp.units", "nanos");
        props.put("value.prefix", "v");
        String json = "{\"sym\":\"abc\",\"px\":1.5,\"ts\":1700000000000000000}";
        assertSameRow(callsForConverted(props, json), callsForRaw(props, json));
    }

    @Test
    void rawJsonMatchesWithIncludedKey() {
        Map<String, String> props = baseProps();
        props.put("include.key", "true");
        props.put("key.prefix", "k");
        String json = "{\"px\":1.5}";
        assertSameRow(callsForConverted(props, json), callsForRaw(props, json));
    }

    /** Keys are not JSON: they still come from the key converter, so both paths must agree. */
    @Test
    void structuredAndLogicalKeysMatchTheConvertedPath() {
        Map<String, String> props = baseProps();
        props.put("include.key", "true");
        props.put("key.prefix", "k");
        Map<String, Object> mapKey = new LinkedHashMap<>();
        mapKey.put("id", 7L);
        for (Object key : new Object[]{"k1", 7L, Boolean.TRUE, mapKey, new java.util.Date(1700000000000L)}) {
            Recorder converted = new Recorder();
            handler(props, converted, false).handle(new SinkRecord("tab", 0, null, key, null,
                    JsonTestUtils.toConnectValue("{\"px\":1.5}"), 0L));
            Recorder raw = new Recorder();
            handler(props, raw, true).handle(new SinkRecord("tab", 0, null, key, null,
                    "{\"px\":1.5}".getBytes(StandardCharsets.UTF_8), 0L));
            assertSameRow(converted.calls, raw.calls, "key type " + key.getClass().getSimpleName());
        }
    }

    /**
     * Deep nesting used to be a StackOverflowError, and an Error is never routed to the DLQ -
     * the record would kill the task on every restart.
     */
    @Test
    void deeplyNestedPayloadsFailAsInvalidDataNotAsAnError() {
        Map<String, String> props = baseProps();
        RecordToRowHandler handler = handler(props, new Recorder(), true);
        StringBuilder objects = new StringBuilder("{");
        for (int i = 0; i < 5000; i++) {
            objects.append("\"a\":{");
        }
        objects.append("\"b\":1");
        for (int i = 0; i < 5001; i++) {
            objects.append('}');
        }
        assertThrows(InvalidDataException.class, () -> handler.handle(record(objects.toString())));

        StringBuilder arrays = new StringBuilder("{\"a\":");
        for (int i = 0; i < 5000; i++) {
            arrays.append('[');
        }
        arrays.append('1');
        for (int i = 0; i < 5000; i++) {
            arrays.append(']');
        }
        arrays.append('}');
        assertThrows(InvalidDataException.class, () -> handler.handle(record(arrays.toString())));
    }

    @Test
    void nestingUpToTheLimitStillWorks() {
        Map<String, String> props = baseProps();
        StringBuilder json = new StringBuilder("{");
        for (int i = 0; i < 30; i++) {
            json.append("\"a\":{");
        }
        json.append("\"b\":1");
        for (int i = 0; i < 31; i++) {
            json.append('}');
        }
        assertSameRow(callsForConverted(props, json.toString()), callsForRaw(props, json.toString()));
    }

    @Test
    void tombstonesAreIgnoredOnBothPaths() {
        Map<String, String> props = baseProps();
        Recorder raw = new Recorder();
        RecordToRowHandler rawHandler = handler(props, raw, true);
        SinkRecord tombstone = new SinkRecord("tab", 0, null, "k", null, null, 0L);
        assertEquals(false, rawHandler.handle(tombstone));
        assertTrue(raw.calls.isEmpty());
    }

    @Test
    void malformedJsonIsInvalidDataAndCancelsTheRow() {
        Map<String, String> props = baseProps();
        Recorder recorder = new Recorder();
        RecordToRowHandler handler = handler(props, recorder, true);
        SinkRecord record = new SinkRecord("tab", 0, null, "k", null,
                "{\"broken\":".getBytes(StandardCharsets.UTF_8), 0L);

        assertThrows(InvalidDataException.class, () -> handler.handle(record));
        assertTrue(recorder.calls.contains("cancelRow()"), "a half-written row must be cancelled: " + recorder.calls);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{\"arr\":[1.5,2.5,3.5]}",
            "{\"arr\":[1,2,3]}",
            "{\"arr\":[]}",
            "{\"firstname\":\"John\",\"meta-data\":[]}",
            "{\"arr\":[[1.0,2.0],[3.0,4.0]]}",
            "{\"arr\":[[[1.0],[2.0]],[[3.0],[4.0]]]}",
            "{\"px\":1.5,\"arr\":[1.0,2.0],\"sym\":\"s\"}",
    })
    void rawJsonMatchesForArrays(String json) {
        Map<String, String> props = baseProps();
        assertSameRow(callsForConverted(props, json), callsForRaw(props, json));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{\"arr\":[[1.0,2.0],[3.0]]}",          // jagged
            "{\"arr\":[1.0,null,3.0]}",             // null element
            "{\"arr\":[\"a\",\"b\"]}",              // unsupported element type
    })
    void rawJsonRejectsTheSameBadArraysAsTheConvertedPath(String json) {
        Map<String, String> props = baseProps();
        Class<? extends Throwable> convertedFailure = failureOf(() -> callsForConverted(props, json));
        Class<? extends Throwable> rawFailure = failureOf(() -> callsForRaw(props, json));
        assertEquals(convertedFailure, rawFailure, "both paths must reject the same payloads");
    }

    private static Class<? extends Throwable> failureOf(Runnable r) {
        try {
            r.run();
            return null;
        } catch (Throwable t) {
            return t.getClass();
        }
    }

    /**
     * Duplicate field names are the one documented divergence. The converted path sees a
     * Map, so the last value wins; the fast path streams and writes the column twice, and
     * QuestDB keeps the first. Detecting this would cost a lookup per field on the hot
     * path for input RFC 8259 says SHOULD NOT occur, so the behaviour is pinned instead.
     */
    @Test
    void duplicateFieldNamesDivergeAndAreDocumented() {
        Map<String, String> props = baseProps();
        assertEquals(List.of("table(tab)", "longColumn(a,2)", "atNow()"),
                callsForConverted(props, "{\"a\":1,\"a\":2}"), "converted path: last value wins");
        assertEquals(List.of("table(tab)", "longColumn(a,1)", "longColumn(a,2)", "atNow()"),
                callsForRaw(props, "{\"a\":1,\"a\":2}"), "raw path writes both; QuestDB keeps the first");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{\"a\":{}}",                                   // empty nested object -> no columns
            "{\"a\":{\"vec\":[1.0,2.0]}}",                    // array nested in an object
            "{\"a\":{\"mat\":[[1.0,2.0],[3.0,4.0]]}}",        // 2D array nested in an object
            "{\"a\":{\"b\":1},\"a_b\":2}",                   // flattened name collides with a real field
            "{\"outer\":{\"inner\":{\"deep\":\"s\"}},\"top\":1}",
            "{\"a\":{\"b\":null},\"c\":1}",                  // null inside a nested object
    })
    void nestedStructuresMatchTheConvertedPath(String json) {
        Map<String, String> props = baseProps();
        assertSameRow(callsForConverted(props, json), callsForRaw(props, json));
    }

    @Test
    void nestedFieldsHonourSymbolsAndDoublesByFlattenedName() {
        Map<String, String> props = baseProps();
        props.put("symbols", "meta_region");
        props.put("doubles", "meta_count");
        String json = "{\"meta\":{\"region\":\"eu\",\"count\":7},\"px\":1.5}";
        assertSameRow(callsForConverted(props, json), callsForRaw(props, json));
    }

    @Test
    void nestedDesignatedTimestampIsMatchedByFlattenedName() {
        Map<String, String> props = baseProps();
        props.put("timestamp.field.name", "meta_ts");
        props.put("timestamp.units", "nanos");
        String json = "{\"meta\":{\"ts\":1700000000000000000},\"px\":1.5}";
        List<String> converted = callsForConverted(props, json);
        List<String> raw = callsForRaw(props, json);
        assertSameRow(converted, raw);
        assertTrue(raw.get(raw.size() - 1).startsWith("at("),
                "the nested field must be used as the designated timestamp: " + raw);
    }

    @Test
    void nestedValuesAlsoGetTheValuePrefix() {
        Map<String, String> props = baseProps();
        props.put("value.prefix", "v");
        assertSameRow(callsForConverted(props, "{\"a\":{\"b\":1}}"),
                callsForRaw(props, "{\"a\":{\"b\":1}}"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"false", "true"})
    void objectsInsideArraysFollowSkipUnsupportedTypes(String skip) {
        Map<String, String> props = baseProps();
        props.put("skip.unsupported.types", skip);
        String json = "{\"arr\":[{\"a\":1}],\"px\":1.5}";
        Class<? extends Throwable> convertedFailure = failureOf(() -> callsForConverted(props, json));
        Class<? extends Throwable> rawFailure = failureOf(() -> callsForRaw(props, json));
        assertEquals(convertedFailure, rawFailure, "skip.unsupported.types must decide on both paths");
        if (convertedFailure == null) {
            assertSameRow(callsForConverted(props, json), callsForRaw(props, json));
        }
    }

    /**
     * Deliberate difference: an integer beyond Long.MAX_VALUE silently overflows on the
     * converted path (JsonConverter yields a wrapped long), while the fast path keeps the
     * magnitude as a double.
     */
    @Test
    void oversizedIntegersDoNotOverflowOnTheFastPath() {
        Map<String, String> props = baseProps();
        String json = "{\"big\":123456789012345678901234567890}";
        assertEquals(List.of("table(tab)", "longColumn(big,-4362896299872285998)", "atNow()"),
                callsForConverted(props, json), "converted path overflows");
        assertEquals(List.of("table(tab)", "doubleColumn(big,1.2345678901234568E29)", "atNow()"),
                callsForRaw(props, json), "fast path keeps the magnitude");
    }

    /** value.format=json_envelope unwraps what JsonConverter writes with schemas.enable=true. */
    @Test
    void envelopeModeUnwrapsThePayload() {
        String enveloped = "{\"schema\":{\"type\":\"struct\",\"fields\":"
                + "[{\"type\":\"double\",\"optional\":false,\"field\":\"px\"}],\"optional\":false},"
                + "\"payload\":{\"px\":1.5,\"sym\":\"abc\"}}";

        Map<String, String> envProps = baseProps();
        envProps.put("value.format", "json_envelope");
        Recorder recorder = new Recorder();
        QuestDBSinkConnectorConfig config = new QuestDBSinkConnectorConfig(envProps);
        new RecordToRowHandler(config, recorder.proxy(), true, false, true).handle(record(enveloped));

        // identical to the same payload sent without an envelope
        assertSameRow(callsForRaw(baseProps(), "{\"px\":1.5,\"sym\":\"abc\"}"), recorder.calls);
    }

    @Test
    void envelopeModeRejectsARecordWithoutPayload() {
        Map<String, String> props = baseProps();
        props.put("value.format", "json_envelope");
        QuestDBSinkConnectorConfig config = new QuestDBSinkConnectorConfig(props);
        RecordToRowHandler handler = new RecordToRowHandler(config, new Recorder().proxy(), true, false, true);
        assertThrows(InvalidDataException.class, () -> handler.handle(record("{\"schema\":{\"type\":\"struct\"}}")));
    }

    /** No envelope guessing: a field named "schema" is ordinary data in value.format=json. */
    @Test
    void aFieldNamedSchemaIsOrdinaryDataInPlainJsonMode() {
        Map<String, String> props = baseProps();
        assertSameRow(callsForConverted(props, "{\"schema\":\"v1\",\"px\":1.5}"),
                callsForRaw(props, "{\"schema\":\"v1\",\"px\":1.5}"));
        // an object-valued "schema" is data too, not a trigger
        assertSameRow(callsForConverted(props, "{\"schema\":{\"v\":1},\"px\":1.5}"),
                callsForRaw(props, "{\"schema\":{\"v\":1},\"px\":1.5}"));
    }

    /**
     * An adversarial review found the fast path only honoured the designated timestamp for
     * string and integer values. A null, fractional, boolean or structured value was written
     * as an ordinary column and the row silently received wall-clock time instead of going
     * to the DLQ - the worst kind of defect for this feature.
     */
    @ParameterizedTest
    @ValueSource(strings = {
            "{\"ts\":null,\"px\":1.5}",
            "{\"ts\":1.7e18,\"px\":1.5}",
            "{\"ts\":true,\"px\":1.5}",
            "{\"ts\":{\"a\":1},\"px\":1.5}",
            "{\"ts\":[1.0,2.0],\"px\":1.5}",
    })
    void aDesignatedTimestampThatIsNotAStringOrIntegerIsRejected(String json) {
        Map<String, String> props = baseProps();
        props.put("timestamp.field.name", "ts");
        props.put("timestamp.units", "nanos");
        assertEquals(failureOf(() -> callsForConverted(props, json)), failureOf(() -> callsForRaw(props, json)),
                "both paths must reject a timestamp value they cannot interpret");
        assertThrows(InvalidDataException.class, () -> callsForRaw(props, json));
    }

    @Test
    void aValidDesignatedTimestampStillWorks() {
        Map<String, String> props = baseProps();
        props.put("timestamp.field.name", "ts");
        props.put("timestamp.units", "nanos");
        List<String> raw = callsForRaw(props, "{\"ts\":1700000000000000000,\"px\":1.5}");
        assertEquals(List.of("table(tab)", "doubleColumn(px,1.5)", "at(1700000000000000)"), raw);
    }

    @Test
    void oversizedIntegersInsideArraysDoNotLoseTheRecord() {
        Map<String, String> props = baseProps();
        // the scalar path already fell back to double; inside an array it threw and the
        // record was lost to the DLQ
        List<String> raw = callsForRaw(props, "{\"arr\":[1,99999999999999999999]}");
        assertEquals(3, raw.size(), raw.toString());
        assertTrue(raw.get(1).startsWith("doubleArray(arr"), raw.toString());
    }

    @Test
    void emptyPayloadIsRejectedRatherThanSilentlyDropped() {
        Map<String, String> props = baseProps();
        Recorder recorder = new Recorder();
        RecordToRowHandler handler = handler(props, recorder, true);
        SinkRecord empty = new SinkRecord("tab", 0, null, "k", null, new byte[0], 0L);
        assertThrows(InvalidDataException.class, () -> handler.handle(empty));
    }

    @Test
    void unterminatedArrayIsRejected() {
        Map<String, String> props = baseProps();
        RecordToRowHandler handler = handler(props, new Recorder(), true);
        assertThrows(InvalidDataException.class, () -> handler.handle(record("{\"arr\":[1,2")));
    }

    @Test
    void deeplyNestedObjectsAreFlattened() {
        Map<String, String> props = baseProps();
        assertSameRow(callsForConverted(props, "{\"a\":{\"b\":{\"c\":{\"d\":1}}}}"),
                callsForRaw(props, "{\"a\":{\"b\":{\"c\":{\"d\":1}}}}"));
    }

    @Test
    void arraysCanStillBeSkipped() {
        Map<String, String> props = baseProps();
        props.put("skip.unsupported.types", "true");
        Recorder skipping = new Recorder();
        RecordToRowHandler skipHandler = handler(props, skipping, true);
        skipHandler.handle(record("{\"arr\":[\"a\"],\"px\":1.5}"));
        assertEquals(List.of("table(tab)", "doubleColumn(px,1.5)", "atNow()"), skipping.calls,
                "an unsupported array is skipped, the rest of the row still lands");
    }

    /** A failed row must not leak its timestamp into the next record. */
    @Test
    void timestampStateDoesNotLeakAfterAFailedRow() {
        Map<String, String> props = baseProps();
        props.put("timestamp.field.name", "ts");
        props.put("timestamp.units", "nanos");
        Recorder recorder = new Recorder();
        RecordToRowHandler handler = handler(props, recorder, true);

        assertThrows(InvalidDataException.class,
                () -> handler.handle(record("{\"ts\":1700000000000000000,\"broken\":")));
        recorder.calls.clear();
        handler.handle(record("{\"px\":2.5}"));
        assertEquals(List.of("table(tab)", "doubleColumn(px,2.5)", "atNow()"), recorder.calls,
                "the previous row's timestamp must not carry over");
    }

    /** The same leak existed on the standard path and is only visible with assertions off. */
    @Test
    void standardPathAlsoDoesNotLeakTheTimestampAfterAFailedRow() {
        Map<String, String> props = baseProps();
        props.put("timestamp.field.name", "ts");
        props.put("timestamp.units", "nanos");
        Recorder recorder = new Recorder();
        RecordToRowHandler handler = handler(props, recorder, false);

        assertThrows(InvalidDataException.class, () -> handler.handle(new SinkRecord("tab", 0, null, "k", null,
                JsonTestUtils.toConnectValue("{\"ts\":1700000000000000000,\"bad\":[{\"o\":1}]}"), 0L)));
        recorder.calls.clear();
        handler.handle(new SinkRecord("tab", 0, null, "k", null,
                JsonTestUtils.toConnectValue("{\"px\":2.5}"), 0L));
        assertEquals(List.of("table(tab)", "doubleColumn(px,2.5)", "atNow()"), recorder.calls,
                "the failed record's timestamp must not carry over");
    }

    @Test
    void wrongConverterIsReportedClearly() {
        Map<String, String> props = baseProps();
        RecordToRowHandler handler = handler(props, new Recorder(), true);
        SinkRecord stringValue = new SinkRecord("tab", 0, null, "k", null, "not-bytes", 0L);
        InvalidDataException e = assertThrows(InvalidDataException.class, () -> handler.handle(stringValue));
        assertTrue(String.valueOf(e.getMessage()).contains("ByteArrayConverter"), e.getMessage());
    }

    // ---- helpers ----

    private static Map<String, String> baseProps() {
        Map<String, String> props = new HashMap<>();
        props.put("client.conf.string", "ws::addr=localhost:9000;");
        props.put("topics", "tab");
        props.put("table", "tab");
        props.put("include.key", "false");
        return props;
    }

    private static List<String> callsForRaw(Map<String, String> props, String json) {
        Recorder recorder = new Recorder();
        Map<String, String> rawProps = new HashMap<>(props);
        rawProps.put("value.format", "json");
        handler(rawProps, recorder, true).handle(record(json));
        return recorder.calls;
    }

    private static List<String> callsForConverted(Map<String, String> props, String json) {
        Recorder recorder = new Recorder();
        Object converted = JsonTestUtils.toConnectValue(json);
        SinkRecord record = new SinkRecord("tab", 0, null, "k", null, converted, 0L);
        handler(props, recorder, false).handle(record);
        return recorder.calls;
    }

    private static RecordToRowHandler handler(Map<String, String> props, Recorder recorder, boolean rawJson) {
        Map<String, String> effective = new HashMap<>(props);
        if (rawJson) {
            effective.put("value.format", "json");
        }
        QuestDBSinkConnectorConfig config = new QuestDBSinkConnectorConfig(effective);
        return new RecordToRowHandler(config, recorder.proxy(), true, false, true);
    }

    private static SinkRecord record(String json) {
        return new SinkRecord("tab", 0, null, "k", null, json.getBytes(StandardCharsets.UTF_8), 0L);
    }

    private static String deepToString(Object array) {
        if (array instanceof double[]) {
            return java.util.Arrays.toString((double[]) array);
        }
        if (array instanceof long[]) {
            return java.util.Arrays.toString((long[]) array);
        }
        return java.util.Arrays.deepToString((Object[]) array);
    }

    static final class Recorder {
        final List<String> calls = new ArrayList<>();

        Sender proxy() {
            return (Sender) Proxy.newProxyInstance(
                    Sender.class.getClassLoader(),
                    new Class<?>[]{Sender.class},
                    (proxy, method, args) -> {
                        switch (method.getName()) {
                            case "table":
                            case "cancelRow":
                            case "atNow":
                                calls.add(method.getName() + "(" + (args == null ? "" : args[0]) + ")");
                                return method.getReturnType() == Sender.class ? proxy : null;
                            case "at":
                                calls.add("at(" + args[0] + ")");
                                return null;
                            case "symbol":
                            case "stringColumn":
                            case "doubleColumn":
                            case "longColumn":
                            case "boolColumn":
                                calls.add(method.getName() + "(" + args[0] + "," + args[1] + ")");
                                return proxy;
                            case "timestampColumn":
                                calls.add("timestampColumn(" + args[0] + "," + args[1] + ")");
                                return proxy;
                            case "doubleArray":
                            case "longArray":
                                // recorded so array equivalence is actually compared
                                calls.add(method.getName() + "(" + args[0] + "," + deepToString(args[1]) + ")");
                                return proxy;
                            default:
                                return method.getReturnType() == Sender.class ? proxy : null;
                        }
                    });
        }
    }

    /** Converts JSON the way Connect's JsonConverter would, so both paths see the same input. */
    static final class JsonTestUtils {
        static Object toConnectValue(String json) {
            org.apache.kafka.connect.json.JsonConverter converter = new org.apache.kafka.connect.json.JsonConverter();
            Map<String, Object> cfg = new LinkedHashMap<>();
            cfg.put("converter.type", "value");
            cfg.put("schemas.enable", "false");
            converter.configure(cfg);
            return converter.toConnectData("tab", json.getBytes(StandardCharsets.UTF_8)).value();
        }
    }
}
