package io.questdb.kafka;

import io.questdb.client.Sender;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

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

    private static RecordToRowHandler newHandler(Recorder recorder, boolean routeSymbolsDirectly) {
        Map<String, String> props = new HashMap<>();
        props.put("client.conf.string", "ws::addr=localhost:9000;");
        props.put("topics", "tab");
        props.put("table", "tab");
        props.put("symbols", "sym,num");
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
