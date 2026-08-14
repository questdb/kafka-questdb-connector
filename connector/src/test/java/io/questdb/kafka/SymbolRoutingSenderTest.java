package io.questdb.kafka;

import io.questdb.client.Sender;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SymbolRoutingSenderTest {

    @Test
    void routesConfiguredColumnsToSymbolsAndPassesTheRestThrough() {
        Recorder recorder = new Recorder();
        Sender sender = new SymbolRoutingSender(recorder.proxy(), "sym,num,flag,ts_sym");

        sender.table("tab")
                .doubleColumn("px", 1.5)          // not a symbol -> stays a double
                .stringColumn("sym", "abc")       // symbol
                .longColumn("num", 42)            // symbol, stringified
                .boolColumn("flag", true)         // symbol, stringified
                .timestampColumn("ts_sym", 7, ChronoUnit.MICROS) // symbol, stringified
                .stringColumn("note", "plain")    // not a symbol -> stays a string
                .symbol("explicit", "value");     // explicit symbol passes through
        sender.atNow();

        assertEquals(
                List.of("table(tab)",
                        "doubleColumn(px,1.5)",
                        "symbol(sym,abc)",
                        "symbol(num,42)",
                        "symbol(flag,true)",
                        "symbol(ts_sym,7)",
                        "stringColumn(note,plain)",
                        "symbol(explicit,value)",
                        "atNow()"),
                recorder.calls);
    }

    /**
     * The point of the class: on QWP, symbols may be interleaved with fields, so
     * nothing is buffered and calls reach the wire in the order they were made.
     */
    @Test
    void doesNotReorderOrBufferColumns() {
        Recorder recorder = new Recorder();
        Sender sender = new SymbolRoutingSender(recorder.proxy(), "sym");

        sender.table("tab").doubleColumn("px", 2.5);
        assertEquals(List.of("table(tab)", "doubleColumn(px,2.5)"), recorder.calls,
                "columns must reach the underlying sender immediately");

        sender.stringColumn("sym", "s1");
        sender.at(123, ChronoUnit.MICROS);
        assertEquals(
                List.of("table(tab)", "doubleColumn(px,2.5)", "symbol(sym,s1)", "at(123)"),
                recorder.calls);
    }

    @Test
    void delegatesLifecycleAndAckMethods() {
        Recorder recorder = new Recorder();
        Sender sender = new SymbolRoutingSender(recorder.proxy(), "sym");

        sender.flush();
        assertEquals(7L, sender.flushAndGetSequence());
        assertEquals(3L, sender.getAckedFsn());
        assertTrue(sender.awaitAckedFsn(3L, 10L));
        assertTrue(sender.drain(10L));
        sender.cancelRow();
        sender.reset();
        sender.close();

        assertEquals(
                List.of("flush()", "flushAndGetSequence()", "getAckedFsn()", "awaitAckedFsn(3)",
                        "drain(10)", "cancelRow()", "reset()", "close()"),
                recorder.calls);
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
                                calls.add("symbol(" + args[0] + "," + args[1] + ")");
                                return proxy;
                            case "stringColumn":
                                calls.add("stringColumn(" + args[0] + "," + args[1] + ")");
                                return proxy;
                            case "doubleColumn":
                                calls.add("doubleColumn(" + args[0] + "," + args[1] + ")");
                                return proxy;
                            case "longColumn":
                                calls.add("longColumn(" + args[0] + "," + args[1] + ")");
                                return proxy;
                            case "boolColumn":
                                calls.add("boolColumn(" + args[0] + "," + args[1] + ")");
                                return proxy;
                            case "timestampColumn":
                                calls.add("timestampColumn(" + args[0] + "," + args[1] + ")");
                                return proxy;
                            case "atNow":
                                calls.add("atNow()");
                                return null;
                            case "at":
                                calls.add("at(" + args[0] + ")");
                                return null;
                            case "flush":
                                calls.add("flush()");
                                return null;
                            case "flushAndGetSequence":
                                calls.add("flushAndGetSequence()");
                                return 7L;
                            case "getAckedFsn":
                                calls.add("getAckedFsn()");
                                return 3L;
                            case "awaitAckedFsn":
                                calls.add("awaitAckedFsn(" + args[0] + ")");
                                return true;
                            case "drain":
                                calls.add("drain(" + args[0] + ")");
                                return true;
                            case "cancelRow":
                                calls.add("cancelRow()");
                                return null;
                            case "reset":
                                calls.add("reset()");
                                return null;
                            case "close":
                                calls.add("close()");
                                return null;
                            default:
                                throw new UnsupportedOperationException(method.getName());
                        }
                    });
        }
    }
}
