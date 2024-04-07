package io.questdb.kafka;

import io.questdb.std.Chars;
import io.questdb.std.str.StringSink;
import org.junit.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ClientConfUtilsTest {

    @Test
    public void testHttpTransportIsResolved() {
        StringSink sink = new StringSink();
        assertTrue(ClientConfUtils.patchConfStr("http::addr=localhost:9000;", sink));
        assertTrue(ClientConfUtils.patchConfStr("https::addr=localhost:9000;", sink));
        assertTrue(ClientConfUtils.patchConfStr("https::addr=localhost:9000;", sink));
        assertFalse(ClientConfUtils.patchConfStr("tcp::addr=localhost:9000;", sink));
        assertFalse(ClientConfUtils.patchConfStr("tcps::addr=localhost:9000;", sink));
    }

    @Test
    public void testHttpTransportTimeBasedFlushesDisabledByDefault() {
        assertConfStringIsPatched("http::addr=localhost:9000;");
        assertConfStringIsPatched("https::addr=localhost:9000;foo=bar;");
        assertConfStringIsPatched("https::addr=localhost:9000;auto_flush_rows=1;");
        assertConfStringIsPatched("https::addr=localhost:9000;auto_flush=on;");

        assertConfStringIsNotPatched("https::addr=localhost:9000;foo=bar;auto_flush_interval=100;");
        assertConfStringIsNotPatched("https::addr=localhost:9000;foo=bar;auto_flush=off;");
        assertConfStringIsNotPatched("https::addr=localhost:9000;foo=bar");
        assertConfStringIsNotPatched("https::addr");
        assertConfStringIsNotPatched("https");
        assertConfStringIsNotPatched("tcp::addr=localhost:9000;");
        assertConfStringIsNotPatched("tcps::addr=localhost:9000;foo=bar;");
        assertConfStringIsNotPatched("tcps::addr=localhost:9000;auto_flush_rows=1;");
        assertConfStringIsNotPatched("tcps::addr=localhost:9000;auto_flush=on;");
        assertConfStringIsNotPatched("unknown::addr=localhost:9000;auto_flush=on;");
    }

    private static void assertConfStringIsPatched(String confStr) {
        StringSink sink = new StringSink();
        ClientConfUtils.patchConfStr(confStr, sink);

        String expected = confStr + "auto_flush_interval=" + Integer.MAX_VALUE + ";";
        assertTrue(Chars.equals(expected, sink), "Conf string = " + confStr + ", expected = " + expected + ", actual = " + sink);
    }

    private static void assertConfStringIsNotPatched(String confStr) {
        StringSink sink = new StringSink();
        ClientConfUtils.patchConfStr(confStr, sink);

        assertEquals(confStr, sink.toString());
    }

}