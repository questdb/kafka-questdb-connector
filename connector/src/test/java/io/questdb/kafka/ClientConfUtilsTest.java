package io.questdb.kafka;

import io.questdb.std.str.StringSink;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class ClientConfUtilsTest {
    private static final int DEFAULT_MAX_PENDING_ROWS = 75_000;
    private static final long DEFAULT_FLUSH_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(1);

    @Test
    public void testHttpTransportIsResolved() {
        StringSink sink = new StringSink();
        FlushConfig flushConfig = new FlushConfig();
        assertTrue(ClientConfUtils.patchConfStr("http::addr=localhost:9000;", sink, flushConfig));
        assertTrue(ClientConfUtils.patchConfStr("https::addr=localhost:9000;", sink, flushConfig));
        assertTrue(ClientConfUtils.patchConfStr("https::addr=localhost:9000;", sink, flushConfig));
        assertFalse(ClientConfUtils.patchConfStr("tcp::addr=localhost:9000;", sink, flushConfig));
        assertFalse(ClientConfUtils.patchConfStr("tcps::addr=localhost:9000;", sink, flushConfig));
    }

    @Test
    public void testClientConfPatching() {
        assertConfStringIsPatched("http::addr=localhost:9000;", "http::addr=localhost:9000;auto_flush=off;", DEFAULT_MAX_PENDING_ROWS, DEFAULT_FLUSH_INTERVAL_NANOS);
        assertConfStringIsPatched("https::addr=localhost:9000;foo=bar;", "https::addr=localhost:9000;foo=bar;auto_flush=off;", DEFAULT_MAX_PENDING_ROWS, DEFAULT_FLUSH_INTERVAL_NANOS);
        assertConfStringIsPatched("https::addr=localhost:9000;auto_flush_rows=1;", "https::addr=localhost:9000;auto_flush=off;",1, DEFAULT_FLUSH_INTERVAL_NANOS);
        assertConfStringIsPatched("https::addr=localhost:9000;auto_flush=on;", "https::addr=localhost:9000;auto_flush=off;", DEFAULT_MAX_PENDING_ROWS, DEFAULT_FLUSH_INTERVAL_NANOS);
        assertConfStringIsPatched("https::addr=localhost:9000;foo=bar;auto_flush_interval=100;", "https::addr=localhost:9000;foo=bar;auto_flush=off;", DEFAULT_MAX_PENDING_ROWS, TimeUnit.MILLISECONDS.toNanos(100));
        assertConfStringIsPatched("https::addr=localhost:9000;foo=bar;auto_flush_interval=100;auto_flush_rows=42;", "https::addr=localhost:9000;foo=bar;auto_flush=off;",42, TimeUnit.MILLISECONDS.toNanos(100));

        // with escaped semi-colon
        assertConfStringIsPatched("https::addr=localhost:9000;foo=b;;ar;auto_flush_interval=100;auto_flush_rows=42;", "https::addr=localhost:9000;foo=b;;ar;auto_flush=off;",42, TimeUnit.MILLISECONDS.toNanos(100));


        assertConfStringIsNotPatched("https::addr=localhost:9000;auto_flush_interval=");
        assertConfStringIsNotPatched("https::addr=localhost:9000;auto_flush_rows=");
        assertConfStringIsNotPatched("https::addr=localhost:9000;auto_flush=");
        assertConfStringIsNotPatched("https::addr=localhost:9000;foo=bar"); // missing trailing semicolon
        assertConfStringIsNotPatched("https::addr=");
        assertConfStringIsNotPatched("https::addr");
        assertConfStringIsNotPatched("https");
        assertConfStringIsNotPatched("http!");
        assertConfStringIsNotPatched("tcp::addr=localhost:9000;");
        assertConfStringIsNotPatched("tcps::addr=localhost:9000;foo=bar;");
        assertConfStringIsNotPatched("tcps::addr=localhost:9000;auto_flush_rows=1;");
        assertConfStringIsNotPatched("tcps::addr=localhost:9000;auto_flush=on;");
        assertConfStringIsNotPatched("unknown::addr=localhost:9000;auto_flush=on;");

        assertConfStringPatchingThrowsConfigException("https::addr=localhost:9000;foo=bar;auto_flush=foo;", "Unknown auto_flush value [auto_flush=foo]");
        assertConfStringPatchingThrowsConfigException("https::addr=localhost:9000;foo=bar;auto_flush_interval=foo;", "Invalid auto_flush_interval value [auto_flush_interval=foo]");
        assertConfStringPatchingThrowsConfigException("https::addr=localhost:9000;foo=bar;auto_flush_rows=foo;", "Invalid auto_flush_rows value [auto_flush_rows=foo]");
        assertConfStringPatchingThrowsConfigException("https::addr=localhost:9000;foo=bar;auto_flush=off;", "QuestDB Kafka connector cannot have auto_flush disabled");
        assertConfStringPatchingThrowsConfigException("https::addr=localhost:9000;foo=bar;auto_flush_interval=off;", "QuestDB Kafka connector cannot have auto_flush_interval disabled");
        assertConfStringPatchingThrowsConfigException("https::addr=localhost:9000;foo=bar;auto_flush_rows=off;", "QuestDB Kafka connector cannot have auto_flush_rows disabled");
    }

    private static void assertConfStringIsPatched(String confStr, String expectedPatchedConfStr, long expectedMaxPendingRows, long expectedFlushNanos) {
        StringSink sink = new StringSink();
        FlushConfig flushConfig = new FlushConfig();
        ClientConfUtils.patchConfStr(confStr, sink, flushConfig);

        Assert.assertEquals(expectedPatchedConfStr, sink.toString());
        Assert.assertEquals(expectedMaxPendingRows, flushConfig.autoFlushRows);
        Assert.assertEquals(expectedFlushNanos, flushConfig.autoFlushNanos);
    }

    private static void assertConfStringIsNotPatched(String confStr) {
        StringSink sink = new StringSink();
        FlushConfig flushConfig = new FlushConfig();
        ClientConfUtils.patchConfStr(confStr, sink, flushConfig);

        assertEquals(confStr, sink.toString());
    }

    private static void assertConfStringPatchingThrowsConfigException(String confStr, String expectedMsg) {
        StringSink sink = new StringSink();
        FlushConfig flushConfig = new FlushConfig();
        try {
            ClientConfUtils.patchConfStr(confStr, sink, flushConfig);
            Assert.fail("Expected ConfigException");
        } catch (ConfigException e) {
            assertEquals(expectedMsg, e.getMessage());
        }
    }

}