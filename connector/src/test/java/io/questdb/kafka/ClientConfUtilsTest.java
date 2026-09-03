package io.questdb.kafka;

import io.questdb.client.Sender;
import io.questdb.client.std.str.StringSink;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

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
        assertTrue(ClientConfUtils.patchConfStr("ws::addr=localhost:9000;", sink, flushConfig));
        assertTrue(ClientConfUtils.patchConfStr("wss::addr=localhost:9000;", sink, flushConfig));
        assertTrue(ClientConfUtils.patchConfStr("https::addr=localhost:9000;", sink, flushConfig));
        assertFalse(ClientConfUtils.patchConfStr("tcp::addr=localhost:9000;", sink, flushConfig));
        assertFalse(ClientConfUtils.patchConfStr("tcps::addr=localhost:9000;", sink, flushConfig));

        assertTrue(ClientConfUtils.isQwp("ws::addr=localhost:9000;"));
        assertTrue(ClientConfUtils.isQwp("wss::addr=localhost:9000;"));
        assertFalse(ClientConfUtils.isQwp("http::addr=localhost:9000;"));
    }

    @Test
    public void testPatchedQwpFlushOwnershipIsAcceptedByClientParser() {
        StringSink sink = new StringSink();
        FlushConfig flushConfig = new FlushConfig();
        ClientConfUtils.patchConfStr(
                "ws::addr=localhost:9000;auto_flush=on;auto_flush_rows=42;auto_flush_interval=100;",
                sink,
                flushConfig
        );

        assertDoesNotThrow(() -> Sender.builder(sink));
        assertEquals(42, flushConfig.autoFlushRows);
        assertEquals(TimeUnit.MILLISECONDS.toNanos(100), flushConfig.autoFlushNanos);
    }

    @Test
    public void testClientConfPatching() {
        assertConfStringIsPatched("http::addr=localhost:9000;", "http::addr=localhost:9000;auto_flush=off;", DEFAULT_MAX_PENDING_ROWS, DEFAULT_FLUSH_INTERVAL_NANOS);
        assertConfStringIsPatched("https::addr=localhost:9000;foo=bar;", "https::addr=localhost:9000;foo=bar;auto_flush=off;", DEFAULT_MAX_PENDING_ROWS, DEFAULT_FLUSH_INTERVAL_NANOS);
        assertConfStringIsPatched("https::addr=localhost:9000;auto_flush_rows=1;", "https::addr=localhost:9000;auto_flush=off;",1, DEFAULT_FLUSH_INTERVAL_NANOS);
        assertConfStringIsPatched("https::addr=localhost:9000;auto_flush=on;", "https::addr=localhost:9000;auto_flush=off;", DEFAULT_MAX_PENDING_ROWS, DEFAULT_FLUSH_INTERVAL_NANOS);
        assertConfStringIsPatched("https::addr=localhost:9000;foo=bar;auto_flush_interval=100;", "https::addr=localhost:9000;foo=bar;auto_flush=off;", DEFAULT_MAX_PENDING_ROWS, TimeUnit.MILLISECONDS.toNanos(100));
        assertConfStringIsPatched("https::addr=localhost:9000;foo=bar;auto_flush_interval=100;auto_flush_rows=42;", "https::addr=localhost:9000;foo=bar;auto_flush=off;",42, TimeUnit.MILLISECONDS.toNanos(100));
        assertConfStringIsPatched("ws::addr=localhost:9000;auto_flush_interval=100;auto_flush_rows=42;", "ws::addr=localhost:9000;sf_append_deadline_millis=30000;auto_flush_bytes=16777216;close_flush_timeout_millis=0;", 42, TimeUnit.MILLISECONDS.toNanos(100));
        assertConfStringIsPatched("wss::addr=localhost:9000;sf_max_total_bytes=1048576;sf_append_deadline_millis=1234;auto_flush_bytes=104857600;", "wss::addr=localhost:9000;sf_max_total_bytes=1048576;sf_append_deadline_millis=1234;auto_flush_bytes=104857600;close_flush_timeout_millis=0;", DEFAULT_MAX_PENDING_ROWS, DEFAULT_FLUSH_INTERVAL_NANOS);
        assertConfStringIsPatched("ws::addr=localhost:9000;close_flush_timeout_millis=2345;", "ws::addr=localhost:9000;close_flush_timeout_millis=2345;sf_append_deadline_millis=30000;auto_flush_bytes=16777216;", DEFAULT_MAX_PENDING_ROWS, DEFAULT_FLUSH_INTERVAL_NANOS);

        // The trailing semicolon is optional in the client's grammar, and such a string is
        // parseable and usable - so it must be patched like any other. Skipping it silently
        // reverted the configured flush settings to defaults and left the client's own
        // auto-flush armed, which the connector's flush accounting assumes is off.
        assertConfStringIsPatched("https::addr=localhost:9000;foo=bar", "https::addr=localhost:9000;foo=bar;auto_flush=off;", DEFAULT_MAX_PENDING_ROWS, DEFAULT_FLUSH_INTERVAL_NANOS);
        assertConfStringIsPatched("http::addr=localhost:9000;auto_flush_rows=1000", "http::addr=localhost:9000;auto_flush=off;", 1000, DEFAULT_FLUSH_INTERVAL_NANOS);
        assertConfStringIsPatched("ws::addr=localhost:9000", "ws::addr=localhost:9000;sf_append_deadline_millis=30000;auto_flush_bytes=16777216;close_flush_timeout_millis=0;", DEFAULT_MAX_PENDING_ROWS, DEFAULT_FLUSH_INTERVAL_NANOS);

        // with escaped semi-colon
        assertConfStringIsPatched("https::addr=localhost:9000;foo=b;;ar;auto_flush_interval=100;auto_flush_rows=42;", "https::addr=localhost:9000;foo=b;;ar;auto_flush=off;",42, TimeUnit.MILLISECONDS.toNanos(100));


        assertConfStringIsNotPatched("https::addr=localhost:9000;auto_flush_interval=");
        assertConfStringIsNotPatched("https::addr=localhost:9000;auto_flush_rows=");
        assertConfStringIsNotPatched("https::addr=localhost:9000;auto_flush=");
        // An empty value still parses, so it is copied through and the client rejects it -
        // spotting that here would mean duplicating the client's own validation.
        assertConfStringIsPatched("https::addr=", "https::addr=;auto_flush=off;", DEFAULT_MAX_PENDING_ROWS, DEFAULT_FLUSH_INTERVAL_NANOS);
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
        assertConfStringPatchingThrowsConfigException("ws::addr=localhost:9000;auto_flush_interval=0;", "Invalid auto_flush_interval value [auto_flush_interval=0]");
        assertConfStringPatchingThrowsConfigException("ws::addr=localhost:9000;auto_flush_rows=0;", "Invalid auto_flush_rows value [auto_flush_rows=0]");
        assertConfStringPatchingThrowsConfigException("https::addr=localhost:9000;foo=bar;auto_flush=off;", "QuestDB Kafka connector cannot have auto_flush disabled");
        assertConfStringPatchingThrowsConfigException("https::addr=localhost:9000;foo=bar;auto_flush_interval=off;", "QuestDB Kafka connector cannot have auto_flush_interval disabled");
        assertConfStringPatchingThrowsConfigException("https::addr=localhost:9000;foo=bar;auto_flush_rows=off;", "QuestDB Kafka connector cannot have auto_flush_rows disabled");
        // auto_flush_bytes passes through to the QWP client, which clamps its effective byte trigger to the server batch cap
        assertConfStringIsPatched("ws::addr=localhost:9000;auto_flush_bytes=1024;", "ws::addr=localhost:9000;auto_flush_bytes=1024;sf_append_deadline_millis=30000;close_flush_timeout_millis=0;", 75_000, TimeUnit.SECONDS.toNanos(1));
        assertConfStringPatchingThrowsConfigException("ws::addr=localhost:9000;sf_dir=/var/lib/qdb;", "QuestDB Kafka connector supports memory-only store-and-forward; sf_dir is not allowed with QWP");
        assertConfStringPatchingThrowsConfigException("wss::addr=localhost:9000;sf_durability=sync;", "QuestDB Kafka connector supports memory-only store-and-forward; sf_durability is not allowed with QWP");
        assertConfStringPatchingThrowsConfigException("ws::addr=localhost:9000;sf_append_deadline_millis=invalid;", "Invalid sf_append_deadline_millis value [sf_append_deadline_millis=invalid]");
        assertConfStringIsPatched("ws::addr=localhost:9000;initial_connect_retry=off;", "ws::addr=localhost:9000;initial_connect_retry=off;sf_append_deadline_millis=30000;auto_flush_bytes=16777216;close_flush_timeout_millis=0;", DEFAULT_MAX_PENDING_ROWS, DEFAULT_FLUSH_INTERVAL_NANOS);
        assertConfStringIsPatched("ws::addr=localhost:9000;initial_connect_retry=FALSE;", "ws::addr=localhost:9000;initial_connect_retry=off;sf_append_deadline_millis=30000;auto_flush_bytes=16777216;close_flush_timeout_millis=0;", DEFAULT_MAX_PENDING_ROWS, DEFAULT_FLUSH_INTERVAL_NANOS);
        assertConfStringPatchingThrowsConfigException("ws::addr=localhost:9000;initial_connect_retry=sync;", "QuestDB Kafka connector requires initial_connect_retry=off for QWP");
        assertConfStringPatchingThrowsConfigException("ws::addr=localhost:9000;initial_connect_retry=on;", "QuestDB Kafka connector requires initial_connect_retry=off for QWP");
        assertConfStringPatchingThrowsConfigException("ws::addr=localhost:9000;initial_connect_retry=true;", "QuestDB Kafka connector requires initial_connect_retry=off for QWP");
        assertConfStringPatchingThrowsConfigException("ws::addr=localhost:9000;initial_connect_retry=async;", "QuestDB Kafka connector requires initial_connect_retry=off for QWP");
        assertConfStringPatchingThrowsConfigException("ws::addr=localhost:9000;initial_connect_retry=garbage;", "QuestDB Kafka connector requires initial_connect_retry=off for QWP");
        assertConfStringPatchingThrowsConfigException("ws::addr=localhost:9000;initial_connect_retry=;", "QuestDB Kafka connector requires initial_connect_retry=off for QWP");
    }

    private static void assertConfStringIsPatched(String confStr, String expectedPatchedConfStr, long expectedMaxPendingRows, long expectedFlushNanos) {
        StringSink sink = new StringSink();
        FlushConfig flushConfig = new FlushConfig();
        ClientConfUtils.patchConfStr(confStr, sink, flushConfig);

        if ((expectedPatchedConfStr.startsWith("ws::") || expectedPatchedConfStr.startsWith("wss::"))
                && !expectedPatchedConfStr.contains("initial_connect_retry=")) {
            expectedPatchedConfStr += "initial_connect_retry=off;";
        }
        if (expectedPatchedConfStr.startsWith("ws::") || expectedPatchedConfStr.startsWith("wss::")) {
            expectedPatchedConfStr += "auto_flush_rows=off;auto_flush_interval=off;";
        }
        assertEquals(expectedPatchedConfStr, sink.toString());
        assertEquals(expectedMaxPendingRows, flushConfig.autoFlushRows);
        assertEquals(expectedFlushNanos, flushConfig.autoFlushNanos);
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
            fail("Expected ConfigException");
        } catch (ConfigException e) {
            assertEquals(expectedMsg, e.getMessage());
        }
    }

}
