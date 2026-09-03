package io.questdb.kafka;

import io.questdb.client.impl.ConfStringParser;
import io.questdb.client.std.Chars;
import io.questdb.client.std.Misc;
import io.questdb.client.std.Numbers;
import io.questdb.client.std.NumericException;
import io.questdb.client.std.str.StringSink;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import java.util.concurrent.TimeUnit;

final class ClientConfUtils {
    static final long DEFAULT_QWP_SF_APPEND_DEADLINE_MILLIS = 30_000L;
    // Any positive value arms the client's byte trigger, which it then clamps to 90% of the
    // server-advertised batch cap. The server never advertises more than the 16 MiB QWP protocol
    // ceiling, so this preserves the effective trigger without making the sender reserve two
    // 200 MiB native buffers before the handshake applies that clamp.
    static final int DEFAULT_QWP_AUTO_FLUSH_BYTES = 16 * 1024 * 1024;
    // Connect fixes the closing commit's offsets from preCommit() before it calls stop(). QWP
    // already publishes there and gives acks a bounded wait, so waiting again in Sender.close()
    // cannot make any additional offsets committable. Skip only that redundant close-time ack
    // wait by default; the sender still closes its resources, and an explicit client setting is
    // preserved. Unacknowledged offsets remain uncommitted and are redelivered by Kafka.
    static final long DEFAULT_QWP_CLOSE_FLUSH_TIMEOUT_MILLIS = 0L;

    private ClientConfUtils() {
    }


    static boolean patchConfStr(String confStr, StringSink sink, FlushConfig flushConfig) {
        flushConfig.reset();

        sink.clear();
        StringSink tmpSink = Misc.getThreadLocalSink();
        int pos = ConfStringParser.of(confStr, tmpSink);
        if (pos < 0) {
            sink.put(confStr);
            return false;
        }

        boolean isHttpTransport = Chars.equals(tmpSink, "http") || Chars.equals(tmpSink, "https");
        boolean isQwpTransport = Chars.equals(tmpSink, "ws") || Chars.equals(tmpSink, "wss");
        if (!isHttpTransport && !isQwpTransport) {
            sink.put(confStr);
            // no patching for TCP transport
            return false;
        }
        sink.put(tmpSink).put("::");

        boolean hasAtLeastOneParam = false;
        boolean hasSfAppendDeadline = false;
        boolean hasAutoFlushBytes = false;
        boolean hasCloseFlushTimeout = false;
        boolean hasInitialConnectRetry = false;
        while (ConfStringParser.hasNext(confStr, pos)) {
            hasAtLeastOneParam = true;
            pos = ConfStringParser.nextKey(confStr, pos, tmpSink);
            if (pos < 0) {
                sink.clear();
                sink.put(confStr);
                return true;
            }
            if (Chars.equals(tmpSink, "auto_flush_interval")) {
                pos = ConfStringParser.value(confStr, pos, tmpSink);
                if (pos < 0 || tmpSink.length() == 0) {
                    sink.clear();
                    sink.put(confStr);
                    // invalid config, let the real client parser to fail
                    return true;
                }
                if (Chars.equals(tmpSink, "off")) {
                    throw new ConfigException("QuestDB Kafka connector cannot have auto_flush_interval disabled");
                }
                try {
                    flushConfig.autoFlushNanos = TimeUnit.MILLISECONDS.toNanos(Numbers.parseLong(tmpSink));
                } catch (NumericException e) {
                    throw new ConfigException("Invalid auto_flush_interval value [auto_flush_interval=" + tmpSink + ']');
                }
                if (isQwpTransport) {
                    // one knob, two layers: the client frames at this cadence and
                    // the connector checkpoints its ledger at the same cadence
                    sink.put("auto_flush_interval=").put(tmpSink).put(';');
                }
            } else if (Chars.equals(tmpSink, "auto_flush_rows")) {
                pos = ConfStringParser.value(confStr, pos, tmpSink);
                if (pos < 0 || tmpSink.length() == 0) {
                    sink.clear();
                    sink.put(confStr);
                    return true;
                }
                if (Chars.equals(tmpSink, "off")) {
                    throw new ConfigException("QuestDB Kafka connector cannot have auto_flush_rows disabled");
                } else {
                    try {
                        flushConfig.autoFlushRows = Numbers.parseInt(tmpSink);
                    } catch (NumericException e) {
                        throw new ConfigException("Invalid auto_flush_rows value [auto_flush_rows=" + tmpSink + ']');
                    }
                }
                if (isQwpTransport) {
                    sink.put("auto_flush_rows=").put(tmpSink).put(';');
                }
            } else if (Chars.equals(tmpSink, "auto_flush")) {
                pos = ConfStringParser.value(confStr, pos, tmpSink);
                if (pos < 0 || tmpSink.length() == 0) {
                    sink.clear();
                    sink.put(confStr);
                    return true;
                }
                if (Chars.equals(tmpSink, "off")) {
                    throw new ConfigException("QuestDB Kafka connector cannot have auto_flush disabled");
                } else if (!Chars.equals(tmpSink, "on")) {
                    throw new ConfigException("Unknown auto_flush value [auto_flush=" + tmpSink + ']');
                }
                if (isQwpTransport) {
                    sink.put("auto_flush=").put(tmpSink).put(';');
                }
            } else if (isQwpTransport && (Chars.equals(tmpSink, "sf_dir") || Chars.equals(tmpSink, "sf_durability"))) {
                throw new ConfigException("QuestDB Kafka connector supports memory-only store-and-forward; " + tmpSink + " is not allowed with QWP");
            } else if (isQwpTransport && Chars.equals(tmpSink, "initial_connect_retry")) {
                pos = ConfStringParser.value(confStr, pos, tmpSink);
                if (pos < 0 || tmpSink.length() == 0) {
                    throw new ConfigException("QuestDB Kafka connector requires initial_connect_retry=off for QWP");
                }
                if (!Chars.equalsIgnoreCase(tmpSink, "off") && !Chars.equalsIgnoreCase(tmpSink, "false")) {
                    throw new ConfigException("QuestDB Kafka connector requires initial_connect_retry=off for QWP");
                }
                hasInitialConnectRetry = true;
                sink.put("initial_connect_retry=off;");
            } else {
                // copy other params
                if (isQwpTransport && Chars.equals(tmpSink, "auto_flush_bytes")) {
                    hasAutoFlushBytes = true;
                } else if (isQwpTransport && Chars.equals(tmpSink, "close_flush_timeout_millis")) {
                    hasCloseFlushTimeout = true;
                }
                boolean isSfAppendDeadline = isQwpTransport && Chars.equals(tmpSink, "sf_append_deadline_millis");
                sink.put(tmpSink).put('=');
                pos = ConfStringParser.value(confStr, pos, tmpSink);
                if (pos < 0) {
                    sink.clear();
                    sink.put(confStr);
                    return true;
                }
                if (isSfAppendDeadline) {
                    hasSfAppendDeadline = true;
                    try {
                        flushConfig.sfAppendDeadlineMillis = Numbers.parseLong(tmpSink);
                    } catch (NumericException e) {
                        throw new ConfigException("Invalid sf_append_deadline_millis value [sf_append_deadline_millis=" + tmpSink + ']');
                    }
                }
                for (int i = 0; i < tmpSink.length(); i++) {
                    char ch = tmpSink.charAt(i);
                    sink.put(ch);
                    // re-escape semicolon
                    if (ch == ';') {
                        sink.put(';');
                    }
                }
                sink.put(';');
            }
        }
        if (!hasAtLeastOneParam) {
            // this is invalid, let the real client parser to fail
            sink.clear();
            sink.put(confStr);
            return true;
        }
        if (isQwpTransport && !hasSfAppendDeadline) {
            sink.put("sf_append_deadline_millis=").put(DEFAULT_QWP_SF_APPEND_DEADLINE_MILLIS).put(';');
        }
        if (isQwpTransport && !hasAutoFlushBytes) {
            sink.put("auto_flush_bytes=").put(DEFAULT_QWP_AUTO_FLUSH_BYTES).put(';');
        }
        if (isQwpTransport && !hasCloseFlushTimeout) {
            sink.put("close_flush_timeout_millis=").put(DEFAULT_QWP_CLOSE_FLUSH_TIMEOUT_MILLIS).put(';');
        }
        if (isQwpTransport && !hasInitialConnectRetry) {
            // reconnect_* settings otherwise promote the initial connection to a blocking
            // retry loop. Connect must own startup retry and retain the current poll batch.
            sink.put("initial_connect_retry=off;");
        }
        if (!isQwpTransport) {
            sink.put("auto_flush=off;");
        }
        // QWP keeps the client's own auto-flush: its effective byte trigger is
        // clamped to the server's advertised batch cap, which is what prevents
        // BatchTooLargeForCapException for multi-row batches. The connector's
        // flush cadence is only a ledger/commit checkpoint on top of it.

        return true;
    }

    static boolean isQwp(String confStr) {
        StringSink scheme = Misc.getThreadLocalSink();
        return ConfStringParser.of(confStr, scheme) >= 0
                && (Chars.equals(scheme, "ws") || Chars.equals(scheme, "wss"));
    }

    static String resolveConfString(QuestDBSinkConnectorConfig config) {
        Password configured = config.getConfigurationString();
        String confStr = configured == null ? null : configured.value();
        if (confStr == null || confStr.isEmpty()) {
            confStr = System.getenv("QDB_CLIENT_CONF");
        }
        if (confStr == null || confStr.isEmpty()) {
            return null;
        }
        return ConfStringEnvInterpolator.expand(confStr);
    }

}
