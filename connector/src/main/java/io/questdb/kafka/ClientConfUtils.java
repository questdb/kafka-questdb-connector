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
    private static final int QWP_INTERNAL_FLUSH_INTERVAL_MILLIS = Integer.MAX_VALUE - 1;

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
        if (!hasUnescapedTrailingSemicolon(confStr)) {
            sink.put(confStr);
            return true;
        }
        sink.put(tmpSink).put("::");

        boolean hasAtLeastOneParam = false;
        boolean hasSfAppendDeadline = false;
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
            } else if (isQwpTransport && Chars.equals(tmpSink, "auto_flush_bytes")) {
                throw new ConfigException("QuestDB Kafka connector does not support auto_flush_bytes with QWP");
            } else if (isQwpTransport && (Chars.equals(tmpSink, "sf_dir") || Chars.equals(tmpSink, "sf_durability"))) {
                throw new ConfigException("QuestDB Kafka connector supports memory-only store-and-forward; " + tmpSink + " is not allowed with QWP");
            } else {
                // copy other params
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
        if (isQwpTransport) {
            // The 1.3.8 QWP client rejects auto_flush=off. Disable its row
            // trigger and leave the required timer at the largest finite value;
            // the connector remains responsible for normal publishing cadence.
            sink.put("auto_flush_rows=off;auto_flush_bytes=off;auto_flush_interval=").put(QWP_INTERNAL_FLUSH_INTERVAL_MILLIS).put(';');
        } else {
            sink.put("auto_flush=off;");
        }

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

    private static boolean hasUnescapedTrailingSemicolon(String confStr) {
        int semicolons = 0;
        for (int i = confStr.length() - 1; i >= 0 && confStr.charAt(i) == ';'; i--) {
            semicolons++;
        }
        return (semicolons & 1) == 1;
    }
}
