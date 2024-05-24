package io.questdb.kafka;

import io.questdb.client.impl.ConfStringParser;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.str.StringSink;
import org.apache.kafka.common.config.ConfigException;

import java.util.concurrent.TimeUnit;

final class ClientConfUtils {
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
        if (!isHttpTransport) {
            sink.put(confStr);
            // no patching for TCP transport
            return false;
        }
        sink.put(tmpSink).put("::");

        boolean hasAtLeastOneParam = false;
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
                if (pos < 0) {
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
                if (pos < 0) {
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
                if (pos < 0) {
                    sink.clear();
                    sink.put(confStr);
                    return true;
                }
                if (Chars.equals(tmpSink, "off")) {
                    throw new ConfigException("QuestDB Kafka connector cannot have auto_flush disabled");
                } else if (!Chars.equals(tmpSink, "on")) {
                    throw new ConfigException("Unknown auto_flush value [auto_flush=" + tmpSink + ']');
                }
            } else {
                // copy other params
                sink.put(tmpSink).put('=');
                pos = ConfStringParser.value(confStr, pos, tmpSink);
                if (pos < 0) {
                    sink.clear();
                    sink.put(confStr);
                    return true;
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
        sink.put("auto_flush=off;");

        return true;
    }
}
