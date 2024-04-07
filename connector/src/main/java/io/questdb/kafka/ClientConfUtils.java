package io.questdb.kafka;

import io.questdb.client.impl.ConfStringParser;
import io.questdb.std.Chars;
import io.questdb.std.str.StringSink;

final class ClientConfUtils {
    private ClientConfUtils() {
    }

    static boolean patchConfStr(String confStr, StringSink sink) {
        int pos = ConfStringParser.of(confStr, sink);
        if (pos < 0) {
            sink.clear();
            sink.put(confStr);
            return false;
        }

        boolean isHttpTransport = Chars.equals(sink, "http") || Chars.equals(sink, "https");
        boolean intervalFlushSetExplicitly = false;
        boolean flushesDisabled = false;
        boolean parseError = false;
        boolean hasAtLeastOneParam = false;

        // disable interval based flushes
        // unless they are explicitly set or auto_flush is entirely off
        // why? the connector has its own mechanism to flush data in a timely manner
        while (ConfStringParser.hasNext(confStr, pos)) {
            hasAtLeastOneParam = true;
            pos = ConfStringParser.nextKey(confStr, pos, sink);
            if (pos < 0) {
                parseError = true;
                break;
            }
            if (Chars.equals(sink, "auto_flush_interval")) {
                intervalFlushSetExplicitly = true;
                pos = ConfStringParser.value(confStr, pos, sink);
            } else if (Chars.equals(sink, "auto_flush")) {
                pos = ConfStringParser.value(confStr, pos, sink);
                flushesDisabled = Chars.equals(sink, "off");
            } else {
                pos = ConfStringParser.value(confStr, pos, sink); // skip other values
            }
            if (pos < 0) {
                parseError = true;
                break;
            }
        }
        sink.clear();
        sink.put(confStr);
        if (!parseError // we don't want to mess with the config if there was a parse error
                && isHttpTransport // we only want to patch http transport
                && !flushesDisabled // if auto-flush is disabled we don't need to do anything
                && !intervalFlushSetExplicitly // if auto_flush_interval is set explicitly we don't want to override it
                && hasAtLeastOneParam // no parameter is also an error since at least address should be set. we let client throw exception in this case
        ) {
            // if everything is ok, we set auto_flush_interval to max value
            // this will effectively disable interval based flushes
            // and the connector will flush data only when it is told to do so by Connector
            // or if a row count limit is reached
            sink.put("auto_flush_interval=").put(Integer.MAX_VALUE).put(';');
        }

        return isHttpTransport;
    }
}
