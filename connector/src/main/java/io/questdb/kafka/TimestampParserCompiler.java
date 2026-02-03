package io.questdb.kafka;

import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.MicrosFormatCompiler;

class TimestampParserCompiler {
    private static MicrosFormatCompiler compiler;
    private static final Object MUTEX = new Object();
    // we assume that there will just a few patterns hence to issue with unbounded cache growth
    private static CharSequenceObjHashMap<DateFormat> cache;

    public static DateFormat compilePattern(String timestampPattern) {
        synchronized (MUTEX) {
            if (compiler == null) {
                compiler = new MicrosFormatCompiler();
                // DateFormat instances are thread-safe, so we can cache them and use for multiple workers
                cache = new CharSequenceObjHashMap<>();
            }
            DateFormat format = cache.get(timestampPattern);
            if (format != null) {
                return format;
            }
            format = compiler.compile(timestampPattern);
            cache.put(timestampPattern, format);
            return format;
        }
    }
}
