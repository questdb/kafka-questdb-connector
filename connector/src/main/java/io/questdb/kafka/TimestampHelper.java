package io.questdb.kafka;

import java.util.concurrent.TimeUnit;

public final class TimestampHelper {
    private TimestampHelper() {
    }

    static TimeUnit guessTimestampUnits(long timestamp) {
        if (timestamp < 10000000000000L) { // 11/20/2286, 5:46:40 PM in millis and 4/26/1970, 5:46:40 PM in micros
            return TimeUnit.MILLISECONDS;
        } else if (timestamp < 10000000000000000L) {
            return TimeUnit.MICROSECONDS;
        } else {
            return TimeUnit.NANOSECONDS;
        }
    }

    static TimeUnit getTimestampUnits(TimeUnit hint, long timestamp) {
        if (hint == null) {
            return guessTimestampUnits(timestamp);
        } else {
            return hint;
        }
    }
}
