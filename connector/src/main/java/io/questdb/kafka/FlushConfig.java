package io.questdb.kafka;

import java.util.concurrent.TimeUnit;

final class FlushConfig {
    int autoFlushRows;
    long autoFlushNanos;
    long sfAppendDeadlineMillis;

    void reset() {
        autoFlushRows = 75_000;
        autoFlushNanos = TimeUnit.SECONDS.toNanos(1);
        sfAppendDeadlineMillis = ClientConfUtils.DEFAULT_QWP_SF_APPEND_DEADLINE_MILLIS;
    }
}
