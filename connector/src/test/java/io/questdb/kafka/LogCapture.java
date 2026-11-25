package io.questdb.kafka;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** Collects what one class logs, as "LEVEL message" lines, from DEBUG up. */
final class LogCapture implements AutoCloseable {
    final List<String> lines = new CopyOnWriteArrayList<>();
    private final AppenderSkeleton appender = new AppenderSkeleton() {
        @Override
        protected void append(LoggingEvent event) {
            lines.add(event.getLevel() + " " + event.getRenderedMessage());
        }

        @Override
        public void close() {
        }

        @Override
        public boolean requiresLayout() {
            return false;
        }
    };
    private final Logger logger;
    private final Level previousLevel;

    LogCapture(Class<?> source) {
        logger = Logger.getLogger(source);
        previousLevel = logger.getLevel();
        logger.setLevel(Level.DEBUG);
        logger.addAppender(appender);
    }

    @Override
    public void close() {
        logger.removeAppender(appender);
        logger.setLevel(previousLevel);
    }
}
