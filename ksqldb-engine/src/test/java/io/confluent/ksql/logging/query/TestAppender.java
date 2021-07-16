package io.confluent.ksql.logging.query;

import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

public class TestAppender extends AppenderSkeleton {
    private final List<LoggingEvent> log = new ArrayList<>();
    @Override
    public boolean requiresLayout() {
        return false;
    }

    @Override
    protected void append(final LoggingEvent loggingEvent) {
        log.add(loggingEvent);
    }

    @Override
    public void close() {
    }

    public List<LoggingEvent> getLog() {
        return new ArrayList<>(log);
    }
}