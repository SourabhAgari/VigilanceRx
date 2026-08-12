package com.healthcare.rxvigilance.logging;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Collects log lines so a test can assert on them. Raises the level for the duration
 * and restores it on close, so a test asserting DEBUG output does not leave the whole
 * suite at DEBUG. Use it in try-with-resources.
 */
public class LogCapture implements AutoCloseable{

    private final List<String> lines = Collections.synchronizedList(new ArrayList<>());
    private final LoggerContext context = (LoggerContext) LogManager.getContext(false);
    private final LoggerConfig loggerConfig;
    private final Level previousLevel;
    private final Appender appender;

    public LogCapture(Class<?> loggerOwner, Level level) {
        loggerConfig = context.getConfiguration().getLoggerConfig(loggerOwner.getName());
        previousLevel = loggerConfig.getLevel();
        appender = new AbstractAppender("log-capture", null, null, true, Property.EMPTY_ARRAY) {
            @Override
            public void append(LogEvent event) {
                lines.add(event.getLevel() + " " + event.getMessage().getFormattedMessage());
            }
        };
        appender.start();
        loggerConfig.addAppender(appender, level, null);
        loggerConfig.setLevel(level);
        context.updateLoggers();
    }

    public List<String> lines() {
        return List.copyOf(lines);
    }

    @Override
    public void close() {
        loggerConfig.removeAppender(appender.getName());
        loggerConfig.setLevel(previousLevel);
        context.updateLoggers();
        appender.stop();
    }
}
