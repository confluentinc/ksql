package io.confluent.ksql.logging.query;

import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;


@Plugin(name = "TestAppender", category = "Core", elementType = Appender.ELEMENT_TYPE, printObject = true)
public class TestAppender extends AbstractAppender {
    private final List<LogEvent> log = new ArrayList<>();

    public TestAppender(String name, Layout<?> layout) {
        super(name, null, layout, false);
    }

    @PluginFactory
    public static TestAppender createAppender(
        @PluginAttribute("name") String name,
        @PluginElement("Layout") Layout<?> layout) {
        return new TestAppender(name, layout);
    }

    @Override
    public void append(LogEvent event) {
        log.add(event);
    }

    public List<LogEvent> getLog() {
        return new ArrayList<>(log);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private String name;
        private Layout<?> layout;

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setLayout(Layout<?> layout) {
            this.layout = layout;
            return this;
        }

        public TestAppender build() {
            return new TestAppender(name, layout);
        }
    }
}