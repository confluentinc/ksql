/*
 * Copyright 2026 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.test.util;

import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;

@Plugin(name = "TestAppender", category = "Core",
    elementType = Appender.ELEMENT_TYPE, printObject = true)
public class TestAppender extends AbstractAppender {
  private final List<LogEvent> log = new ArrayList<>();

  public TestAppender(final String name, final Layout<?> layout) {
    super(name, null, layout, false);
  }

  @PluginFactory
  public static TestAppender createAppender(
      @PluginAttribute("name") final String name,
      @PluginElement("Layout") final Layout<?> layout) {
    return new TestAppender(name, layout);
  }

  @Override
  public void append(final LogEvent event) {
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

    public Builder setName(final String name) {
      this.name = name;
      return this;
    }

    public Builder setLayout(final Layout<?> layout) {
      this.layout = layout;
      return this;
    }

    public TestAppender build() {
      return new TestAppender(name, layout);
    }
  }
}
