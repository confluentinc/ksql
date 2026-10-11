/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.rolling.NoOpTriggeringPolicy;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.config.plugins.validation.constraints.Required;

@Plugin(name = "TimestampLogFileAppender",
    category = "Core",
    elementType = "appender",
    printObject = true)
public class TimestampLogFileAppender extends AbstractAppender {

  private static RollingFileAppender rollingFileAppender;

  protected TimestampLogFileAppender(final String name,
                                     final Layout<?> layout,
                                     final Filter filter,
                                     final boolean ignoreExceptions,
                                     final RollingFileAppender rollingFileAppender) {
    super(name, filter, layout, ignoreExceptions);
  }

  @PluginFactory
  public static TimestampLogFileAppender createAppender(
      @PluginAttribute("name") @Required final String name,
      @PluginAttribute("fileName") final String fileName,
      @PluginElement("Layout") final Layout<?> layout,
      @PluginElement("Filter") final Filter filter,
      @PluginAttribute("filePattern") final String filePattern,
      @PluginAttribute("ImmediateFlush") final boolean immediateFlush,
      @PluginAttribute("ignoreExceptions") final boolean ignoreExceptions) {

    String updatedFileName = fileName;
    if (updatedFileName.contains("%timestamp")) {
      final Date d = new Date();
      final SimpleDateFormat format = new SimpleDateFormat("yyMMdd-HHmmss");
      updatedFileName = updatedFileName.replaceAll("%timestamp", format.format(d));
    }

    rollingFileAppender = RollingFileAppender.newBuilder()
        .withFileName(updatedFileName)
        .withName(name)
        .withFilePattern(filePattern)
        .withIgnoreExceptions(ignoreExceptions)
        .withBufferedIo(true)
        .withBufferSize(8192)
        .withLayout(layout)
        .withFilter(filter)
        .withImmediateFlush(immediateFlush)
        .withPolicy(new NoOpTriggeringPolicy())
        .build();

    return new TimestampLogFileAppender(name,
                                        layout,
                                        filter,
                                        ignoreExceptions,
                                        rollingFileAppender);
  }

  @Override
  public void append(final LogEvent event) {
    rollingFileAppender.append(event);
  }
}