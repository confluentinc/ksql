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

package io.confluent.ksql.properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.test.util.TestAppender;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ConfigOverrideLoggerTest {

  private static final String LOGGER_NAME = ConfigOverrideLogger.class.getName();
  private static final String ENDPOINT = "/ksql";

  private TestAppender appender;

  @Before
  public void setUp() {
    appender = TestAppender.newBuilder()
        .setName("ConfigOverrideLoggerTest-Appender")
        .setLayout(null)
        .build();
    appender.start();

    final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    final Configuration config = ctx.getConfiguration();
    LoggerConfig loggerConfig = config.getLoggerConfig(LOGGER_NAME);
    if (!LOGGER_NAME.equals(loggerConfig.getName())) {
      loggerConfig = new LoggerConfig(LOGGER_NAME, Level.INFO, false);
      config.addLogger(LOGGER_NAME, loggerConfig);
    }
    loggerConfig.setLevel(Level.INFO);
    loggerConfig.setAdditive(false);
    loggerConfig.addAppender(appender, Level.INFO, null);
    ctx.updateLoggers();
  }

  @After
  public void tearDown() {
    final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    final Configuration config = ctx.getConfiguration();
    config.getLoggerConfig(LOGGER_NAME).removeAppender(appender.getName());
    appender.stop();
    ctx.updateLoggers();
    ThreadContext.clearAll();
    ConfigOverrideLogger.reset();
  }

  @Test
  public void shouldNotLogWhenDisabled() {
    configure(false, "auto.offset.reset");

    ConfigOverrideLogger.logOverrides(ENDPOINT, ImmutableMap.of("auto.offset.reset", "earliest"));

    assertThat(appender.getLog(), empty());
  }

  @Test
  public void shouldEmitNoOverridesEventWhenPropertiesEmpty() {
    configure(true, "");

    ConfigOverrideLogger.logOverrides(ENDPOINT, Collections.emptyMap());

    final List<LogEvent> events = appender.getLog();
    assertThat(events, hasSize(1));
    assertThat(events.get(0).getMessage().getFormattedMessage(), is("No Config overrides"));
    assertThat(events.get(0).getContextData().toMap(),
        is(ImmutableMap.of("endpoint", "/ksql")));
  }

  @Test
  public void shouldEmitNoOverridesEventWhenPropertiesNull() {
    configure(true, "");

    ConfigOverrideLogger.logOverrides(ENDPOINT, null);

    final List<LogEvent> events = appender.getLog();
    assertThat(events, hasSize(1));
    assertThat(events.get(0).getMessage().getFormattedMessage(), is("No Config overrides"));
    assertThat(events.get(0).getContextData().toMap(),
        is(ImmutableMap.of("endpoint", "/ksql")));
  }

  @Test
  public void shouldTagInAllowlistTrueWhenPropertyOnAllowlist() {
    configure(true, "auto.offset.reset");

    ConfigOverrideLogger.logOverrides(ENDPOINT, ImmutableMap.of("auto.offset.reset", "earliest"));

    final List<LogEvent> events = appender.getLog();
    assertThat(events, hasSize(1));
    assertThat(events.get(0).getMessage().getFormattedMessage(), is("Config overrides found"));
    assertThat(events.get(0).getContextData().toMap(), is(ImmutableMap.of(
        "endpoint", "/ksql",
        "property", "auto.offset.reset",
        "inAllowlist", "true"
    )));
  }

  @Test
  public void shouldTagInAllowlistFalseWhenPropertyNotOnAllowlist() {
    configure(true, "auto.offset.reset");

    ConfigOverrideLogger.logOverrides(ENDPOINT,
        ImmutableMap.of("ksql.streams.num.stream.threads", "4"));

    final List<LogEvent> events = appender.getLog();
    assertThat(events, hasSize(1));
    assertThat(events.get(0).getMessage().getFormattedMessage(), is("Config overrides found"));
    assertThat(events.get(0).getContextData().toMap(), is(ImmutableMap.of(
        "endpoint", "/ksql",
        "property", "ksql.streams.num.stream.threads",
        "inAllowlist", "false"
    )));
  }

  @Test
  public void shouldEmitOneEventPerPropertyForMultipleOverrides() {
    configure(true, "auto.offset.reset");

    ConfigOverrideLogger.logOverrides(ENDPOINT, ImmutableMap.of(
        "auto.offset.reset", "earliest",
        "ksql.streams.num.stream.threads", "4"
    ));

    final List<LogEvent> events = appender.getLog();
    assertThat(events, hasSize(2));
    events.forEach(e -> {
      final Map<String, String> ctx = e.getContextData().toMap();
      assertThat(ctx.get("endpoint"), is("/ksql"));
      if ("auto.offset.reset".equals(ctx.get("property"))) {
        assertThat(ctx.get("inAllowlist"), is("true"));
      } else {
        assertThat(ctx.get("property"), is("ksql.streams.num.stream.threads"));
        assertThat(ctx.get("inAllowlist"), is("false"));
      }
    });
  }

  @Test
  public void shouldClearThreadContextAfterLogging() {
    configure(true, "auto.offset.reset");

    ConfigOverrideLogger.logOverrides(ENDPOINT, ImmutableMap.of("auto.offset.reset", "earliest"));

    // CloseableThreadContext must remove its keys when the try-with-resources block exits,
    // otherwise MDC values leak into subsequent log lines on the same thread.
    assertThat(ThreadContext.getContext().isEmpty(), is(true));
  }

  private static void configure(final boolean enabled, final String allowlist) {
    final Map<String, Object> overrides = ImmutableMap.of(
        KsqlConfig.KSQL_PROPERTIES_OVERRIDES_LOG, enabled,
        KsqlConfig.KSQL_PROPERTIES_OVERRIDES_ALLOWLIST, allowlist
    );
    ConfigOverrideLogger.configure(new KsqlConfig(overrides));
  }
}
