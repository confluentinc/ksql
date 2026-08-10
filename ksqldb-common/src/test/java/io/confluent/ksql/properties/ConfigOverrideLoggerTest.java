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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
  private static final String RANGE_CHECKED_PROP = KsqlConfig.KSQL_QUERY_RETRY_BACKOFF_INITIAL_MS;

  private TestAppender appender;
  private boolean addedLoggerConfig;

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
      loggerConfig = new LoggerConfig(LOGGER_NAME, Level.DEBUG, false);
      config.addLogger(LOGGER_NAME, loggerConfig);
      addedLoggerConfig = true;
    }
    loggerConfig.setLevel(Level.DEBUG);
    loggerConfig.setAdditive(false);
    loggerConfig.addAppender(appender, Level.DEBUG, null);
    ctx.updateLoggers();
  }

  @After
  public void tearDown() {
    final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    final Configuration config = ctx.getConfiguration();
    config.getLoggerConfig(LOGGER_NAME).removeAppender(appender.getName());
    if (addedLoggerConfig) {
      config.removeLogger(LOGGER_NAME);
      addedLoggerConfig = false;
    }
    appender.stop();
    ctx.updateLoggers();
    ThreadContext.clearAll();
    ConfigOverrideLogger.reset();
  }

  @Test
  public void shouldNotLogOverridesWhenOverridesLogDisabled() {
    configureOverridesLog(false, "auto.offset.reset");

    ConfigOverrideLogger.logOverrides(ENDPOINT, ImmutableMap.of("auto.offset.reset", "earliest"));

    assertThat(appender.getLog(), empty());
  }

  @Test
  public void shouldEmitNoOverridesEventWhenPropertiesEmpty() {
    configureOverridesLog(true, "");

    ConfigOverrideLogger.logOverrides(ENDPOINT, Collections.emptyMap());

    final List<LogEvent> events = appender.getLog();
    assertThat(events, hasSize(1));
    assertThat(events.get(0).getMessage().getFormattedMessage(), is("No Config overrides"));
    assertThat(events.get(0).getContextData().toMap(),
        is(ImmutableMap.of("endpoint", "/ksql")));
  }

  @Test
  public void shouldEmitNoOverridesEventWhenPropertiesNull() {
    configureOverridesLog(true, "");

    ConfigOverrideLogger.logOverrides(ENDPOINT, null);

    final List<LogEvent> events = appender.getLog();
    assertThat(events, hasSize(1));
    assertThat(events.get(0).getMessage().getFormattedMessage(), is("No Config overrides"));
    assertThat(events.get(0).getContextData().toMap(),
        is(ImmutableMap.of("endpoint", "/ksql")));
  }

  @Test
  public void shouldLogOverrideWithInAllowlistTrueWhenOnAllowlist() {
    configureOverridesLog(true, "auto.offset.reset");

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
  public void shouldLogOverrideWithInAllowlistFalseWhenNotOnAllowlist() {
    configureOverridesLog(true, "auto.offset.reset");

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
    configureOverridesLog(true, "auto.offset.reset");

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
  public void shouldClearThreadContextAfterLoggingOverrides() {
    configureOverridesLog(true, "auto.offset.reset");

    ConfigOverrideLogger.logOverrides(ENDPOINT, ImmutableMap.of("auto.offset.reset", "earliest"));

    // CloseableThreadContext must remove its keys when the try-with-resources block exits,
    // otherwise MDC values leak into subsequent log lines on the same thread.
    assertThat(ThreadContext.getContext().isEmpty(), is(true));
  }

  @Test
  public void shouldLogOverrideWithQueryWhenSupplied() {
    configureOverridesLog(true, "auto.offset.reset");

    ConfigOverrideLogger.logOverrides("command_topic_restore", Optional.of("CSAS_FOO_0"),
        ImmutableMap.of("auto.offset.reset", "earliest"));

    final List<LogEvent> events = appender.getLog();
    assertThat(events, hasSize(1));
    assertThat(events.get(0).getMessage().getFormattedMessage(), is("Config overrides found"));
    assertThat(events.get(0).getContextData().toMap(), is(ImmutableMap.of(
        "endpoint", "command_topic_restore",
        "property", "auto.offset.reset",
        "inAllowlist", "true",
        "query", "CSAS_FOO_0"
    )));
  }

  @Test
  public void shouldNotLogRangeViolationWhenRangeValidationLogDisabled() {
    configureRangeValidationLog(false);

    ConfigOverrideLogger.logRangeViolations(ENDPOINT, ImmutableMap.of(RANGE_CHECKED_PROP, -5L));

    assertThat(appender.getLog(), empty());
  }

  @Test
  public void shouldLogRangeViolationWhenRangeValidationLogEnabled() {
    configureRangeValidationLog(true);

    ConfigOverrideLogger.logRangeViolations(ENDPOINT, ImmutableMap.of(RANGE_CHECKED_PROP, -5L));

    final List<LogEvent> events = appender.getLog();
    assertThat(events, hasSize(1));
    assertThat(events.get(0).getLevel(), is(Level.WARN));
    assertThat(events.get(0).getMessage().getFormattedMessage(),
        is("Config override outside intended range: must be between 100 and 60000"));
    assertThat(events.get(0).getContextData().toMap(), is(ImmutableMap.of(
        "endpoint", "/ksql",
        "property", RANGE_CHECKED_PROP,
        "value", "-5"
    )));
  }

  @Test
  public void shouldNotLogRangeViolationWhenValueInRange() {
    configureRangeValidationLog(true);

    ConfigOverrideLogger.logRangeViolations(ENDPOINT, ImmutableMap.of(RANGE_CHECKED_PROP, 100L));

    assertThat(appender.getLog(), empty());
  }

  @Test
  public void shouldNotLogRangeViolationAtMaximum() {
    configureRangeValidationLog(true);

    ConfigOverrideLogger.logRangeViolations(ENDPOINT,
        ImmutableMap.of(RANGE_CHECKED_PROP, 60_000L));

    assertThat(appender.getLog(), empty());
  }

  @Test
  public void shouldLogRangeViolationAboveMaximum() {
    configureRangeValidationLog(true);

    ConfigOverrideLogger.logRangeViolations(ENDPOINT,
        ImmutableMap.of(RANGE_CHECKED_PROP, 60_001L));

    assertThat(appender.getLog(), hasSize(1));
  }

  @Test
  public void shouldNotLogRangeViolationWhenExactValueMatched() {
    configureRangeValidationLog(true);

    ConfigOverrideLogger.logRangeViolations(ENDPOINT,
        ImmutableMap.of("num.stream.threads", 1));

    assertThat(appender.getLog(), empty());
  }

  @Test
  public void shouldLogRangeViolationWhenExactValueNotMatched() {
    configureRangeValidationLog(true);

    ConfigOverrideLogger.logRangeViolations(ENDPOINT,
        ImmutableMap.of("num.stream.threads", 4));

    final List<LogEvent> events = appender.getLog();
    assertThat(events, hasSize(1));
    assertThat(events.get(0).getMessage().getFormattedMessage(),
        is("Config override outside intended range: must be 1"));
  }

  @Test
  public void shouldLogOneEventPerViolatingProperty() {
    configureRangeValidationLog(true);

    ConfigOverrideLogger.logRangeViolations(ENDPOINT, ImmutableMap.of(
        RANGE_CHECKED_PROP, -5L,
        "max.poll.records", 20_000
    ));

    final List<LogEvent> events = appender.getLog();
    assertThat(events, hasSize(2));
    events.forEach(event -> {
      final Map<String, String> ctx = event.getContextData().toMap();
      assertThat(ctx.get("endpoint"), is("/ksql"));
      if (RANGE_CHECKED_PROP.equals(ctx.get("property"))) {
        assertThat(ctx.get("value"), is("-5"));
      } else {
        assertThat(ctx.get("property"), is("max.poll.records"));
        assertThat(ctx.get("value"), is("20000"));
      }
    });
  }

  @Test
  public void shouldNotLogRangeViolationForPropertyWithoutACheck() {
    configureRangeValidationLog(true);

    ConfigOverrideLogger.logRangeViolations(ENDPOINT,
        ImmutableMap.of("auto.offset.reset", "earliest"));

    assertThat(appender.getLog(), empty());
  }

  @Test
  public void shouldLogRangeViolationWithQueryWhenSupplied() {
    configureRangeValidationLog(true);

    ConfigOverrideLogger.logRangeViolations("command_topic_restore", Optional.of("CSAS_FOO_0"),
        ImmutableMap.of(RANGE_CHECKED_PROP, -5L));

    final List<LogEvent> events = appender.getLog();
    assertThat(events, hasSize(1));
    assertThat(events.get(0).getContextData().toMap(), is(ImmutableMap.of(
        "endpoint", "command_topic_restore",
        "property", RANGE_CHECKED_PROP,
        "value", "-5",
        "query", "CSAS_FOO_0"
    )));
  }

  @Test
  public void shouldLogRangeViolationWithoutQueryWhenNotSupplied() {
    configureRangeValidationLog(true);

    ConfigOverrideLogger.logRangeViolations(ENDPOINT, ImmutableMap.of(RANGE_CHECKED_PROP, -5L));

    final List<LogEvent> events = appender.getLog();
    assertThat(events, hasSize(1));
    assertThat(events.get(0).getContextData().toMap().containsKey("query"), is(false));
  }

  @Test
  public void shouldNotLogRangeViolationWhenPropertiesNull() {
    configureRangeValidationLog(true);

    ConfigOverrideLogger.logRangeViolations(ENDPOINT, null);

    assertThat(appender.getLog(), empty());
  }

  @Test
  public void shouldNotThrowWhenRangeCheckedValueIsNull() {
    configureRangeValidationLog(true);

    // A stored command can carry a null override value, and ConfigDef.parseType passes null
    // through rather than rejecting it - so the check must not blow up on it. This is log-only
    // code: throwing here would fail the request (or, on restore, server startup).
    final Map<String, Object> withNullValue = new HashMap<>();
    withNullValue.put(RANGE_CHECKED_PROP, null);

    ConfigOverrideLogger.logRangeViolations(ENDPOINT, withNullValue);

    assertThat(appender.getLog(), empty());
  }

  @Test
  public void shouldClearThreadContextAfterRangeLogging() {
    configureRangeValidationLog(true);

    ConfigOverrideLogger.logRangeViolations("command_topic_restore", Optional.of("CSAS_FOO_0"),
        ImmutableMap.of(RANGE_CHECKED_PROP, -5L));

    assertThat(ThreadContext.getContext().isEmpty(), is(true));
  }

  private static void configureOverridesLog(final boolean enabled, final String allowlist) {
    final Map<String, Object> overrides = ImmutableMap.of(
        KsqlConfig.KSQL_PROPERTIES_OVERRIDES_LOG, enabled,
        KsqlConfig.KSQL_PROPERTIES_OVERRIDES_ALLOWLIST, allowlist
    );
    ConfigOverrideLogger.configure(new KsqlConfig(overrides));
  }

  private static void configureRangeValidationLog(final boolean enabled) {
    ConfigOverrideLogger.configure(new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_PROPERTIES_OVERRIDES_RANGE_VALIDATION_LOG_ENABLED, enabled
    )));
  }
}
