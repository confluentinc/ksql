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
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ConfigOverrideLoggerTest {

  private static final String LOGGER_NAME = ConfigOverrideLogger.class.getName();
  private static final String ENDPOINT = "/ksql";

  private TestAppender appender;
  private Logger logger;

  @Before
  public void setUp() {
    appender = new TestAppender();
    appender.setName("ConfigOverrideLoggerTest-Appender");

    logger = Logger.getLogger(LOGGER_NAME);
    logger.setLevel(Level.DEBUG);
    logger.setAdditivity(false);
    logger.addAppender(appender);
  }

  @After
  public void tearDown() {
    logger.removeAppender(appender);
    MDC.clear();
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

    final List<LoggingEvent> events = appender.getLog();
    assertThat(events, hasSize(1));
    assertThat(events.get(0).getMessage(), is("No Config overrides"));
    assertThat(properties(events.get(0)), is(ImmutableMap.of("endpoint", "/ksql")));
  }

  @Test
  public void shouldEmitNoOverridesEventWhenPropertiesNull() {
    configure(true, "");

    ConfigOverrideLogger.logOverrides(ENDPOINT, null);

    final List<LoggingEvent> events = appender.getLog();
    assertThat(events, hasSize(1));
    assertThat(events.get(0).getMessage(), is("No Config overrides"));
    assertThat(properties(events.get(0)), is(ImmutableMap.of("endpoint", "/ksql")));
  }

  @Test
  public void shouldTagInAllowlistTrueWhenPropertyOnAllowlist() {
    configure(true, "auto.offset.reset");

    ConfigOverrideLogger.logOverrides(ENDPOINT, ImmutableMap.of("auto.offset.reset", "earliest"));

    final List<LoggingEvent> events = appender.getLog();
    assertThat(events, hasSize(1));
    assertThat(events.get(0).getMessage(), is("Config overrides found"));
    assertThat(properties(events.get(0)), is(ImmutableMap.of(
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

    final List<LoggingEvent> events = appender.getLog();
    assertThat(events, hasSize(1));
    assertThat(events.get(0).getMessage(), is("Config overrides found"));
    assertThat(properties(events.get(0)), is(ImmutableMap.of(
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

    final List<LoggingEvent> events = appender.getLog();
    assertThat(events, hasSize(2));
    events.forEach(e -> {
      final Map<String, String> ctx = properties(e);
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

    // ConfigOverrideLogger must remove its MDC keys once logging completes, otherwise
    // values leak into subsequent log lines on the same (pooled) worker thread.
    assertThat(MDC.getContext().isEmpty(), is(true));
  }

  private static void configure(final boolean enabled, final String allowlist) {
    final Map<String, Object> overrides = ImmutableMap.of(
        KsqlConfig.KSQL_PROPERTIES_OVERRIDES_LOG, enabled,
        KsqlConfig.KSQL_PROPERTIES_OVERRIDES_ALLOWLIST, allowlist
    );
    ConfigOverrideLogger.configure(new KsqlConfig(overrides));
  }

  @SuppressWarnings("unchecked")
  private static Map<String, String> properties(final LoggingEvent event) {
    return event.getProperties();
  }
}
