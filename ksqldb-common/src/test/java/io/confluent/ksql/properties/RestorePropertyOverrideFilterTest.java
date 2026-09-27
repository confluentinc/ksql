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
import static org.hamcrest.Matchers.hasEntry;
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

public class RestorePropertyOverrideFilterTest {

  private static final String LOGGER_NAME = RestorePropertyOverrideFilter.class.getName();
  private static final String ENDPOINT = "command_topic_restore";
  private static final String QUERY = "CSAS_FOO_0";
  private static final String DENIED = "sasl.jaas.config";
  private static final String OTHER = "auto.offset.reset";

  private TestAppender appender;
  private boolean addedLoggerConfig;

  @Before
  public void setUp() {
    appender = TestAppender.newBuilder()
        .setName("RestorePropertyOverrideFilterTest-Appender")
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
  }

  @Test
  public void shouldDropDenylistedKeyInDenylistMode() {
    // Given:
    final RestorePropertyOverrideFilter filter = denylistMode(DENIED);

    // When:
    final Map<String, Object> result = filter.filter(ENDPOINT, QUERY, ImmutableMap.of(
        DENIED, "boom",
        OTHER, "earliest"
    ));

    // Then:
    assertThat(result.size(), is(1));
    assertThat(result, hasEntry(OTHER, "earliest"));
  }

  @Test
  public void shouldKeepKeyNotOnDenylistInDenylistMode() {
    // Given:
    final RestorePropertyOverrideFilter filter = denylistMode(DENIED);

    // When:
    final Map<String, Object> result =
        filter.filter(ENDPOINT, QUERY, ImmutableMap.of(OTHER, "earliest"));

    // Then:
    assertThat(result.size(), is(1));
    assertThat(result, hasEntry(OTHER, "earliest"));
  }

  @Test
  public void shouldDropKeyNotOnAllowlistInAllowlistMode() {
    // Given:
    final RestorePropertyOverrideFilter filter = allowlistMode(OTHER);

    // When:
    final Map<String, Object> result = filter.filter(ENDPOINT, QUERY, ImmutableMap.of(
        OTHER, "earliest",
        "ksql.streams.num.stream.threads", "4"
    ));

    // Then:
    assertThat(result.size(), is(1));
    assertThat(result, hasEntry(OTHER, "earliest"));
  }

  @Test
  public void shouldKeepKeyOnAllowlistInAllowlistMode() {
    // Given:
    final RestorePropertyOverrideFilter filter = allowlistMode(OTHER);

    // When:
    final Map<String, Object> result =
        filter.filter(ENDPOINT, QUERY, ImmutableMap.of(OTHER, "earliest"));

    // Then:
    assertThat(result.size(), is(1));
    assertThat(result, hasEntry(OTHER, "earliest"));
  }

  @Test
  public void shouldIgnoreDenylistInAllowlistMode() {
    // Given: allowlist mode, with a key named on BOTH lists. Exactly one list applies, and in
    // allowlist mode it is the allowlist - so the key survives.
    final RestorePropertyOverrideFilter filter =
        new RestorePropertyOverrideFilter(new KsqlConfig(ImmutableMap.of(
            KsqlConfig.KSQL_PROPERTIES_OVERRIDES_VALIDATION_MODE,
            KsqlConfig.KSQL_PROPERTIES_OVERRIDES_VALIDATION_MODE_ALLOWLIST,
            KsqlConfig.KSQL_PROPERTIES_OVERRIDES_ALLOWLIST, DENIED,
            KsqlConfig.KSQL_PROPERTIES_OVERRIDES_DENYLIST, DENIED
        )));

    // When:
    final Map<String, Object> result =
        filter.filter(ENDPOINT, QUERY, ImmutableMap.of(DENIED, "boom"));

    // Then:
    assertThat(result.size(), is(1));
    assertThat(result, hasEntry(DENIED, "boom"));
  }

  @Test
  public void shouldUseDenylistAndIgnoreAllowlistWhenModeUnset() {
    // Given: no validation.mode set, so it defaults to denylist. Both lists are populated.
    final RestorePropertyOverrideFilter filter =
        new RestorePropertyOverrideFilter(new KsqlConfig(ImmutableMap.of(
            KsqlConfig.KSQL_PROPERTIES_OVERRIDES_DENYLIST, DENIED,
            KsqlConfig.KSQL_PROPERTIES_OVERRIDES_ALLOWLIST, OTHER
        )));

    // When:
    final Map<String, Object> result = filter.filter(ENDPOINT, QUERY, ImmutableMap.of(
        DENIED, "boom",
        "ksql.streams.num.stream.threads", "4"
    ));

    // Then: the denylisted key is dropped, and a key absent from the allowlist still survives.
    assertThat(result.size(), is(1));
    assertThat(result, hasEntry("ksql.streams.num.stream.threads", "4"));
  }

  @Test
  public void shouldDropEverythingWhenAllowlistEmptyInAllowlistMode() {
    // Given: in allowlist mode an empty list really does mean "nothing is permitted".
    final RestorePropertyOverrideFilter filter = allowlistMode("");

    // When:
    final Map<String, Object> result =
        filter.filter(ENDPOINT, QUERY, ImmutableMap.of(OTHER, "earliest"));

    // Then:
    assertThat(result.isEmpty(), is(true));
  }

  @Test
  public void shouldKeepEverythingWhenDenylistEmptyInDenylistMode() {
    // Given: an empty denylist must not be read as "nothing is permitted".
    final RestorePropertyOverrideFilter filter = denylistMode("");

    // When:
    final Map<String, Object> result = filter.filter(ENDPOINT, QUERY, ImmutableMap.of(
        OTHER, "earliest",
        "ksql.streams.num.stream.threads", "4"
    ));

    // Then:
    assertThat(result.size(), is(2));
  }

  @Test
  public void shouldReturnEmptyMapForEmptyOverrides() {
    // Given:
    final RestorePropertyOverrideFilter filter = denylistMode(DENIED);

    // When:
    final Map<String, Object> result =
        filter.filter(ENDPOINT, QUERY, Collections.emptyMap());

    // Then:
    assertThat(result.isEmpty(), is(true));
  }

  @Test
  public void shouldNotMutateInputMap() {
    // Given:
    final RestorePropertyOverrideFilter filter = denylistMode(DENIED);
    final Map<String, Object> input = ImmutableMap.of(
        DENIED, "boom",
        OTHER, "earliest"
    );

    // When:
    filter.filter(ENDPOINT, QUERY, input);

    // Then: the original map is untouched.
    assertThat(input.size(), is(2));
  }

  @Test
  public void shouldLogOneWarnPerDroppedKeyWithMdcFields() {
    // Given:
    final RestorePropertyOverrideFilter filter = denylistMode(DENIED);

    // When:
    filter.filter(ENDPOINT, QUERY, ImmutableMap.of(
        DENIED, "boom",
        OTHER, "earliest"
    ));

    // Then: only the dropped key is logged, and the variable fields ride on the MDC so JSON
    // layouts surface them as discrete indexable fields.
    final List<LogEvent> events = appender.getLog();
    assertThat(events, hasSize(1));
    assertThat(events.get(0).getLevel(), is(Level.WARN));
    assertThat(events.get(0).getMessage().getFormattedMessage(),
        is("Config override excluded from restore"));
    assertThat(events.get(0).getContextData().toMap(), is(ImmutableMap.of(
        "endpoint", ENDPOINT,
        "query", QUERY,
        "property", DENIED,
        "mode", "denylist"
    )));
  }

  @Test
  public void shouldNotLogWhenNothingDropped() {
    // Given:
    final RestorePropertyOverrideFilter filter = denylistMode(DENIED);

    // When:
    filter.filter(ENDPOINT, QUERY, ImmutableMap.of(OTHER, "earliest"));

    // Then:
    assertThat(appender.getLog(), empty());
  }

  @Test
  public void shouldClearThreadContextAfterLogging() {
    // Given:
    final RestorePropertyOverrideFilter filter = denylistMode(DENIED);

    // When:
    filter.filter(ENDPOINT, QUERY, ImmutableMap.of(DENIED, "boom"));

    // Then: CloseableThreadContext must remove its keys on exit, otherwise MDC values leak
    // into later log lines on the same thread.
    assertThat(ThreadContext.getContext().isEmpty(), is(true));
  }

  private static RestorePropertyOverrideFilter denylistMode(final String denylist) {
    return new RestorePropertyOverrideFilter(new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_PROPERTIES_OVERRIDES_DENYLIST, denylist
    )));
  }

  private static RestorePropertyOverrideFilter allowlistMode(final String allowlist) {
    return new RestorePropertyOverrideFilter(new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_PROPERTIES_OVERRIDES_VALIDATION_MODE,
        KsqlConfig.KSQL_PROPERTIES_OVERRIDES_VALIDATION_MODE_ALLOWLIST,
        KsqlConfig.KSQL_PROPERTIES_OVERRIDES_ALLOWLIST, allowlist
    )));
  }
}
