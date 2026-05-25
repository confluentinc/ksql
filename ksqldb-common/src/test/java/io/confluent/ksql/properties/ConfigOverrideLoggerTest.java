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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.util.KsqlConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ConfigOverrideLoggerTest {

  private static final String AUDIT_LOGGER_NAME = ConfigOverrideLogger.class.getName();
  private static final String ENDPOINT = "/ksql";

  private CapturingAppender appender;
  private Level priorLevel;
  private boolean priorAdditivity;

  @Before
  public void setUp() {
    appender = new CapturingAppender();
    appender.start();

    final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    final Configuration config = ctx.getConfiguration();
    LoggerConfig loggerConfig = config.getLoggerConfig(AUDIT_LOGGER_NAME);
    if (!AUDIT_LOGGER_NAME.equals(loggerConfig.getName())) {
      loggerConfig = new LoggerConfig(AUDIT_LOGGER_NAME, Level.INFO, false);
      config.addLogger(AUDIT_LOGGER_NAME, loggerConfig);
    }
    priorLevel = loggerConfig.getLevel();
    priorAdditivity = loggerConfig.isAdditive();
    loggerConfig.setLevel(Level.INFO);
    loggerConfig.setAdditive(false);
    loggerConfig.addAppender(appender, Level.INFO, null);
    ctx.updateLoggers();
  }

  @After
  public void tearDown() {
    final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    final Configuration config = ctx.getConfiguration();
    final LoggerConfig loggerConfig = config.getLoggerConfig(AUDIT_LOGGER_NAME);
    loggerConfig.removeAppender(appender.getName());
    loggerConfig.setLevel(priorLevel);
    loggerConfig.setAdditive(priorAdditivity);
    ctx.updateLoggers();
    appender.stop();
  }

  @Test
  public void shouldNotLogWhenDisabled() {
    final ConfigOverrideLogger logger = newLogger(false, "auto.offset.reset");

    logger.logOverrides(ENDPOINT, ImmutableMap.of("auto.offset.reset", "earliest"));

    assertThat(appender.messages(), empty());
  }

  @Test
  public void shouldLogNoOverridesLineWhenPropertiesEmpty() {
    final ConfigOverrideLogger logger = newLogger(true, "");

    logger.logOverrides(ENDPOINT, Collections.emptyMap());

    assertThat(
        appender.messages(),
        contains("event=no_property_overrides endpoint=/ksql")
    );
  }

  @Test
  public void shouldLogNoOverridesLineWhenPropertiesNull() {
    final ConfigOverrideLogger logger = newLogger(true, "");

    logger.logOverrides(ENDPOINT, null);

    assertThat(
        appender.messages(),
        contains("event=no_property_overrides endpoint=/ksql")
    );
  }

  @Test
  public void shouldTagInAllowlistTrueWhenPropertyOnAllowlist() {
    final ConfigOverrideLogger logger = newLogger(true, "auto.offset.reset");

    logger.logOverrides(ENDPOINT, ImmutableMap.of("auto.offset.reset", "earliest"));

    assertThat(
        appender.messages(),
        contains(
            "event=property_override endpoint=/ksql "
                + "property=auto.offset.reset inAllowlist=true"
        )
    );
  }

  @Test
  public void shouldTagInAllowlistFalseWhenPropertyNotOnAllowlist() {
    final ConfigOverrideLogger logger = newLogger(true, "auto.offset.reset");

    logger.logOverrides(ENDPOINT, ImmutableMap.of("ksql.streams.num.stream.threads", "4"));

    assertThat(
        appender.messages(),
        contains(
            "event=property_override endpoint=/ksql "
                + "property=ksql.streams.num.stream.threads inAllowlist=false"
        )
    );
  }

  @Test
  public void shouldEmitOneLineEachForMixedAllowlistMembership() {
    final ConfigOverrideLogger logger = newLogger(true, "auto.offset.reset");

    final Map<String, Object> properties = ImmutableMap.of(
        "auto.offset.reset", "earliest",
        "ksql.streams.num.stream.threads", "4"
    );
    logger.logOverrides(ENDPOINT, properties);

    assertThat(appender.messages(), hasSize(2));
    assertThat(
        appender.messages(),
        containsInAnyOrder(
            "event=property_override endpoint=/ksql "
                + "property=auto.offset.reset inAllowlist=true",
            "event=property_override endpoint=/ksql "
                + "property=ksql.streams.num.stream.threads inAllowlist=false"
        )
    );
  }

  @Test
  public void shouldNotCanonicalizePrefixVariants() {
    // PR 1 logs raw names; canonicalization is a follow-up PR.
    // A prefixed variant of an allowlisted base name MUST tag inAllowlist=false here.
    final ConfigOverrideLogger logger = newLogger(true, "sasl.jaas.config");

    logger.logOverrides(
        ENDPOINT,
        ImmutableMap.of("ksql.streams.consumer.sasl.jaas.config", "foo")
    );

    assertThat(
        appender.messages(),
        contains(
            "event=property_override endpoint=/ksql "
                + "property=ksql.streams.consumer.sasl.jaas.config inAllowlist=false"
        )
    );
  }

  private static ConfigOverrideLogger newLogger(final boolean enabled, final String allowlist) {
    final Map<String, Object> overrides = ImmutableMap.of(
        KsqlConfig.KSQL_PROPERTIES_OVERRIDES_LOG, enabled,
        KsqlConfig.KSQL_PROPERTIES_OVERRIDES_ALLOWLIST, allowlist
    );
    return new ConfigOverrideLogger(new KsqlConfig(overrides));
  }

  private static final class CapturingAppender extends AbstractAppender {

    private final List<LogEvent> events = new ArrayList<>();

    CapturingAppender() {
      super("ConfigOverrideLoggerTest-Appender", null, null, true, null);
    }

    @Override
    public void append(final LogEvent event) {
      events.add(event.toImmutable());
    }

    List<String> messages() {
      return events.stream()
          .map(e -> e.getMessage().getFormattedMessage())
          .collect(Collectors.toList());
    }
  }
}
