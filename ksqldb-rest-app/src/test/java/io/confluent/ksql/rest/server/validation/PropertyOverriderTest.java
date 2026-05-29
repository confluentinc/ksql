/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.rest.server.validation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.UnsetProperty;
import io.confluent.ksql.properties.ConfigOverrideLogger;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.server.TemporaryEngine;

import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlHostInfo;
import io.confluent.ksql.util.KsqlStatementException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.collections4.map.HashedMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PropertyOverriderTest {

  @Rule
  public final TemporaryEngine engine = new TemporaryEngine();

  @Test
  public void shouldFailOnUnknownSetProperty() {
    // When:
    try (MockedStatic<ConfigOverrideLogger> configOverrideLogger =
        mockStatic(ConfigOverrideLogger.class)) {
      final Exception e = assertThrows(
          KsqlStatementException.class,
          () -> CustomValidators.SET_PROPERTY.validate(
              ConfiguredStatement.of(PreparedStatement.of(
                  "SET 'consumer.invalid'='value';",
                  new SetProperty(Optional.empty(), "consumer.invalid", "value")),
                  SessionConfig.of(engine.getKsqlConfig(), ImmutableMap.of())),
              mock(SessionProperties.class),
              engine.getEngine(),
              engine.getServiceContext()
          )
      );

      // Then: property validation throws BEFORE the log fires
      assertThat(e.getMessage(), containsString(
          "Unknown property: consumer.invalid"));
      configOverrideLogger.verifyNoInteractions();
    }
  }

  @Test
  public void shouldAllowSetKnownProperty() {
    // Given:
    final SessionProperties sessionProperties =
        new SessionProperties(new HashedMap<>(), mock(KsqlHostInfo.class), mock(URL.class), false);
    final Map<String, Object> properties = sessionProperties.getMutableScopedProperties();

    // When:
    try (MockedStatic<ConfigOverrideLogger> configOverrideLogger =
        mockStatic(ConfigOverrideLogger.class)) {
      CustomValidators.SET_PROPERTY.validate(
          ConfiguredStatement.of(PreparedStatement.of(
              "SET '" + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG + "' = 'earliest';",
              new SetProperty(Optional.empty(),
                  ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")),
              SessionConfig.of(engine.getKsqlConfig(), ImmutableMap.of())),
          sessionProperties,
          engine.getEngine(),
          engine.getServiceContext()
      );

      // Then:
      configOverrideLogger.verify(() -> ConfigOverrideLogger.logOverrides(
          "SET",
          ImmutableMap.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "")));
    }
    assertThat(properties, hasEntry(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
  }


  @Test
  public void shouldFailOnInvalidSetPropertyValue() {
    // Given:
    final SessionProperties sessionProperties =
        new SessionProperties(new HashedMap<>(), mock(KsqlHostInfo.class), mock(URL.class), false);

    // When:
    try (MockedStatic<ConfigOverrideLogger> configOverrideLogger =
        mockStatic(ConfigOverrideLogger.class)) {
      final Exception e = assertThrows(
          KsqlStatementException.class,
          () -> CustomValidators.SET_PROPERTY.validate(
              ConfiguredStatement.of(PreparedStatement.of(
                  "SET '" + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG + "' = 'invalid';",
                  new SetProperty(Optional.empty(), ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                      "invalid")), SessionConfig.of(engine.getKsqlConfig(), ImmutableMap.of())
              ),
              sessionProperties,
              engine.getEngine(),
              engine.getServiceContext()
          )
      );

      // Then: value-check throws BEFORE the log fires - bad-value attempts are noise and not LOGGED
      assertThat(e.getMessage(), containsString(
          "Invalid value invalid"));
      configOverrideLogger.verifyNoInteractions();
    }
  }

  @Test
  public void shouldFailOnUnknownUnsetProperty() {
    // Given:
    final Map<String, Object> properties = new HashMap<>();
    final SessionProperties sessionProperties =
        new SessionProperties(properties, mock(KsqlHostInfo.class), mock(URL.class), false);

    // When:
    try (MockedStatic<ConfigOverrideLogger> configOverrideLogger =
        mockStatic(ConfigOverrideLogger.class)) {
      final Exception e = assertThrows(
          KsqlStatementException.class,
          () -> CustomValidators.UNSET_PROPERTY.validate(
              ConfiguredStatement.of(PreparedStatement.of(
                  "UNSET 'consumer.invalid';",
                  new UnsetProperty(Optional.empty(), "consumer.invalid")),
                  SessionConfig.of(engine.getKsqlConfig(), new HashMap<>())),
              sessionProperties,
              engine.getEngine(),
              engine.getServiceContext()
          )
      );

      // Then: resolver throws BEFORE the log fires
      assertThat(e.getMessage(), containsString(
          "Unknown property: consumer.invalid"));
      configOverrideLogger.verifyNoInteractions();
    }
  }

  @Test
  public void shouldAllowUnsetKnownProperty() {
    // Given:
    final SessionProperties sessionProperties =
        new SessionProperties(
            Collections.singletonMap(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
            mock(KsqlHostInfo.class),
            mock(URL.class),
            false);
    final Map<String, Object> properties = sessionProperties.getMutableScopedProperties();

    // When:
    try (MockedStatic<ConfigOverrideLogger> configOverrideLogger =
        mockStatic(ConfigOverrideLogger.class)) {
      CustomValidators.UNSET_PROPERTY.validate(
          ConfiguredStatement.of(PreparedStatement.of(
              "UNSET '" + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG + "';",
              new UnsetProperty(Optional.empty(), ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)),
              SessionConfig.of(engine.getKsqlConfig(), ImmutableMap.of())),
          sessionProperties,
          engine.getEngine(),
          engine.getServiceContext()
      );

      // Then:
      configOverrideLogger.verify(() -> ConfigOverrideLogger.logOverrides(
          "UNSET",
          ImmutableMap.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "")));
    }
    assertThat(properties, not(hasKey(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)));
  }
}
