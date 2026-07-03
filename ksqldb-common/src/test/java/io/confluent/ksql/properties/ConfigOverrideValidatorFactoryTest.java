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
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Map;
import org.junit.Test;

public class ConfigOverrideValidatorFactoryTest {

  private static KsqlConfig config(final Map<String, Object> overrides) {
    return new KsqlConfig(overrides);
  }

  @Test
  public void shouldReturnDenyListValidatorByDefault() {
    // Given: no mode set (defaults to denylist)
    final ConfigOverrideValidator validator = ConfigOverrideValidatorFactory.forMode(
        config(ImmutableMap.of()));

    // Then:
    assertThat(validator, instanceOf(DenyListPropertyValidator.class));
  }

  @Test
  public void shouldReturnDenyListValidatorWhenModeIsDenylist() {
    // Given:
    final ConfigOverrideValidator validator = ConfigOverrideValidatorFactory.forMode(config(
        ImmutableMap.of(
            KsqlConfig.KSQL_PROPERTIES_OVERRIDES_VALIDATION_MODE, "denylist",
            KsqlConfig.KSQL_PROPERTIES_OVERRIDES_DENYLIST, "sasl.jaas.config")));

    // Then:
    assertThat(validator, instanceOf(DenyListPropertyValidator.class));
    assertThrows(
        KsqlException.class,
        () -> validator.validateAll(ImmutableMap.of("sasl.jaas.config", "x")));
  }

  @Test
  public void shouldReturnAllowListValidatorWhenModeIsAllowlist() {
    // Given:
    final ConfigOverrideValidator validator = ConfigOverrideValidatorFactory.forMode(config(
        ImmutableMap.of(
            KsqlConfig.KSQL_PROPERTIES_OVERRIDES_VALIDATION_MODE, "allowlist",
            KsqlConfig.KSQL_PROPERTIES_OVERRIDES_ALLOWLIST, "auto.offset.reset")));

    // Then:
    assertThat(validator, instanceOf(AllowListPropertyValidator.class));
    // allowlisted property passes:
    validator.validateAll(ImmutableMap.of("auto.offset.reset", "earliest"));
    // non-allowlisted property is rejected:
    assertThrows(
        KsqlException.class,
        () -> validator.validateAll(ImmutableMap.of("sasl.jaas.config", "x")));
  }

  @Test
  public void shouldReturnAllowListThatRejectsAllWhenAllowlistEmpty() {
    // Given: allowlist mode with no allowlist configured (fail-closed)
    final ConfigOverrideValidator validator = ConfigOverrideValidatorFactory.forMode(config(
        ImmutableMap.of(
            KsqlConfig.KSQL_PROPERTIES_OVERRIDES_VALIDATION_MODE, "allowlist")));

    // Then:
    assertThat(validator, instanceOf(AllowListPropertyValidator.class));
    assertThrows(
        KsqlException.class,
        () -> validator.validateAll(ImmutableMap.of("auto.offset.reset", "earliest")));
  }
}
