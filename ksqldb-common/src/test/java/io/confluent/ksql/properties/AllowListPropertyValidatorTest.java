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
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Arrays;
import org.junit.Test;

public class AllowListPropertyValidatorTest {

  @Test
  public void shouldNotThrowWhenAllPropertiesAreAllowlisted() {
    // Given:
    final AllowListPropertyValidator validator = new AllowListPropertyValidator(Arrays.asList(
        "auto.offset.reset",
        "ksql.streams.num.standby.replicas"
    ));

    // When/Then (no throw):
    validator.validateAll(ImmutableMap.of(
        "auto.offset.reset", "earliest",
        "ksql.streams.num.standby.replicas", "1"
    ));
  }

  @Test
  public void shouldThrowOnPropertyNotOnAllowlist() {
    // Given:
    final AllowListPropertyValidator validator = new AllowListPropertyValidator(Arrays.asList(
        "auto.offset.reset"
    ));

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> validator.validateAll(ImmutableMap.of(
            "auto.offset.reset", "earliest",
            "sasl.jaas.config", "boom"
        ))
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "One or more property overrides set locally are not permitted by the KSQL server "
            + "allowlist (use UNSET to reset their default value): [sasl.jaas.config]"));
  }

  @Test
  public void shouldRejectEveryPropertyWhenAllowlistEmpty() {
    // Given:
    final AllowListPropertyValidator validator =
        new AllowListPropertyValidator(Arrays.asList());

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> validator.validateAll(ImmutableMap.of("auto.offset.reset", "earliest"))
    );

    // Then:
    assertThat(e.getMessage(), containsString("[auto.offset.reset]"));
  }

  @Test
  public void shouldNotThrowOnEmptyOverrides() {
    // Given:
    final AllowListPropertyValidator validator =
        new AllowListPropertyValidator(Arrays.asList());

    // When/Then (no throw):
    validator.validateAll(ImmutableMap.of());
  }

  @Test
  public void shouldRejectServiceIdEvenWhenAllowlisted() {
    // Given: an admin mistakenly allowlists the service id
    final AllowListPropertyValidator validator = new AllowListPropertyValidator(Arrays.asList(
        KsqlConfig.KSQL_SERVICE_ID_CONFIG
    ));

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> validator.validateAll(ImmutableMap.of(KsqlConfig.KSQL_SERVICE_ID_CONFIG, "x"))
    );

    // Then:
    assertThat(e.getMessage(), containsString(KsqlConfig.KSQL_SERVICE_ID_CONFIG));
  }

  @Test
  public void shouldRejectOverridePolicyKnobsEvenWhenAllowlisted() {
    // Given: an admin mistakenly allowlists the policy knobs
    final AllowListPropertyValidator validator = new AllowListPropertyValidator(Arrays.asList(
        KsqlConfig.KSQL_PROPERTIES_OVERRIDES_ALLOWLIST,
        KsqlConfig.KSQL_PROPERTIES_OVERRIDES_VALIDATION_MODE
    ));

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> validator.validateAll(ImmutableMap.of(
            KsqlConfig.KSQL_PROPERTIES_OVERRIDES_VALIDATION_MODE, "denylist"))
    );

    // Then:
    assertThat(e.getMessage(),
        containsString(KsqlConfig.KSQL_PROPERTIES_OVERRIDES_VALIDATION_MODE));
  }

  @Test
  public void shouldRejectWildcardAllowlistEntryAtConstruction() {
    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> new AllowListPropertyValidator(Arrays.asList("ksql.functions.*"))
    );

    // Then:
    assertThat(e.getMessage(), containsString("wildcard"));
    assertThat(e.getMessage(), containsString("ksql.functions.*"));
  }

  @Test
  public void shouldRejectQuestionMarkAllowlistEntryAtConstruction() {
    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> new AllowListPropertyValidator(Arrays.asList("auto.offset.rese?"))
    );

    // Then:
    assertThat(e.getMessage(), containsString("wildcard"));
  }
}
