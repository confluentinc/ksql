/*
 * Copyright 2024 Confluent Inc.
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

package io.confluent.ksql.rest.server.computation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class ConfigMigratorTest {

  @Test
  public void shouldMigrateExactlyOnceToExactlyOnceV2() {
    // Given:
    final Map<String, Object> properties = new HashMap<>();
    properties.put("processing.guarantee", "exactly_once");
    properties.put("num.stream.threads", 4);

    // When:
    final Map<String, Object> result = ConfigMigrator.migrateProcessingGuarantee(
        properties,
        "processing.guarantee"
    );

    // Then:
    assertThat(result, hasEntry("processing.guarantee", "exactly_once_v2"));
    assertThat(result, hasEntry("num.stream.threads", 4));
  }

  @Test
  public void shouldNotModifyExactlyOnceV2() {
    // Given:
    final Map<String, String> properties = ImmutableMap.of(
        "ksql.streams.processing.guarantee", "exactly_once_v2",
        "ksql.streams.num.stream.threads", "4"
    );

    // When:
    final Map<String, String> result = ConfigMigrator.migrateProcessingGuarantee(
        properties,
        "ksql.streams.processing.guarantee"
    );

    // Then:
    assertThat(result, sameInstance(properties));
    assertThat(result.get("ksql.streams.processing.guarantee"), is("exactly_once_v2"));
  }

  @Test
  public void shouldNotModifyAtLeastOnce() {
    // Given:
    final Map<String, Object> properties = ImmutableMap.of(
        "processing.guarantee", "at_least_once",
        "num.stream.threads", 4
    );

    // When:
    final Map<String, Object> result = ConfigMigrator.migrateProcessingGuarantee(
        properties,
        "processing.guarantee"
    );

    // Then:
    assertThat(result, sameInstance(properties));
    assertThat(result.get("processing.guarantee"), is("at_least_once"));
  }

  @Test
  public void shouldNotModifyWhenKeyNotPresent() {
    // Given:
    final Map<String, Object> properties = ImmutableMap.of(
        "num.stream.threads", 4
    );

    // When:
    final Map<String, Object> result = ConfigMigrator.migrateProcessingGuarantee(
        properties,
        "processing.guarantee"
    );

    // Then:
    assertThat(result, sameInstance(properties));
  }

  @Test
  public void shouldHandleEmptyMap() {
    // Given:
    final Map<String, Object> properties = ImmutableMap.of();

    // When:
    final Map<String, Object> result = ConfigMigrator.migrateProcessingGuarantee(
        properties,
        "processing.guarantee"
    );

    // Then:
    assertThat(result, sameInstance(properties));
  }

  @Test
  public void shouldWorkWithDifferentKeyForOriginalProperties() {
    // Given:
    final Map<String, String> properties = new HashMap<>();
    properties.put("ksql.streams.processing.guarantee", "exactly_once");
    properties.put("ksql.streams.application.id", "my_app");

    // When:
    final Map<String, String> result = ConfigMigrator.migrateProcessingGuarantee(
        properties,
        "ksql.streams.processing.guarantee"
    );

    // Then:
    assertThat(result, hasEntry("ksql.streams.processing.guarantee", "exactly_once_v2"));
    assertThat(result, hasEntry("ksql.streams.application.id", "my_app"));
  }

  @Test
  public void shouldWorkWithDifferentKeyForOverwriteProperties() {
    // Given:
    final Map<String, Object> properties = new HashMap<>();
    properties.put("processing.guarantee", "exactly_once");
    properties.put("application.id", "my_app");

    // When:
    final Map<String, Object> result = ConfigMigrator.migrateProcessingGuarantee(
        properties,
        "processing.guarantee"
    );

    // Then:
    assertThat(result, hasEntry("processing.guarantee", "exactly_once_v2"));
    assertThat(result, hasEntry("application.id", "my_app"));
  }

  @Test
  public void shouldPreserveOriginalMapWhenNoMigrationNeeded() {
    // Given:
    final Map<String, Object> properties = ImmutableMap.of(
        "processing.guarantee", "exactly_once_v2",
        "num.stream.threads", 4
    );

    // When:
    final Map<String, Object> result = ConfigMigrator.migrateProcessingGuarantee(
        properties,
        "processing.guarantee"
    );

    // Then:
    assertThat("Should return same instance when no migration needed",
        result, sameInstance(properties));
  }

  @Test
  public void shouldCreateNewMapOnlyWhenMigrationNeeded() {
    // Given:
    final Map<String, Object> properties = ImmutableMap.of(
        "processing.guarantee", "exactly_once"
    );

    // When:
    final Map<String, Object> result = ConfigMigrator.migrateProcessingGuarantee(
        properties,
        "processing.guarantee"
    );

    // Then:
    assertThat("Should create new map when migration needed",
        result != properties, is(true));
    assertThat(result.get("processing.guarantee"), is("exactly_once_v2"));
  }
}

