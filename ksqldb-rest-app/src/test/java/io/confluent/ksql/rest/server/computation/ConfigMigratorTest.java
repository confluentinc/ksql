/*
 * Copyright 2025 Confluent Inc.
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

import static io.confluent.ksql.rest.server.computation.ConfigMigrator.LEGACY_EXACTLY_ONCE;
import static io.confluent.ksql.util.KsqlConfig.KSQL_STREAMS_PREFIX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Test;

public class ConfigMigratorTest {

  @Test
  public void shouldMigrateExactlyOnceInOverwriteProperties() {
    // Given:
    final Map<String, Object> properties = new HashMap<>();
    properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, LEGACY_EXACTLY_ONCE);
    properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);

    // When:
    final Map<String, Object> result = ConfigMigrator.migrateOverwriteProperties(properties);

    // Then:
    assertThat(result, hasEntry(
        StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
        StreamsConfig.EXACTLY_ONCE_V2
    ));
    assertThat(result, hasEntry(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4));
  }

  @Test
  public void shouldMigrateExactlyOnceInOriginalProperties() {
    // Given:
    final Map<String, String> properties = new HashMap<>();
    properties.put(
        KSQL_STREAMS_PREFIX + StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
        LEGACY_EXACTLY_ONCE
    );
    properties.put(KSQL_STREAMS_PREFIX + StreamsConfig.APPLICATION_ID_CONFIG, "my_app");

    // When:
    final Map<String, String> result = ConfigMigrator.migrateOriginalProperties(properties);

    // Then:
    assertThat(result, hasEntry(
        KSQL_STREAMS_PREFIX + StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
        StreamsConfig.EXACTLY_ONCE_V2
    ));
    assertThat(result, hasEntry(
        KSQL_STREAMS_PREFIX + StreamsConfig.APPLICATION_ID_CONFIG,
        "my_app"
    ));
  }

  @Test
  public void shouldNotModifyExactlyOnceV2InOverwriteProperties() {
    // Given:
    final Map<String, Object> properties = ImmutableMap.of(
        StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2,
        StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4
    );

    // When:
    final Map<String, Object> result = ConfigMigrator.migrateOverwriteProperties(properties);

    // Then:
    assertThat(result, sameInstance(properties));
    assertThat(
        result.get(StreamsConfig.PROCESSING_GUARANTEE_CONFIG),
        is(StreamsConfig.EXACTLY_ONCE_V2)
    );
  }

  @Test
  public void shouldNotModifyExactlyOnceV2InOriginalProperties() {
    // Given:
    final Map<String, String> properties = ImmutableMap.of(
        KSQL_STREAMS_PREFIX + StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
        StreamsConfig.EXACTLY_ONCE_V2,
        KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG, "4"
    );

    // When:
    final Map<String, String> result = ConfigMigrator.migrateOriginalProperties(properties);

    // Then:
    assertThat(result, sameInstance(properties));
    assertThat(
        result.get(KSQL_STREAMS_PREFIX + StreamsConfig.PROCESSING_GUARANTEE_CONFIG),
        is(StreamsConfig.EXACTLY_ONCE_V2)
    );
  }

  @Test
  public void shouldNotModifyAtLeastOnce() {
    // Given:
    final Map<String, Object> properties = ImmutableMap.of(
        StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE,
        StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4
    );

    // When:
    final Map<String, Object> result = ConfigMigrator.migrateOverwriteProperties(properties);

    // Then:
    assertThat(result, sameInstance(properties));
    assertThat(
        result.get(StreamsConfig.PROCESSING_GUARANTEE_CONFIG),
        is(StreamsConfig.AT_LEAST_ONCE)
    );
  }

  @Test
  public void shouldNotModifyWhenKeyNotPresent() {
    // Given:
    final Map<String, Object> properties = ImmutableMap.of(
        StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4
    );

    // When:
    final Map<String, Object> result = ConfigMigrator.migrateOverwriteProperties(properties);

    // Then:
    assertThat(result, sameInstance(properties));
  }

  @Test
  public void shouldHandleEmptyMap() {
    // Given:
    final Map<String, Object> properties = ImmutableMap.of();

    // When:
    final Map<String, Object> result = ConfigMigrator.migrateOverwriteProperties(properties);

    // Then:
    assertThat(result, sameInstance(properties));
  }

  @Test
  public void shouldPreserveOriginalMapWhenNoMigrationNeeded() {
    // Given:
    final Map<String, Object> properties = ImmutableMap.of(
        StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2,
        StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4
    );

    // When:
    final Map<String, Object> result = ConfigMigrator.migrateOverwriteProperties(properties);

    // Then:
    assertThat("Should return same instance when no migration needed",
        result, sameInstance(properties));
  }

  @Test
  public void shouldCreateNewMapOnlyWhenMigrationNeeded() {
    // Given:
    final Map<String, Object> properties = ImmutableMap.of(
        StreamsConfig.PROCESSING_GUARANTEE_CONFIG, LEGACY_EXACTLY_ONCE
    );

    // When:
    final Map<String, Object> result = ConfigMigrator.migrateOverwriteProperties(properties);

    // Then:
    assertThat("Should create new map when migration needed",
        result != properties, is(true));
    assertThat(
        result.get(StreamsConfig.PROCESSING_GUARANTEE_CONFIG),
        is(StreamsConfig.EXACTLY_ONCE_V2)
    );
  }
}

