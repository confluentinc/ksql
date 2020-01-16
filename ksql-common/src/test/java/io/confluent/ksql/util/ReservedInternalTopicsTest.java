/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.util;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import java.util.Set;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ReservedInternalTopicsTest {
  private static final Set<String> ALL_INTERNAL_TOPICS_PREFIXES = ImmutableSet.of(
      // Confluent
      "_confluent",
      "__confluent"
  );

  private static final Set<String> ALL_INTERNAL_TOPICS_LITERALS = ImmutableSet.of(
      // Confluent
      "_secrets",

      // Kafka
      "__consumer_offsets",
      "__transaction_state",

      // Replicator
      "__consumer_timestamps",

      // Schema Registry
      "_schemas",

      // Connect
      "connect-configs",
      "connect-offsets",
      "connect-status",
      "connect-statuses"
  );

  @Test
  public void shouldReturnTrueOnAllInternalTopicsPrefixes() {
    // Given
    ALL_INTERNAL_TOPICS_PREFIXES.forEach(topic -> {
      // When
      final boolean isReserved = ReservedInternalTopics.isInternalTopic(topic + "_any_name");

      // Then
      assertThat(isReserved, is(true));
    });
  }

  @Test
  public void shouldReturnTrueOnAllInternalTopicsLiterals() {
    // Given
    ALL_INTERNAL_TOPICS_LITERALS.forEach(topic -> {
      // When
      final boolean isReserved = ReservedInternalTopics.isInternalTopic(topic);

      // Then
      assertThat(isReserved, is(true));
    });
  }

  @Test
  public void shouldReturnFalseOnAnyNonInternalTopic() {
    // Given
    ALL_INTERNAL_TOPICS_PREFIXES.forEach(topic -> {
      // When
      final boolean isReserved = ReservedInternalTopics.isInternalTopic("any_name_" + topic);

      // Then
      assertThat(isReserved, is(false));
    });
  }

  @Test
  public void shouldReturnTrueOnKsqlInternalTopics() {
    // Given
    final String ksqlInternalTopic = ReservedInternalTopics.KSQL_INTERNAL_TOPIC_PREFIX + "_test";

    // When
    final boolean isReserved = ReservedInternalTopics.isInternalTopic(ksqlInternalTopic);

    // Then
    assertThat(isReserved, is(true));
  }
}
