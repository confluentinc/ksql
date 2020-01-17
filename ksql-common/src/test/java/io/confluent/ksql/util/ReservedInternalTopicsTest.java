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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Set;
import java.util.regex.PatternSyntaxException;

import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ReservedInternalTopicsTest {
  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private ReservedInternalTopics internalTopics;
  private KsqlConfig ksqlConfig;

  @Before
  public void setUp() {
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.SYSTEM_INTERNAL_TOPICS_CONFIG, "prefix_.*,literal,.*_suffix"
    ));

    internalTopics = new ReservedInternalTopics(ksqlConfig);
  }


  @Test
  public void shouldReturnTrueOnAllInternalTopics() {
    // Given
    final List<String> topicNames = ImmutableList.of(
        "prefix_", "_suffix", "prefix_topic", "topic_suffix", "literal"
    );

    topicNames.forEach(topic -> {
      // When
      final boolean isReserved = internalTopics.isInternalTopic(topic);

      // Then
      assertThat("Should return true on internal topic: " + topic,
          isReserved, is(true));
    });
  }

  @Test
  public void shouldReturnFalseOnNonInternalTopics() {
    // Given
    final List<String> topicNames = ImmutableList.of(
        "topic_prefix_", "_suffix_topic"
    );

    // Given
    topicNames.forEach(topic -> {
      // When
      final boolean isReserved = internalTopics.isInternalTopic(topic);

      // Then
      assertThat("Should return false on non-internal topic: " + topic,
          isReserved, is(false));
    });
  }

  @Test
  public void shouldReturnTrueOnKsqlInternalTopics() {
    // Given
    final String ksqlInternalTopic = ReservedInternalTopics.KSQL_INTERNAL_TOPIC_PREFIX + "_test";

    // When
    final boolean isReserved =
        internalTopics.isInternalTopic(ksqlInternalTopic);

    // Then
    assertThat(isReserved, is(true));
  }

  @Test
  public void shouldFilterAllInternalTopics() {
    // Given
    final Set<String> topics = ImmutableSet.of(
        "prefix_name", "literal", "tt", "name1", "suffix", "p_suffix"
    );

    // When
    final Set<String> filteredTopics = internalTopics.filterInternalTopics(topics);

    // Then
    assertThat(filteredTopics, is(ImmutableSet.of("tt", "name1", "suffix")));
  }

  @Test
  public void shouldThrowWhenInvalidSystemTopicsListIsUsed() {
    // Given
    final KsqlConfig givenConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.SYSTEM_INTERNAL_TOPICS_CONFIG, "*_suffix"
    ));

    // Then
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Cannot get a list of system internal topics due to" +
        " an invalid configuration in '" + KsqlConfig.SYSTEM_INTERNAL_TOPICS_CONFIG + "'");

    // When
    new ReservedInternalTopics(givenConfig);
  }

  @Test
  public void shouldReturnCommandTopic() {
    // Given/When
    final String commandTopic = ReservedInternalTopics.commandTopic(ksqlConfig);

    // Then
    assertThat("_confluent-ksql-default__command_topic", is(commandTopic));
  }

  @Test
  public void shouldReturnConfigsTopic() {
    // Given/When
    final String commandTopic = ReservedInternalTopics.configsTopic(ksqlConfig);

    // Then
    assertThat("_confluent-ksql-default__configs", is(commandTopic));
  }
}
