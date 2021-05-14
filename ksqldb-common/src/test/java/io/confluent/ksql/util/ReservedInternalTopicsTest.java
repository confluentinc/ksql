/*
 * Copyright 2021 Confluent Inc.
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.logging.processing.ProcessingLogConfig;
import java.util.List;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;

public class ReservedInternalTopicsTest {
  private static final String KSQL_PROCESSING_LOG_TOPIC = "default_ksql_processing_log";

  private ReservedInternalTopics internalTopics;
  private KsqlConfig ksqlConfig;

  @Before
  public void setUp() {
    ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_HIDDEN_TOPICS_CONFIG, "prefix_.*,literal,.*_suffix",
        KsqlConfig.KSQL_READONLY_TOPICS_CONFIG, "ro_prefix_.*,ro_literal,.*_suffix_ro"
    ));

    internalTopics = new ReservedInternalTopics(ksqlConfig);
  }


  @Test
  public void shouldReturnTrueOnAllHiddenTopics() {
    // Given
    final List<String> topicNames = ImmutableList.of(
        ReservedInternalTopics.KSQL_INTERNAL_TOPIC_PREFIX + "_test",
        "prefix_", "_suffix", "prefix_topic", "topic_suffix", "literal"
    );

    topicNames.forEach(topic -> {
      // When
      final boolean isHidden = internalTopics.isHidden(topic);

      // Then
      assertThat("Should return true on hidden topic: " + topic,
          isHidden, is(true));
    });
  }

  @Test
  public void shouldReturnTrueOnAllReadOnlyTopics() {
    // Given
    final List<String> topicNames = ImmutableList.of(
        KSQL_PROCESSING_LOG_TOPIC,
        ReservedInternalTopics.KSQL_INTERNAL_TOPIC_PREFIX + "_test",
        "ro_prefix_", "_suffix_ro", "ro_prefix_topic", "topic_suffix_ro", "ro_literal"
    );

    topicNames.forEach(topic -> {
      // When
      final boolean isReadOnly = internalTopics.isReadOnly(topic);

      // Then
      assertThat("Should return true on read-only topic: " + topic,
          isReadOnly, is(true));
    });
  }

  @Test
  public void shouldReturnFalseOnNonHiddenTopics() {
    // Given
    final List<String> topicNames = ImmutableList.of(
        KSQL_PROCESSING_LOG_TOPIC,
        "topic_prefix_", "_suffix_topic"
    );

    // Given
    topicNames.forEach(topic -> {
      // When
      final boolean isHidden = internalTopics.isHidden(topic);

      // Then
      assertThat("Should return false on non-hidden topic: " + topic,
          isHidden, is(false));
    });
  }

  @Test
  public void shouldReturnFalseOnNonReadOnlyTopics() {
    // Given
    final List<String> topicNames = ImmutableList.of(
        "topic_prefix_", "_suffix_topic"
    );

    // Given
    topicNames.forEach(topic -> {
      // When
      final boolean isReadOnly = internalTopics.isReadOnly(topic);

      // Then
      assertThat("Should return false on non read-only topic: " + topic,
          isReadOnly, is(false));
    });
  }

  @Test
  public void shouldRemoveAllHiddenTopics() {
    // Given
    final Set<String> topics = ImmutableSet.of(
        "prefix_name", "literal", "tt", "name1", "suffix", "p_suffix"
    );

    // When
    final Set<String> filteredTopics = internalTopics.removeHiddenTopics(topics);

    // Then
    assertThat(filteredTopics, is(ImmutableSet.of("tt", "name1", "suffix")));
  }

  @Test
  public void shouldReturnCommandTopic() {
    // Given/When
    final String commandTopic = ReservedInternalTopics.commandTopic(ksqlConfig);

    // Then
    assertThat(commandTopic, is("_confluent-ksql-default__command_topic"));
  }

  @Test
  public void shouldReturnConfigsTopic() {
    // Given/When
    final String commandTopic = ReservedInternalTopics.configsTopic(ksqlConfig);

    // Then
    assertThat(commandTopic, is("_confluent-ksql-default__configs"));
  }

  @Test
  public void shouldReturnProcessingLogTopic() {
    // Given/When
    final ProcessingLogConfig processingLogConfig = new ProcessingLogConfig(ImmutableMap.of());
    final String processingLogTopic = ReservedInternalTopics.processingLogTopic(
        processingLogConfig, ksqlConfig);

    // Then
    assertThat(processingLogTopic, is("default_ksql_processing_log"));
  }
}
