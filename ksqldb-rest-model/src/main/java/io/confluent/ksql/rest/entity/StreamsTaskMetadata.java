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

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ImmutableMap;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.streams.processor.TaskMetadata;

@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamsTaskMetadata {

  private final String taskId;
  private final Set<TopicPartitionEntity> topicPartitions;
  @JsonDeserialize(keyUsing = TopicPartitionEntity.TopicPartitionEntityDeserializer.class)
  private final ImmutableMap<TopicPartitionEntity, Long> endOffsets;
  @JsonDeserialize(keyUsing = TopicPartitionEntity.TopicPartitionEntityDeserializer.class)
  private final ImmutableMap<TopicPartitionEntity, Long> committedOffsets;
  private final Optional<Long> timeCurrentIdlingStarted;

  @SuppressWarnings("checkstyle:LineLength")
  @JsonCreator
  public StreamsTaskMetadata(
      @JsonProperty("taskId") final String taskId,
      @JsonProperty("topicPartitions") final Set<TopicPartitionEntity> topicPartitions,
      @JsonProperty("endOffsets") final ImmutableMap<TopicPartitionEntity, Long> endOffsets,
      @JsonProperty("committedOffsets") final ImmutableMap<TopicPartitionEntity, Long> committedOffsets,
      @JsonProperty("timeCurrentIdlingStarted") final Optional<Long> timeCurrentIdlingStarted
  ) {

    this.taskId = taskId;
    this.topicPartitions = topicPartitions;
    this.endOffsets = endOffsets;
    this.committedOffsets = committedOffsets;
    this.timeCurrentIdlingStarted = timeCurrentIdlingStarted;
  }

  public static StreamsTaskMetadata fromStreamsTaskMetadata(final TaskMetadata taskMetadata) {
    return new StreamsTaskMetadata(
        taskMetadata.taskId(),
        taskMetadata
            .topicPartitions()
            .stream()
            .map(t -> new TopicPartitionEntity(t.topic(), t.partition()))
            .collect(Collectors.toSet()),
        ImmutableMap.of(),
        ImmutableMap.of(),
        Optional.empty());
  }

  public String getTaskId() {
    return taskId;
  }

  public Set<TopicPartitionEntity> getTopicPartitions() {
    return topicPartitions;
  }

  public ImmutableMap<TopicPartitionEntity, Long> getEndOffsets() {
    return endOffsets;
  }

  public ImmutableMap<TopicPartitionEntity, Long> getCommittedOffsets() {
    return committedOffsets;
  }

  public Optional<Long> getTimeCurrentIdlingStarted() {
    return timeCurrentIdlingStarted;
  }

  // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
  @Override
  public boolean equals(final Object o) {
    // CHECKSTYLE_RULES.ON: CyclomaticComplexity
    if (this == o) {
      return true;
    }
    if (!(o instanceof StreamsTaskMetadata)) {
      return false;
    }
    final StreamsTaskMetadata that = (StreamsTaskMetadata) o;
    return Objects.equals(taskId, that.taskId)
        && Objects.equals(topicPartitions, that.topicPartitions)
        && Objects.equals(committedOffsets, that.committedOffsets)
        && Objects.equals(endOffsets, that.endOffsets)
        && Objects.equals(timeCurrentIdlingStarted, that.timeCurrentIdlingStarted);
  }

  @Override
  public int hashCode() {
    return Objects.hash(taskId, topicPartitions, committedOffsets, endOffsets, topicPartitions);
  }
}
