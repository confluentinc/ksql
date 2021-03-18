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

import com.google.common.collect.ImmutableSet;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.TaskMetadata;

@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamsTaskMetadata {

  private final String taskId;
  private final Set<TopicOffset> topicOffsets;
  private final Optional<Long> timeCurrentIdlingStarted;

  @SuppressWarnings("checkstyle:LineLength")
  @JsonCreator
  public StreamsTaskMetadata(
      @JsonProperty("taskId") final String taskId,
      @JsonProperty("topicOffsets") final Set<TopicOffset> topicOffsets,
      @JsonProperty("timeCurrentIdlingStarted") final Optional<Long> timeCurrentIdlingStarted
  ) {

    this.taskId = taskId;
    this.topicOffsets = ImmutableSet.copyOf(topicOffsets);
    this.timeCurrentIdlingStarted = timeCurrentIdlingStarted;
  }

  public static StreamsTaskMetadata fromStreamsTaskMetadata(final TaskMetadata taskMetadata) {
    final Set<TopicOffset> topicOffsets = new HashSet<>();
    for (TopicPartition topicPartition: taskMetadata.topicPartitions()) {
      topicOffsets.add(
          new TopicOffset(
              new TopicPartitionEntity(topicPartition.topic(), topicPartition.partition()),
              taskMetadata.endOffsets().getOrDefault(topicPartition, 0L),
              taskMetadata.committedOffsets().getOrDefault(topicPartition, 0L)
          )
      );
    }
    return new StreamsTaskMetadata(
        taskMetadata.taskId(),
        topicOffsets,
        taskMetadata.timeCurrentIdlingStarted());
  }

  public String getTaskId() {
    return taskId;
  }

  public Set<TopicOffset> getTopicOffsets() {
    return topicOffsets;
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
        && Objects.equals(topicOffsets, that.topicOffsets)
        && Objects.equals(timeCurrentIdlingStarted, that.timeCurrentIdlingStarted);
  }

  @Override
  public int hashCode() {
    return Objects.hash(taskId, topicOffsets, timeCurrentIdlingStarted);
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  static class TopicOffset {
    private final TopicPartitionEntity topicPartitionEntity;
    private final Long endOffset;
    private final Long committedOffset;

    @JsonCreator
    TopicOffset(
        @JsonProperty("topicPartitionEntity") final TopicPartitionEntity topicPartitionEntity,
        @JsonProperty("endOffset") final Long endOffset,
        @JsonProperty("committedOffset") final Long committedOffset
    ) {
      this.topicPartitionEntity = topicPartitionEntity;
      this.endOffset = endOffset;
      this.committedOffset = committedOffset;
    }

    public TopicPartitionEntity getTopicPartitionEntity() {
      return topicPartitionEntity;
    }

    public Long getCommittedOffset() {
      return committedOffset;
    }

    public Long getEndOffset() {
      return endOffset;
    }
  }

}
