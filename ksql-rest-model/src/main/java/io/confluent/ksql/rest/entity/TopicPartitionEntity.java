/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
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
import com.google.errorprone.annotations.Immutable;
import java.util.Objects;

@Immutable
@JsonIgnoreProperties(ignoreUnknown = true)
public class TopicPartitionEntity {

  private final String topic;
  private final int partition;

  @JsonCreator
  public TopicPartitionEntity(
      @JsonProperty("topic") final String topic,
      @JsonProperty("partition") final int partition
  ) {
    this.topic = topic;
    this.partition = partition;
  }

  public String getTopic() {
    return topic;
  }

  public int getPartition() {
    return partition;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final TopicPartitionEntity that = (TopicPartitionEntity) o;
    return topic.equals(that.topic)
        && partition == that.partition;
  }

  @Override
  public int hashCode() {
    return Objects.hash(topic, partition);
  }

  @Override
  public String toString() {
    return "TopicPartition{"
        + "topic=" + topic
        + ", partition=" + partition
        + '}';
  }
}
