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

import java.util.Set;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.TaskMetadata;

public class StreamsTaskMetadata {

  private final String taskId;
  private final Set<TopicPartition> topicPartitions;

  StreamsTaskMetadata(final TaskMetadata taskMetadata) {
    this.taskId = taskMetadata.taskId();
    this.topicPartitions = taskMetadata.topicPartitions();
  }

  public String getTaskId() {
    return taskId;
  }

  public Set<TopicPartition> getTopicPartitions() {
    return topicPartitions;
  }
}
