/*
 * Copyright 2018 Confluent Inc.
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

import java.util.Map;
import org.apache.kafka.common.TopicPartition;

public class StreamPullQueryMetadata {
  private final TransientQueryMetadata transientQueryMetadata;
  private final Map<TopicPartition, Long> endOffsets;

  public StreamPullQueryMetadata(
      final TransientQueryMetadata transientQueryMetadata,
      final Map<TopicPartition, Long> endOffsets) {
    this.transientQueryMetadata = transientQueryMetadata;
    this.endOffsets = endOffsets;
  }

  public TransientQueryMetadata getTransientQueryMetadata() {
    return transientQueryMetadata;
  }

  public Map<TopicPartition, Long> getEndOffsets() {
    return endOffsets;
  }
}
