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

import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.kafka.common.TopicPartition;

public class StreamPullQueryMetadata {
  private final TransientQueryMetadata transientQueryMetadata;
  private final ImmutableMap<TopicPartition, Long> endOffsets;

  @SuppressFBWarnings(
      value = "EI_EXPOSE_REP2",
      justification = "See notes on getters."
  )
  public StreamPullQueryMetadata(
      final TransientQueryMetadata transientQueryMetadata,
      final ImmutableMap<TopicPartition, Long> endOffsets) {
    this.transientQueryMetadata = transientQueryMetadata;
    this.endOffsets = endOffsets;
  }


  @SuppressFBWarnings(
      value = "EI_EXPOSE_REP",
      justification = "TransientQueryMetadata is mutable, but it's what we need to pass around."
  )
  public TransientQueryMetadata getTransientQueryMetadata() {
    return transientQueryMetadata;
  }

  @SuppressFBWarnings(
      value = "EI_EXPOSE_REP",
      justification = "ImmutableMap is immutable. https://github.com/spotbugs/spotbugs/issues/1601"
  )
  public ImmutableMap<TopicPartition, Long> getEndOffsets() {
    return endOffsets;
  }
}
