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

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ConsumerPartitionOffsets {

  private final int partition;
  private final long logStartOffset;
  private final long logEndOffset;
  private final long consumerOffset;

  @JsonCreator
  public ConsumerPartitionOffsets(
      @JsonProperty("partition") final int partition,
      @JsonProperty("logStartOffset") final long logStartOffset,
      @JsonProperty("logEndOffset") final long logEndOffset,
      @JsonProperty("consumerOffset") final long consumerOffset
  ) {
    Preconditions.checkArgument(partition >= 0,
        "Invalid partition: " + partition);
    Preconditions.checkArgument(logStartOffset >= 0,
        "Invalid start offset: " + logStartOffset);
    Preconditions.checkArgument(logEndOffset >= 0,
        "Invalid end offset: " + logEndOffset);
    Preconditions.checkArgument(consumerOffset >= 0,
        "Invalid consumer offset: " + logEndOffset);
    this.partition = partition;
    this.logStartOffset = logStartOffset;
    this.logEndOffset = logEndOffset;
    this.consumerOffset = consumerOffset;
  }

  public int getPartition() {
    return partition;
  }

  public long getConsumerOffset() {
    return consumerOffset;
  }

  public long getLogStartOffset() {
    return logStartOffset;
  }

  public long getLogEndOffset() {
    return logEndOffset;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ConsumerPartitionOffsets that = (ConsumerPartitionOffsets) o;
    return partition == that.partition
        && logStartOffset == that.logStartOffset
        && logEndOffset == that.logEndOffset
        && consumerOffset == that.consumerOffset;
  }

  @Override
  public int hashCode() {
    return Objects.hash(partition, logStartOffset, logEndOffset, consumerOffset);
  }
}
