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
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class QueryOffsetSummary {
  private final String groupId;
  private final String kafkaTopic;
  private final List<ConsumerPartitionOffsets> offsets;

  @JsonCreator
  public QueryOffsetSummary(
      @JsonProperty("groupId") final String groupId,
      @JsonProperty("kafkaTopic") final String kafkaTopic,
      @JsonProperty("offsets") final List<ConsumerPartitionOffsets> offsets
  ) {
    this.groupId = Objects.requireNonNull(groupId);
    this.kafkaTopic = Objects.requireNonNull(kafkaTopic);
    this.offsets = ImmutableList.copyOf(Objects.requireNonNull(offsets, "offsets"));
  }

  public String getGroupId() {
    return groupId;
  }

  public String getKafkaTopic() {
    return kafkaTopic;
  }

  public List<ConsumerPartitionOffsets> getOffsets() {
    return offsets;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final QueryOffsetSummary that = (QueryOffsetSummary) o;
    return Objects.equals(groupId, that.groupId)
        && Objects.equals(kafkaTopic, that.kafkaTopic)
        && Objects.equals(offsets, that.offsets);
  }

  @Override
  public int hashCode() {
    return Objects.hash(groupId, kafkaTopic, offsets);
  }
}
