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
import java.util.List;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SourceConsumerGroupOffsets {
  private final String groupId;
  private final String kafkaTopic;
  private final Long maxLag;
  private final List<SourceConsumerGroupOffset> offsets;

  @JsonCreator
  public SourceConsumerGroupOffsets(
      @JsonProperty("groupId") final String groupId,
      @JsonProperty("kafkaTopic") final String kafkaTopic,
      @JsonProperty("maxLag") final Long maxLag,
      @JsonProperty("offsets") final List<SourceConsumerGroupOffset> offsets
  ) {
    this.groupId = groupId;
    this.kafkaTopic = kafkaTopic;
    this.maxLag = maxLag;
    this.offsets = offsets;
  }

  public String getGroupId() {
    return groupId;
  }

  public String getKafkaTopic() {
    return kafkaTopic;
  }

  public Long getMaxLag() {
    return maxLag;
  }

  public List<SourceConsumerGroupOffset> getOffsets() {
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
    final SourceConsumerGroupOffsets that = (SourceConsumerGroupOffsets) o;
    return Objects.equals(groupId, that.groupId)
        && Objects.equals(kafkaTopic, that.kafkaTopic)
        && Objects.equals(maxLag, that.maxLag)
        && Objects.equals(offsets, that.offsets);
  }

  @Override
  public int hashCode() {
    return Objects.hash(groupId, kafkaTopic, maxLag, offsets);
  }
}
