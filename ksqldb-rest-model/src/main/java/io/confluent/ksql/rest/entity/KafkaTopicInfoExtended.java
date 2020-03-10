/*
 * Copyright 2019 Confluent Inc.
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
import com.fasterxml.jackson.annotation.JsonSubTypes;
import java.util.List;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSubTypes({})
public class KafkaTopicInfoExtended {

  private final String name;
  private final List<Integer> replicaInfo;
  private final int consumerGroupCount;
  private final int consumerCount;

  @JsonCreator
  public KafkaTopicInfoExtended(
      @JsonProperty("name") final String name,
      @JsonProperty("replicaInfo") final List<Integer> replicaInfo,
      @JsonProperty("consumerCount") final int consumerCount,
      @JsonProperty("consumerGroupCount") final int consumerGroupCount
  ) {
    this.name = name;
    this.replicaInfo = replicaInfo;
    this.consumerGroupCount = consumerGroupCount;
    this.consumerCount = consumerCount;
  }

  public String getName() {
    return name;
  }

  public List<Integer> getReplicaInfo() {
    return replicaInfo;
  }

  public int getConsumerCount() {
    return consumerCount;
  }

  public int getConsumerGroupCount() {
    return consumerGroupCount;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final KafkaTopicInfoExtended that = (KafkaTopicInfoExtended) o;
    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }
}
