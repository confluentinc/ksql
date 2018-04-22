/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;

import java.util.List;
import java.util.Objects;

@JsonSubTypes({})
public class KafkaTopicInfo {

  private final String name;
  private final boolean registered;
  private final List<Integer> replicaInfo;
  private final int consumerGroupCount;
  private final int consumerCount;

  @JsonCreator
  public KafkaTopicInfo(
      @JsonProperty("name") String name,
      @JsonProperty("registered") boolean registered,
      @JsonProperty("replicaInfo") List<Integer> replicaInfo,
      @JsonProperty("consumerCount") int consumerCount,
      @JsonProperty("consumerGroupCount") int consumerGroupCount
  ) {
    this.name = name;
    this.registered = registered;
    this.replicaInfo = replicaInfo;
    this.consumerGroupCount = consumerGroupCount;
    this.consumerCount = consumerCount;
  }

  public String getName() {
    return name;
  }

  public boolean getRegistered() {
    return registered;
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
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    KafkaTopicInfo that = (KafkaTopicInfo) o;
    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }
}
