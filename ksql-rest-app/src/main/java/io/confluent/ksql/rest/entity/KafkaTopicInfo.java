/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class KafkaTopicInfo {

  private final String name;
  private final String registered;
  private final String partitionCount;
  private final String replicaInfo;

  @JsonCreator
  public KafkaTopicInfo(
      @JsonProperty("name") String name,
      @JsonProperty("registered") String registered,
      @JsonProperty("partitionCount") String partitionCount,
      @JsonProperty("replicaInfo") String replicaInfo
  ) {
    this.name = name;
    this.registered = registered;
    this.partitionCount = partitionCount;
    this.replicaInfo = replicaInfo;
  }

  public String getName() {
    return name;
  }

  public String getRegistered() {
    return registered;
  }

  public String getPartitionCount() {
    return partitionCount;
  }

  public String getReplicaInfo() {
    return replicaInfo;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    KafkaTopicInfo that = (KafkaTopicInfo) o;
    return Objects.equals(name, that.name) &&
        Objects.equals(partitionCount, that.partitionCount) &&
        Objects.equals(replicaInfo, that.replicaInfo) &&
        Objects.equals(registered, that.registered);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, partitionCount, replicaInfo, registered);
  }
}