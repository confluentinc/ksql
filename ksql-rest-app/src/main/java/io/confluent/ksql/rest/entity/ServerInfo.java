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
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Objects;

@JsonTypeInfo(include = JsonTypeInfo.As.WRAPPER_OBJECT, use = JsonTypeInfo.Id.NAME)
@JsonTypeName("KsqlServerInfo")
@JsonSubTypes({})
public class ServerInfo {
  private final String version;
  private final String kafkaClusterId;
  private final String ksqlServiceId;

  @JsonCreator
  public ServerInfo(
      @JsonProperty("version") String version,
      @JsonProperty("kafkaClusterId") String kafkaClusterId,
      @JsonProperty("ksqlServiceId") String ksqlServiceId) {
    this.version = version;
    this.kafkaClusterId = kafkaClusterId;
    this.ksqlServiceId = ksqlServiceId;
  }

  public String getVersion() {
    return version;
  }

  public String getKafkaClusterId() {
    return kafkaClusterId;
  }

  public String getKsqlServiceId() {
    return ksqlServiceId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ServerInfo that = (ServerInfo) o;
    return Objects.equals(version, that.version)
           && Objects.equals(kafkaClusterId, that.kafkaClusterId)
           && Objects.equals(ksqlServiceId, that.ksqlServiceId);
  }

  @Override
  public int hashCode() {

    return Objects.hash(version, kafkaClusterId);
  }
}
