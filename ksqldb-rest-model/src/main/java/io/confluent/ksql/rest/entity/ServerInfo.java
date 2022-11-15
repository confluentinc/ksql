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
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(include = JsonTypeInfo.As.WRAPPER_OBJECT, use = JsonTypeInfo.Id.NAME)
@JsonTypeName("KsqlServerInfo")
@JsonSubTypes({})
public class ServerInfo {
  private final String version;
  private final String kafkaClusterId;
  private final String ksqlServiceId;
  private final String serverStatus;

  @JsonCreator
  public ServerInfo(
      @JsonProperty("version") final String version,
      @JsonProperty("kafkaClusterId") final String kafkaClusterId,
      @JsonProperty("ksqlServiceId") final String ksqlServiceId,
      @JsonProperty("serverStatus") final String serverStatus) {
    this.version = version;
    this.kafkaClusterId = kafkaClusterId;
    this.ksqlServiceId = ksqlServiceId;
    this.serverStatus = serverStatus;
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

  public String getServerStatus() {
    return serverStatus;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ServerInfo that = (ServerInfo) o;
    return Objects.equals(version, that.version) 
        && Objects.equals(kafkaClusterId, that.kafkaClusterId)
        && Objects.equals(ksqlServiceId, that.ksqlServiceId) 
        && Objects.equals(serverStatus, that.serverStatus);
  }

  @Override
  public int hashCode() {
    return Objects.hash(version, kafkaClusterId, ksqlServiceId, serverStatus);
  }
}
