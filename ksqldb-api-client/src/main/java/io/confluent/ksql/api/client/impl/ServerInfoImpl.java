/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.api.client.impl;

import io.confluent.ksql.api.client.ServerInfo;
import java.util.Objects;

public class ServerInfoImpl implements ServerInfo {
  private final String version;
  private final String kafkaClusterId;
  private final String ksqlServiceId;
  private final String serverStatus;

  public ServerInfoImpl(
      final String version,
      final String kafkaClusterId,
      final String ksqlServiceId,
      final String serverStatus
  ) {
    this.version = version;
    this.kafkaClusterId = kafkaClusterId;
    this.ksqlServiceId = ksqlServiceId;
    this.serverStatus = serverStatus;
  }

  @Override
  public String getVersion() {
    return version;
  }

  @Override
  public String getKafkaClusterId() {
    return kafkaClusterId;
  }

  @Override
  public String getKsqlServiceId() {
    return ksqlServiceId;
  }

  @Override
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
    final ServerInfoImpl that = (ServerInfoImpl) o;
    return version == that.version
        && kafkaClusterId.equals(that.kafkaClusterId)
        && ksqlServiceId.equals(that.ksqlServiceId)
        && serverStatus.equals(that.serverStatus);
  }

  @Override
  public int hashCode() {
    return Objects.hash(version, kafkaClusterId, ksqlServiceId, serverStatus);
  }

  @Override
  public String toString() {
    return "ServerInfo{"
        + "version='" + version + '\''
        + ", kafkaClusterId='" + kafkaClusterId + '\''
        + ", ksqlServiceId='" + ksqlServiceId + '\''
        + ", serverStatus='" + serverStatus + '\''
        + '}';
  }
}
