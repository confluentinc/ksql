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
  private final String serverVersion;
  private final String kafkaClusterId;
  private final String ksqlServiceId;

  public ServerInfoImpl(
      final String serverVersion,
      final String kafkaClusterId,
      final String ksqlServiceId
  ) {
    this.serverVersion = Objects.requireNonNull(serverVersion, "serverVersion");
    this.kafkaClusterId = Objects.requireNonNull(kafkaClusterId, "kafkaClusterId");
    this.ksqlServiceId = Objects.requireNonNull(ksqlServiceId, "ksqlServiceId");
  }

  @Override
  public String getServerVersion() {
    return serverVersion;
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
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ServerInfoImpl that = (ServerInfoImpl) o;
    return serverVersion.equals(that.serverVersion)
        && kafkaClusterId.equals(that.kafkaClusterId)
        && ksqlServiceId.equals(that.ksqlServiceId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(serverVersion, kafkaClusterId, ksqlServiceId);
  }

  @Override
  public String toString() {
    return "ServerInfo{"
        + "serverVersion='" + serverVersion + '\''
        + ", kafkaClusterId='" + kafkaClusterId + '\''
        + ", ksqlServiceId='" + ksqlServiceId + '\''
        + '}';
  }
}
