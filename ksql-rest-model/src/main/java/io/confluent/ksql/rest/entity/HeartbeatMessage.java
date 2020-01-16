/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
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
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSubTypes({})
public class HeartbeatMessage {

  private final HostInfoEntity hostInfo;
  private final long timestamp;

  @JsonCreator
  public HeartbeatMessage(@JsonProperty("hostInfo") final HostInfoEntity hostInfo,
                          @JsonProperty("timestamp") final long timestamp) {
    this.hostInfo = hostInfo;
    this.timestamp = timestamp;
  }

  public HostInfoEntity getHostInfo() {
    return hostInfo;
  }

  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public boolean equals(final Object other) {
    if (this == other) {
      return true;
    }

    if (!(other instanceof HeartbeatMessage)) {
      return false;
    }

    final HeartbeatMessage that = (HeartbeatMessage) other;
    return this.timestamp == that.timestamp && Objects.equals(hostInfo, that.hostInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(hostInfo, timestamp);
  }

  @Override
  public String toString() {
    return "HearbeatRequest{"
        + "hostInfo='" + hostInfo + '\''
        + "timestamp='" + timestamp + '\''
        + '}';
  }
}
