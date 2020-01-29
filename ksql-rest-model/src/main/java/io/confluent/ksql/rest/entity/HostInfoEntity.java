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
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import java.util.Objects;
import org.apache.kafka.streams.state.HostInfo;

@JsonIgnoreProperties(ignoreUnknown = true)
public class HostInfoEntity {

  private final String host;
  private final int port;

  public HostInfoEntity(
      final String host,
      final int port
  ) {
    this.host = Objects.requireNonNull(host, "host");
    this.port = Objects.requireNonNull(port, "port");
  }

  @JsonCreator
  public HostInfoEntity(final String serializedPair) {
    final String [] parts = serializedPair.split(":");
    Preconditions.checkArgument(parts.length == 2);
    this.host = Objects.requireNonNull(parts[0], "host");
    this.port = Integer.parseInt(parts[1]);
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public HostInfo toHostInfo() {
    return new HostInfo(host, port);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final HostInfoEntity that = (HostInfoEntity) o;
    return Objects.equals(host, that.host)
        && port == that.port;
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, port);
  }

  @JsonValue
  @Override
  public String toString() {
    return host + ":" + port;
  }
}
