/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.util;

import com.google.errorprone.annotations.Immutable;
import java.util.Objects;

/**
 * Immutable representation of {@link org.apache.kafka.streams.state.HostInfo HostInfo}
 * from KStreams.
 */
@Immutable
public class KsqlHost {

  private final String host;
  private final int port;

  public KsqlHost(final String host, final int port) {
    this.host = host;
    this.port = port;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final KsqlHost hostInfo = (KsqlHost) o;
    return port == hostInfo.port && host.equals(hostInfo.host);
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, port);
  }

  public String host() {
    return host;
  }

  public int port() {
    return port;
  }

  @Override
  public String toString() {
    return "KsqlHost{host='" + this.host + '\'' + ", port=" + this.port + '}';
  }
}
