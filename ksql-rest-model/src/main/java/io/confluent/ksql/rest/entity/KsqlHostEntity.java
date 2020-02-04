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
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlHost;
import java.util.Objects;

@Immutable
@JsonIgnoreProperties(ignoreUnknown = true)
public class KsqlHostEntity {

  private final String host;
  private final int port;

  public KsqlHostEntity(
      final String host,
      final int port
  ) {
    this.host = Objects.requireNonNull(host, "host");
    this.port = Objects.requireNonNull(port, "port");
  }

  @JsonCreator
  public KsqlHostEntity(final String serializedPair) {
    final String [] parts = serializedPair.split(":");
    if (parts.length != 2) {
      throw new KsqlException("Invalid host info. Expected format: <hostname>:<port>, but was "
                                  + serializedPair);
    }

    this.host = Objects.requireNonNull(parts[0], "host");

    try {
      this.port = Integer.parseInt(parts[1]);
    } catch (final Exception e) {
      throw new KsqlException("Invalid port. Expected format: <hostname>:<port>, but was "
                                  + serializedPair, e);
    }
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public KsqlHost toKsqlHost() {
    return new KsqlHost(host, port);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final KsqlHostEntity that = (KsqlHostEntity) o;
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