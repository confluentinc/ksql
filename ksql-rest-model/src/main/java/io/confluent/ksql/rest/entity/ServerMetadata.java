/*
 * Copyright 2019 Confluent Inc.
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
import com.google.errorprone.annotations.Immutable;

import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
@Immutable
public final class ServerMetadata {
  private final String version;
  private final ServerClusterId clusterId;

  @JsonCreator
  public ServerMetadata(
      @JsonProperty("version") final String version,
      @JsonProperty("clusterId") final ServerClusterId clusterId
  ) {
    this.version = Objects.requireNonNull(version, "version");
    this.clusterId = Objects.requireNonNull(clusterId, "clusterId");
  }

  public String getVersion() {
    return version;
  }

  public ServerClusterId getClusterId() {
    return clusterId;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ServerMetadata that = (ServerMetadata) o;
    return Objects.equals(version, that.version)
        && Objects.equals(clusterId, that.clusterId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(version, clusterId);
  }
}
