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

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Map;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
@Immutable
public final class ClusterStatusResponse {

  private final ImmutableMap<KsqlHostInfoEntity, HostStatusEntity> clusterStatus;

  @JsonCreator
  public ClusterStatusResponse(
      @JsonProperty("clusterStatus")
      final Map<KsqlHostInfoEntity, HostStatusEntity> clusterStatus) {
    this.clusterStatus = ImmutableMap.copyOf(requireNonNull(clusterStatus, "status"));
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "clusterStatus is ImmutableMap")
  public Map<KsqlHostInfoEntity, HostStatusEntity> getClusterStatus() {
    return clusterStatus;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ClusterStatusResponse that = (ClusterStatusResponse) o;
    return Objects.equals(clusterStatus, that.clusterStatus);
  }

  @Override
  public int hashCode() {
    return Objects.hash(clusterStatus);
  }

  @Override
  public String toString() {
    return "ClusterStatus = " + clusterStatus;
  }
}
