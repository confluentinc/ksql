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
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Map;
import java.util.Objects;

@Immutable
@JsonIgnoreProperties(ignoreUnknown = true)
public class HostStatusEntity {

  private final boolean hostAlive;
  private final long lastStatusUpdateMs;
  private final ImmutableMap<String, ActiveStandbyEntity> activeStandbyPerQuery;
  private final HostStoreLags hostStoreLags;

  @JsonCreator
  public HostStatusEntity(
      @JsonProperty("hostAlive") final boolean hostAlive,
      @JsonProperty("lastStatusUpdateMs") final long lastStatusUpdateMs,
      @JsonProperty("activeStandbyPerQuery")
      final Map<String, ActiveStandbyEntity> activeStandbyPerQuery,
      @JsonProperty("hostStoreLags") final HostStoreLags hostStoreLags
  ) {
    this.hostAlive = hostAlive;
    this.lastStatusUpdateMs = lastStatusUpdateMs;
    this.activeStandbyPerQuery = ImmutableMap.copyOf(Objects.requireNonNull(
        activeStandbyPerQuery, "activeStandbyPerQuery"));
    this.hostStoreLags = Objects.requireNonNull(hostStoreLags, "hostStoreLags");
  }

  public boolean getHostAlive() {
    return hostAlive;
  }

  public long getLastStatusUpdateMs() {
    return lastStatusUpdateMs;
  }

  @SuppressFBWarnings(
      value = "EI_EXPOSE_REP",
      justification = "activeStandbyPerQuery is ImmutableMap"
  )
  public ImmutableMap<String, ActiveStandbyEntity> getActiveStandbyPerQuery() {
    return activeStandbyPerQuery;
  }

  public HostStoreLags getHostStoreLags() {
    return hostStoreLags;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final HostStatusEntity that = (HostStatusEntity) o;
    return hostAlive == that.hostAlive
        && lastStatusUpdateMs == that.lastStatusUpdateMs
        && Objects.equals(activeStandbyPerQuery, that.activeStandbyPerQuery)
        && Objects.equals(hostStoreLags, that.hostStoreLags);
  }

  @Override
  public int hashCode() {
    return Objects.hash(hostAlive, lastStatusUpdateMs, activeStandbyPerQuery, hostStoreLags);
  }

  @Override
  public String toString() {
    return "HostStatusEntity{"
        + "hostAlive=" + hostAlive
        + ", lastStatusUpdateMs=" + lastStatusUpdateMs
        + ", activeStandbyPerQuery=" + activeStandbyPerQuery
        + ", hostStoreLags=" + hostStoreLags
        + '}';
  }
}
