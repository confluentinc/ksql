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
import java.util.Optional;

@JsonIgnoreProperties(ignoreUnknown = true)
@Immutable
public final class HealthCheckResponse {

  private final boolean isHealthy;
  private final ImmutableMap<String, HealthCheckResponseDetail> details;
  private final Optional<String> serverState;

  @JsonCreator
  public HealthCheckResponse(
      @JsonProperty("isHealthy") final boolean isHealthy,
      @JsonProperty("details") final Map<String, HealthCheckResponseDetail> details,
      @JsonProperty("serverState") final Optional<String> serverState
  ) {
    this.isHealthy = isHealthy;
    this.details = ImmutableMap.copyOf(requireNonNull(details, "details"));
    this.serverState = serverState;
  }

  public boolean getIsHealthy() {
    return isHealthy;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "details is ImmutableMap")
  public Map<String, HealthCheckResponseDetail> getDetails() {
    return details;
  }

  public Optional<String> getServerState() {
    return serverState;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final HealthCheckResponse that = (HealthCheckResponse) o;
    return isHealthy == that.isHealthy
        && Objects.equals(details, that.details)
        && Objects.equals(serverState, that.serverState);
  }

  @Override
  public int hashCode() {
    return Objects.hash(isHealthy, details, serverState);
  }
}
