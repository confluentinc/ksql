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
import java.util.Map;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
@Immutable
public final class HealthCheckResponse {
  private final boolean isHealthy;
  private final Map<String, HealthCheckResponseDetail> details;

  @JsonCreator
  public HealthCheckResponse(
      @JsonProperty("isHealthy") final boolean isHealthy,
      @JsonProperty("details") final Map<String, HealthCheckResponseDetail> details
  ) {
    this.isHealthy = isHealthy;
    this.details = Objects.requireNonNull(details, "details");
  }

  public boolean getIsHealthy() {
    return isHealthy;
  }

  public Map<String, HealthCheckResponseDetail> getDetails() {
    return details;
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
        && Objects.equals(details, that.details);
  }

  @Override
  public int hashCode() {
    return Objects.hash(isHealthy, details);
  }
}
