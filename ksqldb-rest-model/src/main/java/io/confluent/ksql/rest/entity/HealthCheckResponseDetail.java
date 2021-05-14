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

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
@Immutable
public final class HealthCheckResponseDetail {
  private final boolean isHealthy;

  @JsonCreator
  public HealthCheckResponseDetail(
      @JsonProperty("isHealthy") final boolean isHealthy
  ) {
    this.isHealthy = isHealthy;
  }

  public boolean getIsHealthy() {
    return isHealthy;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final HealthCheckResponseDetail that = (HealthCheckResponseDetail) o;
    return isHealthy == that.isHealthy;
  }

  @Override
  public int hashCode() {
    return Objects.hash(isHealthy);
  }
}
