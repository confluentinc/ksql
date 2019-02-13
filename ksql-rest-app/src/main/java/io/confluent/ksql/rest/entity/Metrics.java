/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
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
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.Collection;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName("metrics")
public class Metrics {

  private final Collection<Metric> metrics;
  private final Collection<Metric> errorMetrics;

  @JsonCreator
  public Metrics(
      @JsonProperty("metrics") final Collection<Metric> metrics,
      @JsonProperty("errorMetrics") final Collection<Metric> errorMetrics
  ) {
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    this.errorMetrics = Objects.requireNonNull(errorMetrics, "errorMetrics");
  }

  public Collection<Metric> getMetrics() {
    return metrics;
  }

  public Collection<Metric> getErrorMetrics() {
    return errorMetrics;
  }

  @Override
  public String toString() {
    return "Metrics{" + "metrics=" + metrics
        + ", errorMetrics=" + errorMetrics
        + '}';
  }
}
