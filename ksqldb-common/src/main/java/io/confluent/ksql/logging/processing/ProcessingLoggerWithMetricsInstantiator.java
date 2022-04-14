/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.logging.processing;

import static java.util.Objects.requireNonNull;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.common.logging.StructuredLogger;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.metrics.Metrics;

public class ProcessingLoggerWithMetricsInstantiator {
  private final StructuredLogger inner;
  private final ProcessingLogConfig config;
  private final Map<String, String> customMetricsTags;
  private final Metrics metrics;

  public ProcessingLoggerWithMetricsInstantiator(
      final ProcessingLogConfig config,
      final StructuredLogger inner,
      final Map<String, String> customMetricsTags,
      final Metrics metrics
  ) {
    this.inner = requireNonNull(inner, "inner");
    this.config = requireNonNull(config, "config");
    this.customMetricsTags = requireNonNull(customMetricsTags, "customMetricsTags");
    this.metrics = requireNonNull(metrics, "metrics");
  }

  public StructuredLogger getInner() {
    return inner;
  }

  public ProcessingLogConfig getConfig() {
    return config;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP")
  public Map<String, String> getCustomMetricsTags() {
    return customMetricsTags;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP")
  public Metrics getMetrics() {
    return metrics;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ProcessingLoggerWithMetricsInstantiator instantiator =
        (ProcessingLoggerWithMetricsInstantiator) o;
    return inner.equals(instantiator.inner)
        && config.equals(instantiator.config)
        && customMetricsTags.equals(instantiator.customMetricsTags)
        && metrics.equals(instantiator.metrics);
  }

  @Override
  public int hashCode() {
    return Objects.hash(inner, config, customMetricsTags, metrics);
  }
}
