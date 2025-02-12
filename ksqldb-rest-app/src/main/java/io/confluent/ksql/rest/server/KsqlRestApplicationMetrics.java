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
 *
 */

package io.confluent.ksql.rest.server;

import java.time.Duration;
import java.util.Objects;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KsqlRestApplicationMetrics {
  private static final Logger LOGGER = LoggerFactory.getLogger(KsqlRestApplicationMetrics.class);

  private static final String GROUP = "ksql-rest-application";

  private final Metrics metrics;

  KsqlRestApplicationMetrics(final Metrics metrics) {
    this.metrics = Objects.requireNonNull(metrics, "metrics");
  }

  void recordServerStartLatency(final Duration latency) {
    final MetricName metricName = metrics.metricName(
        "startup-time-ms",
        GROUP
    );
    if (metrics.metric(metricName) != null) {
      LOGGER.error("ksql server startup latency already registered");
      return;
    }
    LOGGER.info("ksql server took {} to become ready", latency.toString());
    metrics.addMetric(metricName, (c, t) -> latency.toMillis());
  }
}
