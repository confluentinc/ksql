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

package io.confluent.ksql.rest.server.computation;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.util.ReservedInternalTopics;
import java.io.Closeable;
import java.util.Collections;
import java.util.Objects;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Metrics;

/**
 * Emits a JMX metric that indicates the health of the CommandRunner thread. 
 */
public class CommandRunnerStatusMetric implements Closeable {

  private static final String DEFAULT_METRIC_GROUP_PREFIX = "ksql-rest-app";
  private static final String METRIC_GROUP_POST_FIX = "-command-runner";
  
  private final Metrics metrics;
  private final MetricName metricName;

  CommandRunnerStatusMetric(
      final String ksqlServiceId,
      final CommandRunner commandRunner,
      final String metricGroupPrefix
  ) {
    this(
        MetricCollectors.getMetrics(),
        commandRunner,
        ksqlServiceId,
        metricGroupPrefix.isEmpty() ? DEFAULT_METRIC_GROUP_PREFIX : metricGroupPrefix
    );
  }

  @VisibleForTesting
  CommandRunnerStatusMetric(
      final Metrics metrics,
      final CommandRunner commandRunner,
      final String ksqlServiceId,
      final String metricsGroupPrefix
  ) {
    this.metrics =  Objects.requireNonNull(metrics, "metrics");
    final String metricGroupName = metricsGroupPrefix + METRIC_GROUP_POST_FIX;
    this.metricName = metrics.metricName(
        "status",
        ReservedInternalTopics.KSQL_INTERNAL_TOPIC_PREFIX + ksqlServiceId + metricGroupName,
       "The status of the commandRunner thread as it processes the command topic.",
        Collections.emptyMap()
    );

    this.metrics.addMetric(metricName, (Gauge<String>)
        (config, now) -> commandRunner.checkCommandRunnerStatus().name());
  }

  /**
   * Close the metric
   */
  @Override
  public void close() {
    metrics.removeMetric(metricName);
  }
}
