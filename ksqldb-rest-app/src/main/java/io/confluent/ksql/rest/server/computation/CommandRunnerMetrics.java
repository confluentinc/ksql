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
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.ReservedInternalTopics;
import java.io.Closeable;
import java.util.Collections;
import java.util.Objects;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Metrics;

/**
 * Emits JMX metrics for the CommandRunner thread. 
 */
public class CommandRunnerMetrics implements Closeable {

  private static final String DEFAULT_METRIC_GROUP_PREFIX = "ksql-rest-app";
  private static final String METRIC_GROUP_POST_FIX = "-command-runner";
  
  private final Metrics metrics;
  private final MetricName commandRunnerStatusMetricNameLegacy;
  private final MetricName commandRunnerDegradedReasonMetricNameLegacy;
  private final MetricName commandRunnerStatusMetricName;
  private final MetricName commandRunnerDegradedReasonMetricName;
  private final MetricName commandRunnerStatusNumMetricName;
  private final MetricName commandRunnerDegradedReasonNumMetricName;

  CommandRunnerMetrics(
      final String ksqlServiceId,
      final CommandRunner commandRunner,
      final String metricGroupPrefix,
      final Metrics metrics
  ) {
    this(
        metrics,
        commandRunner,
        ksqlServiceId,
        metricGroupPrefix.isEmpty() ? DEFAULT_METRIC_GROUP_PREFIX : metricGroupPrefix
    );
  }

  @VisibleForTesting
  CommandRunnerMetrics(
      final Metrics metrics,
      final CommandRunner commandRunner,
      final String ksqlServiceId,
      final String metricsGroupPrefix
  ) {
    this.metrics =  Objects.requireNonNull(metrics, "metrics");
    final String metricGroupName = metricsGroupPrefix + METRIC_GROUP_POST_FIX;
    this.commandRunnerStatusMetricNameLegacy = metrics.metricName(
        "status",
        ReservedInternalTopics.KSQL_INTERNAL_TOPIC_PREFIX + ksqlServiceId + metricGroupName,
       "The status of the commandRunner thread as it processes the command topic.",
        Collections.emptyMap()
    );
    this.commandRunnerDegradedReasonMetricNameLegacy = metrics.metricName(
        "degraded-reason",
        ReservedInternalTopics.KSQL_INTERNAL_TOPIC_PREFIX + ksqlServiceId + metricGroupName,
        "The reason for why the commandRunner thread is in a DEGRADED state.",
        Collections.emptyMap()
    );

    // Metrics with the ksql service id as a tag instead of in the metric name
    this.commandRunnerStatusMetricName = metrics.metricName(
        "status",
        ReservedInternalTopics.CONFLUENT_PREFIX + metricGroupName,
        "The status of the commandRunner thread as it processes the command topic.",
        Collections.singletonMap(KsqlConstants.KSQL_SERVICE_ID_METRICS_TAG, ksqlServiceId)
    );

    this.commandRunnerDegradedReasonMetricName = metrics.metricName(
        "degraded-reason",
        ReservedInternalTopics.CONFLUENT_PREFIX + metricGroupName,
        "The reason for why the commandRunner thread is in a DEGRADED state.",
        Collections.singletonMap(KsqlConstants.KSQL_SERVICE_ID_METRICS_TAG, ksqlServiceId)
    );

    this.commandRunnerStatusNumMetricName = metrics.metricName(
            "status-num",
            ReservedInternalTopics.CONFLUENT_PREFIX + metricGroupName,
            "The status number of the commandRunner thread as it processes the command topic.",
            Collections.singletonMap(KsqlConstants.KSQL_SERVICE_ID_METRICS_TAG, ksqlServiceId)
    );

    this.commandRunnerDegradedReasonNumMetricName = metrics.metricName(
            "degraded-reason-num",
            ReservedInternalTopics.CONFLUENT_PREFIX + metricGroupName,
            "The reason number for why the commandRunner thread is in a DEGRADED state.",
            Collections.singletonMap(KsqlConstants.KSQL_SERVICE_ID_METRICS_TAG, ksqlServiceId)
    );

    this.metrics.addMetric(commandRunnerStatusMetricNameLegacy, (Gauge<String>)
        (config, now) -> commandRunner.checkCommandRunnerStatus().name());
    this.metrics.addMetric(commandRunnerDegradedReasonMetricNameLegacy, (Gauge<String>)
        (config, now) -> commandRunner.getCommandRunnerDegradedReason().name());
    this.metrics.addMetric(commandRunnerStatusMetricName, (Gauge<String>)
        (config, now) -> commandRunner.checkCommandRunnerStatus().name());
    this.metrics.addMetric(commandRunnerDegradedReasonMetricName, (Gauge<String>)
        (config, now) -> commandRunner.getCommandRunnerDegradedReason().name());
    this.metrics.addMetric(commandRunnerStatusNumMetricName, (Gauge<Integer>)
            (config, now) -> commandRunner.checkCommandRunnerStatus().ordinal());
    this.metrics.addMetric(commandRunnerDegradedReasonNumMetricName, (Gauge<Integer>)
            (config, now) -> commandRunner.getCommandRunnerDegradedReason().ordinal());
  }

  /**
   * Close the metric
   */
  @Override
  public void close() {
    metrics.removeMetric(commandRunnerStatusNumMetricName);
    metrics.removeMetric(commandRunnerDegradedReasonNumMetricName);
    metrics.removeMetric(commandRunnerStatusMetricName);
    metrics.removeMetric(commandRunnerDegradedReasonMetricName);
    metrics.removeMetric(commandRunnerStatusMetricNameLegacy);
    metrics.removeMetric(commandRunnerDegradedReasonMetricNameLegacy);
  }
}
