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

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.stats.CumulativeSum;

public class MeteredProcessingLogger implements ProcessingLogger {
  public static final String PROCESSING_LOG_ERROR_METRIC_NAME = "processing-log-errors-sum";
  public static final String PROCESSING_LOG_METRICS_GROUP_NAME = "processing-log-metrics";
  public static final String PROCESSING_LOG_METRIC_DESCRIPTION =
      "The total number of errors emitted by the processing log.";

  private final ProcessingLogger logger;
  private final CumulativeSum sumOfErrors;

  public MeteredProcessingLogger(
      final ProcessingLogger logger,
      final Metrics metrics,
      final Map<String, String> customMetricsTags
  ) {
    if (metrics == null) {
      throw new RuntimeException("Expected metrics to be passed into metered processing logger");
    }
    this.logger = Objects.requireNonNull(logger, "logger");
    this.sumOfErrors = configureTotalProcessingErrors(
        metrics,
        customMetricsTags
    );
  }

  @Override
  public void error(final ErrorMessage msg) {
    final Instant instant = Instant.now();
    sumOfErrors.record(new MetricConfig(), 1.0, instant.getEpochSecond());
    logger.error(msg);
  }

  private static CumulativeSum configureTotalProcessingErrors(
      final Metrics metrics,
      final Map<String, String> metricsTags
  ) {
    CumulativeSum sum = new CumulativeSum();
    final MetricName errorMetric = metrics.metricName(
        PROCESSING_LOG_ERROR_METRIC_NAME,
        PROCESSING_LOG_METRICS_GROUP_NAME,
        PROCESSING_LOG_METRIC_DESCRIPTION,
        metricsTags);

    final KafkaMetric metric = metrics.metric(errorMetric);

    // If metric doesn't exist, add the metric. If it already exists, grab the CumulativeSum
    // measurable so that it can be used in this particular processing logger instance.
    if (metric == null) {
      metrics.addMetric(errorMetric, sum);
    } else {
      sum = (CumulativeSum) metric.measurable();
    }
    return sum;
  }
}
