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

import org.apache.kafka.common.metrics.*;
import org.apache.kafka.common.metrics.stats.CumulativeSum;

public class ProcessingLoggerImplWithMetrics extends ProcessingLoggerImpl {
  public static final String PROCESS_LOG_ERRORS = "processing-log-errors-sum";
  public static final String PROCESSING_LOG_METRICS_GROUP_NAME = "processing-log-metrics";

  private final CumulativeSum sumOfErrors;

  public ProcessingLoggerImplWithMetrics(
      final ProcessingLoggerWithMetricsInstantiator instantiator
  ) {
    super(instantiator.getConfig(), instantiator.getInner());
    this.sumOfErrors = configureTotalProcessingErrors(
        instantiator.getMetrics(),
        instantiator.getCustomMetricsTags()
    );
  }

  @Override
  public void error(final ErrorMessage msg) {
    final Instant instant = Instant.now();
    sumOfErrors.record(new MetricConfig(), 1.0, instant.getEpochSecond());
    super.error(msg);
  }

  private static CumulativeSum configureTotalProcessingErrors(
      final Metrics metrics,
      final Map<String, String> metricsTags
  ) {
    final String description = "The total number of errors emitted by the processing log.";
    CumulativeSum sum = new CumulativeSum();
    final KafkaMetric metric = metrics.metric(metrics.metricName(
          PROCESS_LOG_ERRORS,
          PROCESSING_LOG_METRICS_GROUP_NAME,
          description,
          metricsTags));

    // If metric doesn't exist, add the metric. If it already exists, grab the CumulativeSum measurable
    // so that it can be used in this particular processing logger instance.
    if (metric == null) {
      metrics.addMetric(metrics.metricName(
          PROCESS_LOG_ERRORS,
          PROCESSING_LOG_METRICS_GROUP_NAME,
          description,
          metricsTags), sum);
    } else {
      sum = (CumulativeSum) metric.measurable();
    }
    return sum;
  }
}
