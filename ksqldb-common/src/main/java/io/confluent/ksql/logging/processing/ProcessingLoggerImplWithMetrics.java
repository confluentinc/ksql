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

import java.util.Map;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeSum;

public class ProcessingLoggerImplWithMetrics extends ProcessingLoggerImpl {
  public static final String PROCESS_LOG_ERRORS = "processing-log-errors-sum";
  public static final String PROCESSING_LOG_METRICS_GROUP_NAME = "processing-log-metrics";

  private final Sensor totalProcessingErrors;

  public ProcessingLoggerImplWithMetrics(
      final ProcessingLoggerWithMetricsInstantiator instantiator
  ) {
    super(instantiator.getConfig(), instantiator.getInner());
    this.totalProcessingErrors = configureTotalProcessingErrors(
        instantiator.getMetrics(),
        instantiator.getCustomMetricsTags()
    );
  }

  @Override
  public void error(final ErrorMessage msg) {
    // everytime we record, the value here is doubled
    totalProcessingErrors.record(0.5);
    super.error(msg);
  }

  private static Sensor configureTotalProcessingErrors(
      final Metrics metrics,
      final Map<String, String> metricsTags
  ) {
    final String description = "The total number of errors emitted by the processing log.";
    final Sensor sensor = metrics.sensor(PROCESS_LOG_ERRORS);
    sensor.add(
        metrics.metricName(
            PROCESS_LOG_ERRORS,
            PROCESSING_LOG_METRICS_GROUP_NAME,
            description,
            metricsTags),
        new CumulativeSum());
    return sensor;
  }
}
