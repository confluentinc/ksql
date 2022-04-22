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

package io.confluent.ksql.util;

import java.util.Map;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeSum;

public final class QueryMetricsUtil {
  public static final String PROCESSING_LOG_ERROR_METRIC_NAME = "processing-error-total";
  public static final String PROCESSING_LOG_METRICS_GROUP_NAME = "processing-diagnostic-metrics";
  public static final String PROCESSING_LOG_METRIC_DESCRIPTION =
      "The total number of errors emitted by the processing log.";

  private QueryMetricsUtil() {}

  public static Sensor getProcessingLogErrorMetricSensor(
      final String queryId,
      final Metrics metrics,
      final Map<String, String> metricsTags
  ) {
    final Map<String, String> customMetricsTags =
        MetricsTagsUtil.getCustomMetricsTagsForQuery(queryId, metricsTags);
    final MetricName errorMetric = metrics.metricName(
        PROCESSING_LOG_ERROR_METRIC_NAME,
        PROCESSING_LOG_METRICS_GROUP_NAME,
        PROCESSING_LOG_METRIC_DESCRIPTION,
        customMetricsTags);

    final Sensor sensor = metrics.sensor(queryId);
    sensor.add(errorMetric, new CumulativeSum());
    System.out.println("steven here");
    System.out.println(customMetricsTags);
    return sensor;
  }
}
