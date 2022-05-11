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

  private QueryMetricsUtil() {}
  // returns a metrics sensor that tracks the number of times a query was restarted when hitting
  // an uncaught exception

  public static Sensor createQueryRestartMetricSensor(
      final String queryId,
      final Map<String, String> metricsTags,
      final Metrics metrics
  ) {
    final Map<String, String> customMetricsTagsForQuery =
        MetricsTagsUtil.getMetricsTagsWithQueryId(queryId, metricsTags);
    final MetricName restartMetricName = metrics.metricName(
        QueryMetadataImpl.QUERY_RESTART_METRIC_NAME,
        QueryMetadataImpl.QUERY_RESTART_METRIC_GROUP_NAME,
        QueryMetadataImpl.QUERY_RESTART_METRIC_DESCRIPTION,
        customMetricsTagsForQuery
    );
    final Sensor sensor = metrics.sensor(
        QueryMetadataImpl.QUERY_RESTART_METRIC_GROUP_NAME + "-" + queryId);
    sensor.add(restartMetricName, new CumulativeSum());
    return sensor;
  }
}
