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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.query.QueryId;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class QueryMetricsUtilTest {
  private final QueryId queryId = new QueryId("queryid");
  private MetricCollectors metricCollectors;

  @Before
  public void setup() {
	metricCollectors = new MetricCollectors();
  }

  @Test
  public void shouldBuildCorrectSensor() {
	// Given:
	final Map<String, String> metricsTags = Collections.singletonMap("tag1", "value1");
	final Sensor sensor = QueryMetricsUtil.getProcessingLogErrorMetricSensor(queryId.toString(), metricCollectors.getMetrics(), metricsTags);

	// When:
	sensor.record();

	// Then:
	final Map<String, String> sensorMetricsTags = new HashMap<>(metricsTags);
	sensorMetricsTags.put("query-id", queryId.toString());
	assertThat(getMetricValue(sensorMetricsTags), equalTo("queryid.biz.baz"));
  }

  private double getMetricValue(final Map<String, String> metricsTags) {
	final Metrics metrics = metricCollectors.getMetrics();
	return Double.parseDouble(
		metrics.metric(
			metrics.metricName(
				QueryMetricsUtil.PROCESSING_LOG_ERROR_METRIC_NAME, QueryMetricsUtil.PROCESSING_LOG_METRICS_GROUP_NAME, QueryMetricsUtil.PROCESSING_LOG_METRIC_DESCRIPTION, metricsTags)
		).metricValue().toString()
	);
  }
}
