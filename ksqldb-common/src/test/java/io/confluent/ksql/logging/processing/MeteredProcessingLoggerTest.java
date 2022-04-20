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

import static io.confluent.ksql.logging.processing.MeteredProcessingLogger.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.*;

import io.confluent.ksql.logging.processing.ProcessingLogger.ErrorMessage;

import java.util.Collections;
import java.util.Map;

import io.confluent.ksql.metrics.MetricCollectors;
import org.apache.kafka.common.metrics.Metrics;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MeteredProcessingLoggerTest {

  @Mock
  private ProcessingLogger processingLogger;
  @Mock
  private ErrorMessage errorMsg;

  private MeteredProcessingLogger meteredProcessingLogger;
  private MetricCollectors metricCollectors;
  private final Map<String, String> originalMetricsTags = Collections.singletonMap("tag1", "value1");

  @Before
  public void setup() {
	metricCollectors = new MetricCollectors();
	meteredProcessingLogger = new MeteredProcessingLogger(processingLogger, metricCollectors.getMetrics(), originalMetricsTags);
  }

  @Test
  public void shouldIncrementTheSameMetric() {
	// When:

	// this should not throw an exception because only one logger should register the metric
	final MeteredProcessingLogger logger2 = new MeteredProcessingLogger(processingLogger, metricCollectors.getMetrics(), originalMetricsTags);
	logger2.error(errorMsg);

	// Then:
	assertThat(getMetricValue(originalMetricsTags), equalTo(1.0));
	meteredProcessingLogger.error(errorMsg);
	assertThat(getMetricValue(originalMetricsTags), equalTo(2.0));
	logger2.error(errorMsg);
	assertThat(getMetricValue(originalMetricsTags), equalTo(3.0));
	meteredProcessingLogger.error(errorMsg);
	assertThat(getMetricValue(originalMetricsTags), equalTo(4.0));
  }

  @Test
  public void shouldIncrementSeparateMetrics() {
	// When:
	final Map<String, String> otherMetricsTags = Collections.singletonMap("another-tag", "another-value");
	final MeteredProcessingLogger differentLogger = new MeteredProcessingLogger(processingLogger, metricCollectors.getMetrics(), otherMetricsTags);
	differentLogger.error(errorMsg);

	// Then:
	assertThat(getMetricValue(otherMetricsTags), equalTo(1.0));
	meteredProcessingLogger.error(errorMsg);
	assertThat(getMetricValue(originalMetricsTags), equalTo(1.0));
	differentLogger.error(errorMsg);

	assertThat(getMetricValue(otherMetricsTags), equalTo(2.0));
	meteredProcessingLogger.error(errorMsg);
	assertThat(getMetricValue(originalMetricsTags), equalTo(2.0));
  }

  @Test
  public void shouldRecordMetric() {
	// When:
	meteredProcessingLogger.error(errorMsg);

	// Then:
	verify(processingLogger).error(errorMsg);
	assertThat(getMetricValue(originalMetricsTags), equalTo(1.0));
  }

  private double getMetricValue(final Map<String, String> metricsTags) {
	final Metrics metrics = metricCollectors.getMetrics();
	return Double.parseDouble(
		metrics.metric(
			metrics.metricName(
				PROCESSING_LOG_ERROR_METRIC_NAME, PROCESSING_LOG_METRICS_GROUP_NAME, metricsTags)
		).metricValue().toString()
	);
  }
}