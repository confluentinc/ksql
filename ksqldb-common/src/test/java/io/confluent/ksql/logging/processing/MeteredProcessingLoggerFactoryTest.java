/*
 * Copyright 2018 Confluent Inc.
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.common.logging.StructuredLogger;
import io.confluent.common.logging.StructuredLoggerFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import io.confluent.ksql.metrics.MetricCollectors;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MeteredProcessingLoggerFactoryTest {
  @Mock
  private StructuredLoggerFactory innerFactory;
  @Mock
  private StructuredLogger innerLogger;
  @Mock
  private ProcessingLogConfig config;
  @Mock
  private BiFunction<ProcessingLogConfig, StructuredLogger, ProcessingLogger> loggerFactory;
  @Mock
  private Function<Metrics, BiFunction<ProcessingLogger, Sensor, ProcessingLogger>> loggerWithMetricsFactory;
  @Mock
  private BiFunction<ProcessingLogger, Sensor, ProcessingLogger> loggerWithMetricsFactoryHelper;
  @Mock
  private ProcessingLogger logger;
  @Mock
  private ProcessingLogger loggerWithMetrics;

  private final Map<String, String> customMetricsTags = Collections.singletonMap("tag1", "value1");
  private MeteredProcessingLoggerFactory factory;
  private MetricCollectors metricCollectors;

  @Before
  public void setup() {
    metricCollectors = new MetricCollectors();
    when(innerFactory.getLogger(anyString())).thenReturn(innerLogger);
    when(loggerFactory.apply(config, innerLogger)).thenReturn(logger);
    when(loggerWithMetricsFactory.apply(any())).thenReturn(loggerWithMetricsFactoryHelper);
    when(loggerWithMetricsFactoryHelper.apply(any(), any())).thenReturn(loggerWithMetrics);
    factory = new MeteredProcessingLoggerFactory(config, innerFactory, metricCollectors.getMetrics(), loggerFactory, loggerWithMetricsFactory, customMetricsTags);
  }

  @Test
  public void shouldCreateLoggerWithoutPassingInTags() {
    // Given:
    final ProcessingLogger testLogger = factory.getLogger("foo.bar");
    final Sensor sensor = metricCollectors.getMetrics().getSensor("foo.bar");
    final Map<String, String> metricsTags = new HashMap<>(customMetricsTags);
    metricsTags.put("logger-id", "foo.bar");

    // When:
    sensor.record();
    sensor.record();

    // Then:
    assertThat(testLogger, is(this.loggerWithMetrics));
    verify(innerFactory).getLogger("foo.bar");
    verify(loggerFactory).apply(config, innerLogger);
    verify(loggerWithMetricsFactory).apply(metricCollectors.getMetrics());
    verify(loggerWithMetricsFactoryHelper).apply(logger, sensor);

    // verify the metric was created correctly
    assertThat(getMetricValue(metricsTags), equalTo(2.0));
  }

  @Test
  public void shouldCreateLoggerWithPassingInAdditionalMetricsTags() {
    // Given:
    final Map<String, String> metricsTags = new HashMap<>(customMetricsTags);
    metricsTags.put("query-id", "some-id");
    final ProcessingLogger testLogger = factory.getLogger("boo.far", metricsTags);

    // When:
    final Sensor sensor = metricCollectors.getMetrics().getSensor("boo.far");
    sensor.record();
    metricsTags.put("logger-id", "boo.far");

    // Then:
    assertThat(testLogger, is(this.loggerWithMetrics));
    verify(innerFactory).getLogger("boo.far");
    verify(loggerFactory).apply(config, innerLogger);
    verify(loggerWithMetricsFactory).apply(metricCollectors.getMetrics());
    verify(loggerWithMetricsFactoryHelper).apply(logger, sensor);

    // verify the metric was created correctly
    assertThat(getMetricValue(metricsTags), equalTo(1.0));
  }

  @Test
  public void shouldReturnExistingLogger() {
    // When:
    factory.getLogger("boo.far", Collections.singletonMap("tag-value", "some-id-1"));
    factory.getLogger("boo.far", Collections.singletonMap("tag-value", "some-id-2"));
    factory.getLogger("boo.far", Collections.singletonMap("tag-value", "some-id-3"));
    final Sensor sensor = metricCollectors.getMetrics().getSensor("boo.far");

    // Then:
    verify(innerFactory, times(1)).getLogger("boo.far");
    verify(loggerFactory, times(1)).apply(config, innerLogger);
    verify(loggerWithMetricsFactory, times(1)).apply(metricCollectors.getMetrics());
    verify(loggerWithMetricsFactoryHelper, times(1)).apply(logger, sensor);
  }

  @Test
  public void shouldGetLoggers() {
    // Given:
    factory.getLogger("foo.bar");
    factory.getLogger("foo.bar", Collections.singletonMap("tag-value", "some-id-2"));
    factory.getLogger("boo.far", Collections.singletonMap("tag-value", "some-id-2"));

    // When:
    final Collection<ProcessingLogger> loggers = factory.getLoggers();

    // Then:
    assertThat(loggers.size(), equalTo(2));
  }

  @Test
  public void shouldHandleNullMetrics() {
    // Given:
    final ProcessingLoggerFactory nullMetricsFactory = new MeteredProcessingLoggerFactory(config, innerFactory, null, loggerFactory, loggerWithMetricsFactory, customMetricsTags);

    // When:
    final ProcessingLogger logger1 = nullMetricsFactory.getLogger("boo.far");
    final ProcessingLogger logger2 = nullMetricsFactory.getLogger("boo.far", Collections.singletonMap("tag1", "some-id-2"));

    // Then:
    assertThat(logger1, is(loggerWithMetrics));
    assertThat(logger2, is(loggerWithMetrics));
    verify(loggerWithMetricsFactory).apply(null);
    // no sensor created because metrics object is null
    verify(loggerWithMetricsFactoryHelper).apply(logger, null);
  }

  @Test
  public void shouldReturnLoggersWithPrefix() {
    // Given:
    factory.getLogger("boo.far.deserializer");
    factory.getLogger("boo.far.serializer", Collections.singletonMap("tag1", "some-id-2"));
    factory.getLogger("far.boo", Collections.singletonMap("tag3", "some-id-2"));

    // Then:
    assertThat(factory.getLoggersWithPrefix("boo.far").size(), is(2));
  }

  private double getMetricValue( final Map<String, String> metricsTags) {
	final Metrics metrics = metricCollectors.getMetrics();
	return Double.parseDouble(
		metrics.metric(
			metrics.metricName(
				MeteredProcessingLoggerFactory.PROCESSING_LOG_ERROR_METRIC_NAME,
                MeteredProcessingLoggerFactory.PROCESSING_LOG_METRICS_GROUP_NAME,
                MeteredProcessingLoggerFactory.PROCESSING_LOG_METRIC_DESCRIPTION,
                metricsTags)
		).metricValue().toString()
	);
  }
}