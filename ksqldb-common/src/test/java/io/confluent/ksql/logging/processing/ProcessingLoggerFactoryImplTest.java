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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
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
public class ProcessingLoggerFactoryImplTest {
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

  private final Collection<String> loggers = ImmutableList.of("logger1", "logger2");

  private final Map<String, String> customMetricsTags = Collections.singletonMap("tag1", "value1");
  private ProcessingLoggerFactoryImpl factory;
  private MetricCollectors metricCollectors;

  @Before
  public void setup() {
    metricCollectors = new MetricCollectors();
    when(innerFactory.getLogger(anyString())).thenReturn(innerLogger);
    when(innerFactory.getLoggers()).thenReturn(loggers);
    when(loggerFactory.apply(config, innerLogger)).thenReturn(logger);
    when(loggerWithMetricsFactory.apply(any())).thenReturn(loggerWithMetricsFactoryHelper);
    when(loggerWithMetricsFactoryHelper.apply(any(), any())).thenReturn(loggerWithMetrics);
    factory = new ProcessingLoggerFactoryImpl(config, innerFactory, metricCollectors.getMetrics(), loggerFactory, loggerWithMetricsFactory, customMetricsTags);
  }

  @Test
  public void shouldCreateLoggerWithoutPassingInQueryId() {
    // Given:
    final ProcessingLogger testLogger = factory.getLoggerWithMetrics("foo.bar");
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
  public void shouldCreateLoggerWithPassingInQueryId() {
    // Given:
    final ProcessingLogger testLogger = factory.getLoggerWithMetrics("boo.far", "some-id");
    final Sensor sensor = metricCollectors.getMetrics().getSensor("boo.far");
    final Map<String, String> metricsTags = new HashMap<>(customMetricsTags);
    metricsTags.put("logger-id", "boo.far");
    metricsTags.put("query-id", "some-id");

    // When:
    sensor.record();

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
    factory.getLoggerWithMetrics("boo.far", "some-id");
    factory.getLoggerWithMetrics("boo.far", "some-id-2");
    factory.getLoggerWithMetrics("boo.far", "some-id-3");
    final Sensor sensor = metricCollectors.getMetrics().getSensor("boo.far");

    // Then:
    verify(innerFactory, times(1)).getLogger("boo.far");
    verify(loggerFactory, times(1)).apply(config, innerLogger);
    verify(loggerWithMetricsFactory, times(1)).apply(metricCollectors.getMetrics());
    verify(loggerWithMetricsFactoryHelper, times(1)).apply(logger, sensor);
  }

  @Test
  public void shouldGetLoggers() {
    // When:
    final Collection<String> loggers = factory.getLoggers();

    // Then:
    assertThat(loggers, equalTo(this.loggers));
  }

  @Test
  public void shouldHandleNullMetrics() {
    // Given:
    final ProcessingLoggerFactory nullMetricsFactory = new ProcessingLoggerFactoryImpl(config, innerFactory, null, loggerFactory, loggerWithMetricsFactory, customMetricsTags);

    // When:
    final ProcessingLogger logger1 = nullMetricsFactory.getLoggerWithMetrics("boo.far");
    final ProcessingLogger logger2 = nullMetricsFactory.getLoggerWithMetrics("boo.far", "some-id-2");

    // Then:
    assertThat(logger1, is(loggerWithMetrics));
    assertThat(logger2, is(loggerWithMetrics));
    verify(loggerWithMetricsFactory).apply(null);
    verify(loggerWithMetricsFactoryHelper).apply(logger, null);
  }

  private double getMetricValue( final Map<String, String> metricsTags) {
	final Metrics metrics = metricCollectors.getMetrics();
	return Double.parseDouble(
		metrics.metric(
			metrics.metricName(
				ProcessingLoggerFactoryImpl.PROCESSING_LOG_ERROR_METRIC_NAME,
                ProcessingLoggerFactoryImpl.PROCESSING_LOG_METRICS_GROUP_NAME,
                ProcessingLoggerFactoryImpl.PROCESSING_LOG_METRIC_DESCRIPTION,
                metricsTags)
		).metricValue().toString()
	);
  }
}