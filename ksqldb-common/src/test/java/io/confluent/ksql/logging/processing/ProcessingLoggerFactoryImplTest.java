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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.common.logging.StructuredLogger;
import io.confluent.common.logging.StructuredLoggerFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.kafka.common.metrics.Metrics;
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
  private ProcessingLoggerFactoryImpl.TriFunction<ProcessingLogger, Metrics, Map<String, String>, ProcessingLogger> loggerWithMetricsFactory;
  @Mock
  private ProcessingLogger logger;
  @Mock
  private ProcessingLogger loggerWithMetrics;
  @Mock
  private Metrics metrics;

  private final Collection<String> loggers = ImmutableList.of("logger1", "logger2");

  private final Map<String, String> customMetricsTags = Collections.emptyMap();
  private ProcessingLoggerFactoryImpl factory;

  @Before
  public void setup() {
    when(innerFactory.getLogger(anyString())).thenReturn(innerLogger);
    when(innerFactory.getLoggers()).thenReturn(loggers);
    when(loggerFactory.apply(config, innerLogger)).thenReturn(logger);
    when(loggerWithMetricsFactory.apply(logger, metrics, customMetricsTags)).thenReturn(loggerWithMetrics);
    factory = new ProcessingLoggerFactoryImpl(config, innerFactory, metrics, loggerFactory, loggerWithMetricsFactory);
  }

  @Test
  public void shouldCreateLogger() {
    // When:
    final ProcessingLogger logger = factory.getLogger("foo.bar");
    final ProcessingLogger loggerWithMetrics = factory.getLoggerWithMetrics("bar.food", customMetricsTags);

    // Then:
    assertThat(logger, is(this.logger));
    verify(innerFactory).getLogger("foo.bar");
    verify(loggerFactory).apply(config, innerLogger);

    assertThat(loggerWithMetrics, is(this.loggerWithMetrics));
    verify(innerFactory).getLogger("bar.food");
    verify(loggerWithMetricsFactory).apply(logger, metrics, customMetricsTags);
  }

  @Test
  public void shouldGetLoggers() {
    // When:
    final Collection<String> loggers = factory.getLoggers();

    // Then:
    assertThat(loggers, equalTo(this.loggers));
  }
}