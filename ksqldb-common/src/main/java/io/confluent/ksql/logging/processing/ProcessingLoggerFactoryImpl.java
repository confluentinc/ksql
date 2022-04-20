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

import io.confluent.common.logging.StructuredLogger;
import io.confluent.common.logging.StructuredLoggerFactory;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.kafka.common.metrics.Metrics;

public class ProcessingLoggerFactoryImpl implements ProcessingLoggerFactory {
  private final ProcessingLogConfig config;
  private final StructuredLoggerFactory innerFactory;
  private final Metrics metrics;
  private final BiFunction<ProcessingLogConfig, StructuredLogger, ProcessingLogger> loggerFactory;
  private final TriFunction<ProcessingLogger, Metrics, Map<String, String>, ProcessingLogger>
      loggerWithMetricsFactory;

  ProcessingLoggerFactoryImpl(
      final ProcessingLogConfig config,
      final StructuredLoggerFactory innerFactory,
      final Metrics metrics
  ) {
    this(config, innerFactory, metrics, ProcessingLoggerImpl::new, MeteredProcessingLogger::new);
  }

  ProcessingLoggerFactoryImpl(
      final ProcessingLogConfig config,
      final StructuredLoggerFactory innerFactory,
      final Metrics metrics,
      final BiFunction<ProcessingLogConfig, StructuredLogger, ProcessingLogger> loggerFactory,
      final TriFunction<ProcessingLogger, Metrics, Map<String, String>, ProcessingLogger>
          loggerWithMetricsFactory
  ) {
    this.config = config;
    this.innerFactory = innerFactory;
    this.metrics = metrics;
    this.loggerFactory = loggerFactory;
    this.loggerWithMetricsFactory = loggerWithMetricsFactory;
  }

  @Override
  public ProcessingLogger getLogger(final String name) {
    return loggerFactory.apply(config, innerFactory.getLogger(name));
  }

  @Override
  public ProcessingLogger getLoggerWithMetrics(
      final String name,
      final Map<String, String> customMetricsTags
  ) {
    return loggerWithMetricsFactory.apply(
        loggerFactory.apply(config, innerFactory.getLogger(name)),
        metrics,
        customMetricsTags
    );
  }

  @Override
  public Collection<String> getLoggers() {
    return innerFactory.getLoggers();
  }

  interface TriFunction<A,B,C,R> {

    R apply(A a, B b, C c);

    default <V> TriFunction<A, B, C, V> andThen(
        Function<? super R, ? extends V> after) {
      Objects.requireNonNull(after);
      return (A a, B b, C c) -> after.apply(apply(a, b, c));
    }
  }
}
