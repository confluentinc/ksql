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
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.kafka.common.metrics.Metrics;

public class ProcessingLoggerFactoryImpl implements ProcessingLoggerFactory {
  private final ProcessingLogConfig config;
  private final StructuredLoggerFactory innerFactory;
  private final BiFunction<ProcessingLogConfig, StructuredLogger, ProcessingLogger> loggerFactory;
  private final Function<ProcessingLoggerWithMetricsInstantiator, ProcessingLogger>
      loggerWithMetricsFactory;

  ProcessingLoggerFactoryImpl(
      final ProcessingLogConfig config,
      final StructuredLoggerFactory innerFactory
  ) {
    this(config, innerFactory, ProcessingLoggerImpl::new, ProcessingLoggerImplWithMetrics::new);
  }

  ProcessingLoggerFactoryImpl(
      final ProcessingLogConfig config,
      final StructuredLoggerFactory innerFactory,
      final BiFunction<ProcessingLogConfig, StructuredLogger, ProcessingLogger> loggerFactory,
      final Function<ProcessingLoggerWithMetricsInstantiator, ProcessingLogger>
          loggerWithMetricsFactory
  ) {
    this.config = config;
    this.innerFactory = innerFactory;
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
      final Metrics metrics,
      final Map<String, String> customMetricsTags
  ) {
    return loggerWithMetricsFactory.apply(
        new ProcessingLoggerWithMetricsInstantiator(
            config,
            innerFactory.getLogger(name),
            customMetricsTags,
            metrics
        )
    );
  }

  @Override
  public Collection<String> getLoggers() {
    return innerFactory.getLoggers();
  }
}
