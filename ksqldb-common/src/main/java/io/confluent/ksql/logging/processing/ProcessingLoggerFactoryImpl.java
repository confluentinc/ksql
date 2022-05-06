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
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;

import io.confluent.ksql.util.MetricsTagsUtil;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeSum;

public class ProcessingLoggerFactoryImpl implements ProcessingLoggerFactory {
  public static final String PROCESSING_LOG_ERROR_METRIC_NAME = "processing-error-total";
  public static final String PROCESSING_LOG_METRICS_GROUP_NAME = "processing-diagnostic-metrics";
  public static final String PROCESSING_LOG_METRIC_DESCRIPTION =
      "The total number of errors emitted by the processing log.";

  private final ProcessingLogConfig config;
  private final StructuredLoggerFactory innerFactory;
  private final Metrics metrics;
  private final Map<String, String> metricsTags;
  private final BiFunction<ProcessingLogConfig, StructuredLogger, ProcessingLogger> loggerFactory;
  private final Function<Metrics, BiFunction<ProcessingLogger, Sensor, ProcessingLogger>>
      loggerWithMetricsFactory;
  private final Map<String, ProcessingLogger> processingLoggers;

  ProcessingLoggerFactoryImpl(
      final ProcessingLogConfig config,
      final StructuredLoggerFactory innerFactory,
      final Metrics metrics,
      final Map<String, String> metricsTags
  ) {
    this(
        config,
        innerFactory,
        metrics,
        ProcessingLoggerImpl::new,
        metricObject -> (processingLogger, sensor) ->
            new MeteredProcessingLogger(processingLogger, metricObject, sensor),
        metricsTags
    );
  }

  ProcessingLoggerFactoryImpl(
      final ProcessingLogConfig config,
      final StructuredLoggerFactory innerFactory,
      final Metrics metrics,
      final BiFunction<ProcessingLogConfig, StructuredLogger, ProcessingLogger> loggerFactory,
      final Function<Metrics, BiFunction<ProcessingLogger, Sensor, ProcessingLogger>>
          loggerWithMetricsFactory,
      final Map<String, String> metricsTags
  ) {
    this.config = config;
    this.innerFactory = innerFactory;
    this.metrics = metrics;
    this.loggerFactory = loggerFactory;
    this.loggerWithMetricsFactory = loggerWithMetricsFactory;
    this.metricsTags = metricsTags;
    this.processingLoggers = new ConcurrentHashMap<>();
  }

  @Override
  public ProcessingLogger getLoggerWithMetrics(
      final String name
  ) {
    return getLoggerWithMetrics(name, "");
  }

  @Override
  public ProcessingLogger getLoggerWithMetrics(
      final String name,
      final String queryId
  ) {
    if (processingLoggers.containsKey(name)) {
      return processingLoggers.get(name);
    }

    // the metrics may be null if this is factory is created from a SandboxedExecutionContext
    Sensor errorSensor = null;
    if (metrics != null) {
      errorSensor = configureProcessingErrorSensor(metrics, metricsTags, name, queryId);
    }
    final ProcessingLogger meteredProcessingLogger = loggerWithMetricsFactory.apply(metrics).apply(
        getLogger(name),
        errorSensor
    );
    processingLoggers.put(name, meteredProcessingLogger);
    return meteredProcessingLogger;
  }

  @Override
  public Collection<String> getLoggers() {
    return innerFactory.getLoggers();
  }

  private static Sensor configureProcessingErrorSensor(
      final Metrics metrics,
      final Map<String, String> metricsTags,
      final String loggerName,
      final String queryId
  ) {
    final Map<String, String> metricsTagsWithLoggerAndQueryId =
        MetricsTagsUtil.getMetricsTagsWithLoggerId(
            loggerName,
            MetricsTagsUtil.getMetricsTagsWithQueryId(queryId, metricsTags));
    final MetricName errorMetric = metrics.metricName(
        PROCESSING_LOG_ERROR_METRIC_NAME,
        PROCESSING_LOG_METRICS_GROUP_NAME,
        PROCESSING_LOG_METRIC_DESCRIPTION,
        metricsTagsWithLoggerAndQueryId
    );
    final Sensor sensor = metrics.sensor(loggerName);
    sensor.add(errorMetric, new CumulativeSum());
    return sensor;
  }

  private ProcessingLogger getLogger(final String name) {
    return loggerFactory.apply(config, innerFactory.getLogger(name));
  }
}
