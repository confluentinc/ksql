/*
 * Copyright 2022 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.metrics;

import com.google.common.collect.ImmutableList;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.Time;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;

/**
 * {@code MetricAppender} publishes JMX metrics around the number of messages that
 * are sent to any logger configured to use this appender.
 */
public class MetricAppender extends AppenderSkeleton {

  private static final String KSQL_LOGGING_JMX_PREFIX = "io.confluent.ksql.metrics.logging";
  private static final String KSQL_LOGGING_METRIC_GROUP = "ksql-logging";

  private final Metrics metrics;
  private final Sensor errors;
  private final Sensor warns;
  private final Sensor infos;

  public MetricAppender() {
    metrics = new Metrics(
        new MetricConfig().samples(100).timeWindow(1, TimeUnit.SECONDS),
        ImmutableList.of(new JmxReporter()),
        Time.SYSTEM,
        new KafkaMetricsContext(KSQL_LOGGING_JMX_PREFIX)
    );

    errors = metrics.sensor(KSQL_LOGGING_METRIC_GROUP + "-error-rate");
    errors.add(
        metrics.metricName("errors", KSQL_LOGGING_METRIC_GROUP, "number of error logs per second"),
        new Rate()
    );

    warns = metrics.sensor(KSQL_LOGGING_METRIC_GROUP + "-warn-rate");
    warns.add(
        metrics.metricName("warns", KSQL_LOGGING_METRIC_GROUP, "number of warn logs per second"),
        new Rate()
    );

    infos = metrics.sensor(KSQL_LOGGING_METRIC_GROUP + "-info-rate");
    infos.add(
        metrics.metricName("infos", KSQL_LOGGING_METRIC_GROUP, "number of info logs per second"),
        new Rate()
    );
  }

  @Override
  protected void append(final LoggingEvent event) {
    if (event.getLevel() == Level.INFO) {
      infos.record();
    }  else if (event.getLevel() == Level.WARN) {
      warns.record();
    } else if (event.getLevel() == Level.ERROR) {
      errors.record();
    }
  }

  @Override
  public void close() {
    metrics.close();
  }

  @Override
  public boolean requiresLayout() {
    return false;
  }

}
