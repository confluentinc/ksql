/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.metrics;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.confluent.common.utils.Time;
import io.confluent.ksql.metrics.TopicSensors.SensorMetric;
import io.confluent.ksql.metrics.TopicSensors.Stat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MeasurableStat;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.Total;

public final class StreamsErrorCollector implements MetricCollector {
  public static final String CONSUMER_FAILED_MESSAGES = "consumer-failed-messages";
  public static final String CONSUMER_FAILED_MESSAGES_PER_SEC
      = "consumer-failed-messages-per-sec";

  private static final Map<String, StreamsErrorCollector> COLLECTORS = Maps.newConcurrentMap();

  private final Map<String, TopicSensors<Object>> topicSensors = Maps.newConcurrentMap();
  private String id;
  private final Metrics metrics;
  private final Time time;

  private StreamsErrorCollector() {
    this.metrics = MetricCollectors.getMetrics();
    this.time = MetricCollectors.getTime();
  }

  public void setId(final String id) {
    this.id = id;
  }

  private void collect(final String topic) {
    topicSensors
        .computeIfAbsent(topic, this::buildSensors)
        .increment(null, true);
  }

  private TopicSensors buildSensors(final String topic) {
    final List<TopicSensors.SensorMetric<Object>> sensors = new ArrayList<>();
    synchronized (this.metrics) {
      sensors.add(
          buildSensor(topic, CONSUMER_FAILED_MESSAGES, new Total(), o -> 1.0));
      sensors.add(
          buildSensor(topic, CONSUMER_FAILED_MESSAGES_PER_SEC, new Rate(), o -> 1.0));
    };
    return new TopicSensors<>(topic, sensors);
  }

  private SensorMetric<Object> buildSensor(
      final String key,
      final String metricNameString,
      final MeasurableStat stat,
      final Function<Object, Double> recordValue
  ) {
    final String name = "sec-" + key + "-" + metricNameString + "-" + id;

    final MetricName metricName = new MetricName(
        metricNameString,
        "consumer-metrics",
        "consumer-" + name,
        ImmutableMap.of("key", key, "id", id)
    );
    final Sensor sensor = metrics.sensor(name);
    sensor.add(metricName, stat);

    final KafkaMetric metric = metrics.metrics().get(metricName);

    return new TopicSensors.SensorMetric<Object>(sensor, metric, time, true) {
      void record(final Object o) {
        sensor.record(recordValue.apply(o));
        super.record(o);
      }
    };
  }

  private void cleanup() {
    MetricCollectors.remove(id);
    topicSensors.values().forEach(v -> v.close(metrics));
  }

  @Override
  public double errorRate() {
    final List<TopicSensors.Stat> allStats = new ArrayList<>();
    topicSensors.values().forEach(record -> allStats.addAll(record.errorRateStats()));
    return allStats
        .stream()
        .mapToDouble(TopicSensors.Stat::getValue)
        .sum();
  }

  @Override
  public double aggregateStat(final String name, final boolean isError) {
    return MetricUtils.aggregateStat(name, isError, topicSensors.values());
  }

  @Override
  public Collection<Stat> stats(final String topic, final boolean isError) {
    return MetricUtils.stats(topic, isError, topicSensors.values());
  }

  public static void recordError(final String applicationId, final String topic) {
    final StreamsErrorCollector errorCollector = COLLECTORS.computeIfAbsent(
        applicationId,
        aid -> {
          final StreamsErrorCollector ec = new StreamsErrorCollector();
          final String collectorId = MetricCollectors.addCollector(aid, ec);
          ec.setId(collectorId);
          return ec;
        }
    );
    errorCollector.collect(topic);
  }

  public static void notifyApplicationClose(final String applicationId) {
    final StreamsErrorCollector errorCollector = COLLECTORS.remove(applicationId);
    if (errorCollector != null) {
      errorCollector.cleanup();
    }
  }
}
