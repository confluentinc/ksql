/**
 * Copyright 2017 Confluent Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.metrics;

import com.google.common.collect.ImmutableMap;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MeasurableStat;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.Total;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.common.utils.Time;

public class ProducerCollector implements MetricCollector {

  private final Map<String, TopicSensors> topicSensors = new HashMap<>();
  private Metrics metrics;
  private String id;
  private Time time;

  public void configure(Map<String, ?> map) {
    String id = (String) map.get(ProducerConfig.CLIENT_ID_CONFIG);
    configure(
        MetricCollectors.getMetrics(),
        MetricCollectors.addCollector(id, this),
        MetricCollectors.getTime()
    );
  }

  ProducerCollector configure(final Metrics metrics, final String id, Time time) {
    this.id = id;
    this.metrics = metrics;
    this.time = time;
    return this;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public ProducerRecord onSend(ProducerRecord record) {
    collect(record, false);
    return record;
  }

  public void recordError(String topic) {
    collect(true, topic.toLowerCase());
  }

  private void collect(ProducerRecord record, boolean isError) {
    collect(isError, record.topic().toLowerCase());
  }

  private void collect(boolean isError, String topic) {
    topicSensors.computeIfAbsent(getKey(topic), k ->
        new TopicSensors<>(topic, buildSensors(k))
    ).increment(null, isError);
  }


  private List<TopicSensors.SensorMetric<ProducerRecord>> buildSensors(String key) {
    List<TopicSensors.SensorMetric<ProducerRecord>> sensors = new ArrayList<>();

    // Note: synchronized due to metrics registry not handling concurrent add/check-exists
    // activity in a reliable way
    synchronized (metrics) {
      addSensor(key, "messages-per-sec", new Rate(), sensors, false);
      addSensor(key, "total-messages", new Total(), sensors, false);
      addSensor(key, "failed-messages", new Total(), sensors, true);
      addSensor(key, "failed-messages-per-sec", new Rate(), sensors, true);
    }
    return sensors;
  }

  private void addSensor(
      String key,
      String metricNameString,
      MeasurableStat stat,
      List<TopicSensors.SensorMetric<ProducerRecord>> results,
      boolean isError
  ) {
    String name = "prod-" + key + "-" + metricNameString + "-" + id;

    MetricName metricName = new MetricName(
        metricNameString,
        "producer-metrics",
        "producer-" + name,
        ImmutableMap.of("key", key, "id", id)
    );
    Sensor existingSensor = metrics.getSensor(name);
    Sensor sensor = metrics.sensor(name);

    // either a new sensor or a new metric with different id
    if (existingSensor == null || metrics.metrics().get(metricName) == null) {
      sensor.add(metricName, stat);
    }
    KafkaMetric metric = metrics.metrics().get(metricName);

    results.add(new TopicSensors.SensorMetric<ProducerRecord>(sensor, metric, time, isError) {
      void record(ProducerRecord record) {
        sensor.record(1);
        super.record(record);
      }
    });
  }


  private String getKey(String topic) {
    return topic;
  }

  public void close() {
    MetricCollectors.remove(this.id);
    topicSensors.values().forEach(v -> v.close(metrics));
  }

  @Override
  public Collection<TopicSensors.Stat> stats(String topic, boolean isError) {
    final List<TopicSensors.Stat> list = new ArrayList<>();
    topicSensors
        .values()
        .stream()
        .filter(counter -> counter.isTopic(topic))
        .forEach(record -> list.addAll(record.stats(isError)));
    return list;
  }

  @Override
  public double currentMessageProductionRate() {
    final List<TopicSensors.Stat> allStats = new ArrayList<>();
    topicSensors.values().forEach(record -> allStats.addAll(record.stats(false)));

    return allStats
        .stream()
        .filter(stat -> stat.name().contains("messages-per-sec"))
        .mapToDouble(TopicSensors.Stat::getValue)
        .sum();
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
  public String toString() {
    return getClass().getSimpleName() + " " + this.id + " " + this.topicSensors.toString();
  }
}
