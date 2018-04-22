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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
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
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import io.confluent.common.utils.Time;

public class ConsumerCollector implements MetricCollector {

  private final Map<String, TopicSensors> topicSensors = new HashMap<>();
  private Metrics metrics;
  private String id;
  private String groupId;
  private Time time;

  public void configure(Map<String, ?> map) {
    String id = (String) map.get(ConsumerConfig.GROUP_ID_CONFIG);
    if (id != null) {
      this.groupId = id;
    }
    if (id == null) {
      id = (String) map.get(ConsumerConfig.CLIENT_ID_CONFIG);
    }
    if (id.contains("")) {
      configure(
          MetricCollectors.getMetrics(),
          MetricCollectors.addCollector(id, this),
          MetricCollectors.getTime()
      );
    }
  }

  ConsumerCollector configure(final Metrics metrics, final String id, final Time time) {
    this.id = id;
    this.metrics = metrics;
    this.time = time;
    return this;
  }

  @Override
  public String getGroupId() {
    return this.groupId;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public ConsumerRecords onConsume(ConsumerRecords records) {
    collect(records);
    return records;
  }

  @SuppressWarnings("unchecked")
  private void collect(ConsumerRecords consumerRecords) {
    Stream<ConsumerRecord> stream = StreamSupport.stream(consumerRecords.spliterator(), false);
    stream.forEach(record -> record(record.topic().toLowerCase(), false, record));
  }

  public void recordError(String topic) {
    record(topic, true, null);
  }

  private void record(String topic, boolean isError, ConsumerRecord record) {
    topicSensors.computeIfAbsent(getCounterKey(topic), k ->
        new TopicSensors<>(topic, buildSensors(k))
    ).increment(record, isError);
  }

  private String getCounterKey(String topic) {
    return topic;
  }

  private List<TopicSensors.SensorMetric<ConsumerRecord>> buildSensors(String key) {

    List<TopicSensors.SensorMetric<ConsumerRecord>> sensors = new ArrayList<>();

    // Note: synchronized due to metrics registry not handling concurrent add/check-exists
    // activity in a reliable way
    synchronized (this.metrics) {
      addSensor(key, "consumer-messages-per-sec", new Rate(), sensors, false);
      addSensor(key, "consumer-total-messages", new Total(), sensors, false);
      addSensor(key, "consumer-failed-messages", new Total(), sensors, true);
      addSensor(key, "consumer-total-message-bytes", new Total(), sensors, false,
          (r) -> {
            if (r == null) {
              return 0.0;
            } else {
              return ((double) r.serializedValueSize() + r.serializedKeySize());
            }
          });
      addSensor(key, "failed-messages-per-sec", new Rate(), sensors, true);
    }
    return sensors;
  }

  private void addSensor(
      String key,
      String metricNameString,
      MeasurableStat stat,
      List<TopicSensors.SensorMetric<ConsumerRecord>> sensors,
      boolean isError
  ) {
    addSensor(key, metricNameString, stat, sensors, isError, (r) -> (double)1);
  }

  private void addSensor(
      String key,
      String metricNameString,
      MeasurableStat stat,
      List<TopicSensors.SensorMetric<ConsumerRecord>> sensors,
      boolean isError,
      Function<ConsumerRecord, Double> recordValue
  ) {
    String name = "cons-" + key + "-" + metricNameString + "-" + id;

    MetricName metricName = new MetricName(
        metricNameString,
        "consumer-metrics",
        "consumer-" + name,
        ImmutableMap.of("key", key, "id", id)
    );
    Sensor existingSensor = metrics.getSensor(name);
    Sensor sensor = metrics.sensor(name);

    // re-use the existing measurable stats to share between consumers
    if (existingSensor == null || metrics.metrics().get(metricName) == null) {
      sensor.add(metricName, stat);
    }

    KafkaMetric metric = metrics.metrics().get(metricName);

    sensors.add(new TopicSensors.SensorMetric<ConsumerRecord>(sensor, metric, time, isError) {
      void record(ConsumerRecord record) {
        sensor.record(recordValue.apply(record));
        super.record(record);
      }
    });
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
  public double currentMessageConsumptionRate() {
    final List<TopicSensors.Stat> allStats = new ArrayList<>();
    topicSensors.values().forEach(record -> allStats.addAll(record.stats(false)));

    return allStats
        .stream()
        .filter(stat -> stat.name().contains("consumer-messages-per-sec"))
        .mapToDouble(TopicSensors.Stat::getValue)
        .sum();
  }

  @Override
  public double totalMessageConsumption() {
    final List<TopicSensors.Stat> allStats = new ArrayList<>();
    topicSensors.values().forEach(record -> allStats.addAll(record.stats(false)));

    return allStats
        .stream()
        .filter(stat -> stat.name().contains("consumer-total-messages"))
        .mapToDouble(TopicSensors.Stat::getValue)
        .sum();
  }

  @Override
  public double totalBytesConsumption() {
    final List<TopicSensors.Stat> allStats = new ArrayList<>();
    topicSensors.values().forEach(record -> allStats.addAll(record.stats(false)));

    return allStats
        .stream()
        .filter(stat -> stat.name().contains("consumer-total-message-bytes"))
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
    return getClass().getSimpleName() + " id:" + this.id + " " + topicSensors.keySet();
  }
}
