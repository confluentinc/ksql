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
import io.confluent.common.utils.Time;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MeasurableStat;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.Total;

import java.util.*;

public class ProducerCollector implements MetricCollector {

  private final Map<String, Counter> topicPartitionCounters = new HashMap<>();
  private Metrics metrics;
  private String id;
  private Time time;

  public void configure(Map<String, ?> map) {
    String id = (String) map.get(ProducerConfig.CLIENT_ID_CONFIG);
    configure(MetricCollectors.getMetrics(), MetricCollectors.addCollector(id, this), MetricCollectors.getTime());
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

  public ProducerRecord onSend(ProducerRecord record) {
    collect(record);
    return record;
  }

  private void collect(ProducerRecord record) {
    topicPartitionCounters.computeIfAbsent(getKey(record.topic()), k ->
            new Counter<>(record.topic(), buildSensors(k))
    ).increment(record);
  }

  private Map<String, Counter.SensorMetric<ProducerRecord>> buildSensors(String key) {
    HashMap<String, Counter.SensorMetric<ProducerRecord>> sensors = new HashMap<>();

    // Note: synchronized due to metrics registry not handling concurrent add/check-exists activity in a reliable way
    synchronized (metrics) {
      addSensor(key, "events-per-sec", new Rate(), sensors);
      addSensor(key, "total-events", new Total(), sensors);
    }
    return sensors;
  }

  private void addSensor(String key, String metricNameString, MeasurableStat stat, HashMap<String, Counter.SensorMetric<ProducerRecord>> results) {
    String name = "prod-" + key + "-" + metricNameString + "-" + id;

    MetricName metricName = new MetricName(metricNameString, "producer-metrics", "producer-" + name,  ImmutableMap.of("key", key, "id", id));
    Sensor existingSensor = metrics.getSensor(name);
    Sensor sensor = metrics.sensor(name);

    // either a new sensor or a new metric with different id
    if (existingSensor == null ||  metrics.metrics().get(metricName) == null) {
      sensor.add(metricName, stat);
    }
    KafkaMetric metric = metrics.metrics().get(metricName);

    results.put(metricName.name(), new Counter.SensorMetric<ProducerRecord>(sensor, metric, time) {
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
    topicPartitionCounters.values().forEach(v -> v.close(metrics));
  }

  public Collection<Counter.Stat> stats(String topic) {
    final List<Counter.Stat> list = new ArrayList<>();
    topicPartitionCounters.values().stream().filter(counter -> counter.isTopic(topic)).forEach(record -> list.addAll(record.stats()));
    return list;
  }


  @Override
  public String toString() {
    return getClass().getSimpleName() + " " + this.id + " " + this.topicPartitionCounters.toString();
  }
}
