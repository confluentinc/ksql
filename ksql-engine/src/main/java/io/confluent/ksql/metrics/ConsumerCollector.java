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

import io.confluent.common.metrics.KafkaMetric;
import io.confluent.common.metrics.MetricName;
import io.confluent.common.metrics.Metrics;
import io.confluent.common.metrics.Sensor;
import io.confluent.common.metrics.stats.Rate;
import io.confluent.common.metrics.stats.Total;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ConsumerCollector {

  private final Map<String, Counter> topicPartitionCounters = new HashMap<>();
  private final Metrics metrics;
  private final String id;

  ConsumerCollector(Metrics metrics, String id) {
    this.metrics = metrics;
    this.id = id;
  }

  void onConsume(ConsumerRecords records) {
    collect(records);
  }

  @SuppressWarnings("unchecked")
  private void collect(ConsumerRecords consumerRecords) {
    Stream<ConsumerRecord> stream = StreamSupport.stream(consumerRecords.spliterator(), false);
    stream.forEach((record) -> {

      topicPartitionCounters.computeIfAbsent(getKey(record.topic(), record.partition()), k ->
              new Counter<>(k, record.topic(), record.partition(), buildSensors(k, metrics, record))
      ).increment(record);

    });
  }

  // TODO: per stream thread uses its own consumer - either share stats (perf overhead) or handle keying
  private String getKey(String topic, Integer partition) {
    // dont record at partition level because we cannot aggregate across them...
    return id + "_" + topic + "_";// + partition;// + "-" + registered++;
  }

  private Map<String, Counter.SensorMetric<ConsumerRecord>> buildSensors(String key, Metrics metrics, ConsumerRecord record) {

    HashMap<String, Counter.SensorMetric<ConsumerRecord>> results = new HashMap<>();

    addRateSensor(key, metrics, results);
    addBandwidthSensor(key, metrics, results);
    addTotalSensor(key, metrics, results);

    return results;
  }

  private void addRateSensor(String key, Metrics metrics, HashMap<String, Counter.SensorMetric<ConsumerRecord>> results) {
    String name = "cons-" + key + "-rate-per-sec";
    Sensor existingSensor = metrics.getSensor(name);

    if (existingSensor == null) {
      System.out.println("cons NEW:" + name + " id:" + id);
      Sensor sensor = metrics.sensor(name);

      MetricName consumerRate = new MetricName("rate-per-sec", name, "consumer-rate-per-sec");
      sensor.add(consumerRate, new Rate(TimeUnit.SECONDS));
      KafkaMetric rate = metrics.metrics().get(consumerRate);

      results.put(consumerRate.name(), new Counter.SensorMetric<ConsumerRecord>(sensor, rate) {
        void record(ConsumerRecord record) {
          sensor.record(1);
          super.record(record);
        }
      });
    } else {
      System.out.println("cons EXISTING:" + name);
    }
  }

  private void addBandwidthSensor(String key, Metrics metrics, HashMap<String, Counter.SensorMetric<ConsumerRecord>> results) {
    String name = "cons-" + key + "-bytes-per-sec";
    Sensor sensor = metrics.sensor(name);
    MetricName metricName = new MetricName("bytes-per-sec", name, "consumer-bytes-per-sec");
    sensor.add(metricName, new Rate(TimeUnit.SECONDS));
    KafkaMetric metric = metrics.metrics().get(metricName);

    results.put(metricName.name(), new Counter.SensorMetric<ConsumerRecord>(sensor, metric) {
      void record(ConsumerRecord record) {
        sensor.record(record.serializedValueSize());
        super.record(record);
      }
    });
  }

  private void addTotalSensor(String key, Metrics metrics, HashMap<String, Counter.SensorMetric<ConsumerRecord>> sensors) {
    String name = "cons-" + key + "-total-events";
    Sensor sensor = metrics.sensor(name);
    MetricName metricName = new MetricName("total-events", name, "consumer-total-events");
    sensor.add(metricName, new Total());
    KafkaMetric metric = metrics.metrics().get(metricName);

    sensors.put(metricName.name(), new Counter.SensorMetric<ConsumerRecord>(sensor, metric) {
      void record(ConsumerRecord record) {
        sensor.record(record.serializedValueSize());
        super.record(record);
      }
    });
  }

  public void close() {
  }

  public String statsForTopic(final String topic, boolean verbose) {
    return topicPartitionCounters.values().stream().filter(counter -> counter.isTopic(topic)).map(record -> record.statsAsString(verbose)).collect(Collectors.joining(", "));
  }

  public Map<String, Counter.SensorMetric> stats(final String topic) {
    Map<String, Counter.SensorMetric> metrics = new HashMap<>();

    topicPartitionCounters.values().stream().filter(counter ->
        counter.isTopic(topic)).forEach(counter ->
        ((Map<String, Counter.SensorMetric>) counter.sensors).values().stream().forEach(metric -> {
          // metrics.computeIfAbsent(metric.sensor().name(), k -> metric.clone()).sensor().record(metric.metric().value());
          if (metrics.containsKey(metric.sensor().name())) {
            System.err.println("already got metrics for:" + metric.metric().metricName());
            //metrics.get(metric.sensor().name()).sensor().record(metric.metric().value());
          } else {
            metrics.put(metric.sensor().name(), metric);
          }
        }
        )
    );

    return metrics;
  }


}
