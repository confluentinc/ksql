/**
 * Copyright 2017 Confluent Inc.
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

import io.confluent.common.metrics.stats.Rate;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import io.confluent.common.metrics.*;


import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ProducerCollector {

  final Map<String, Counter> topicCounters = new HashMap<>();
  private final Metrics metrics;
  private String id;

  public ProducerCollector(Metrics metrics) {
    this.metrics = metrics;
  }

  public ProducerRecord onSend(ProducerRecord record) {
    if (id == null) {
      throw new RuntimeException(new IllegalStateException("Cannot run without 'id' being set, use  ProducerCollector.configure(map)"));
    }
    collect(record);
    return record;
  }
  private void collect(ProducerRecord record) {
    topicCounters.computeIfAbsent(record.topic(), k -> new Counter()).increment(id, metrics, record);
  }

  public void close() {
    topicCounters.values().stream().forEach(v -> {
      v.close(metrics);
    });
  }

  public String statsForTopic(String topic) {
    return topicCounters.containsKey(topic) ? "Producer:" + topicCounters.get(topic).toString() : "";
  }

  public void configure(Map<String, ?> config) {
    this.id = (String) config.get(ProducerConfig.CLIENT_ID_CONFIG);
  }

  public static class Counter {
    Map<Integer, PartitionCounter> partitionCounters = new HashMap<>();
    private Sensor sensor;
    private KafkaMetric rate;


    public void increment(String id, Metrics metrics, ProducerRecord record) {
      if (sensor == null) {
        String name = "producer-" + id + "-" +record.topic();
        sensor = metrics.sensor(name);
        MetricName producerRate = new MetricName(name + "-rate-per-sec", name, "producer-stats");
        sensor.add(producerRate, new Rate(TimeUnit.SECONDS));
        rate = metrics.metrics().get(producerRate);
      }
      sensor.record(1);
      partitionCounters.computeIfAbsent(record.partition(), k -> new PartitionCounter()).increment(record);
    }

    @Override
    public String toString() {
      return "Sensor: " + sensor.name() + " Metric: " + rate.metricName() + " Rate:" + rate.value() + " " + partitionCounters;
    }

    public void close(Metrics metrics) {
    // TODO: not yet supported in commons-metrics
    // metrics.removeSensor(sensor.name());
    }

    public static class PartitionCounter {
      long records;
      void increment(ProducerRecord record) {
        records++;
      }

      @Override
      public String toString() {
        return   "Records:" + records;
      }
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " " + this.topicCounters.toString();
  }
}
