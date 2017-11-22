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

import com.google.common.collect.ImmutableMap;
import io.confluent.common.metrics.stats.Rate;
import org.apache.kafka.clients.producer.ProducerRecord;
import io.confluent.common.metrics.*;


import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ProducerCollector {

  private final Map<String, Counter> topicPartitionCounters = new HashMap<>();
  private final Metrics metrics;
  private final String id;

  ProducerCollector(Metrics metrics, String id) {
    this.metrics = metrics;
    this.id = id;
  }

  ProducerRecord onSend(ProducerRecord record) {
    collect(record);
    return record;
  }
  private void collect(ProducerRecord record) {
    topicPartitionCounters.computeIfAbsent(getKey(record.topic(), record.partition()), k ->
      new Counter<>(k, record.topic(), record.partition(), buildSensors(metrics, record))
    ).increment(record);
  }

  private Map<String, Counter.SensorMetric<ProducerRecord>> buildSensors(Metrics metrics, ProducerRecord record) {
    String name = "producer-" + id + "-" +record.topic();
    Sensor sensor = metrics.sensor(name);
    MetricName producerRate = new MetricName(name + "-rate-per-sec", name, "producer-statsAsString");
    sensor.add(producerRate, new Rate(TimeUnit.SECONDS));
    KafkaMetric rate = metrics.metrics().get(producerRate);
    return ImmutableMap.of(producerRate.name(), new Counter.SensorMetric<ProducerRecord>(sensor, rate){
      void record(ProducerRecord record) {
        sensor.record(1);
      }
    });
  }

  private String getKey(String topic, Integer partition) {
    return id + topic + partition;
  }

  public void close() {
    topicPartitionCounters.values().stream().forEach(v -> {
      v.close(metrics);
    });
  }

  public String statsForTopic(final String topic) {
    return topicPartitionCounters.values().stream().filter(counter -> counter.isTopic(topic)).map(record -> record.statsAsString()).collect(Collectors.joining(", "));
  }



  @Override
  public String toString() {
    return getClass().getSimpleName() + " " + this.topicPartitionCounters.toString();
  }
}
