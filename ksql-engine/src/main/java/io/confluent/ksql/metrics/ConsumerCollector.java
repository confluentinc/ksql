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
import io.confluent.common.metrics.KafkaMetric;
import io.confluent.common.metrics.MetricName;
import io.confluent.common.metrics.Metrics;
import io.confluent.common.metrics.Sensor;
import io.confluent.common.metrics.stats.Rate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.HashMap;
import java.util.Map;
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
              new Counter<>(k, record.topic(), record.partition(), buildSensors(metrics, record))
      ).increment(record);

    });
  }

  private String getKey(String topic, Integer partition) {
    return id + topic + partition;
  }

  private Map<String, Counter.SensorMetric<ConsumerRecord>> buildSensors(Metrics metrics, ConsumerRecord record) {

    HashMap<String, Counter.SensorMetric<ConsumerRecord>> results = new HashMap<>();

    addConsumerRateSensor(metrics, record, results);
    addConsumerBandwidthSensor(metrics, record, results);

    return results;
  }

  private void addConsumerRateSensor(Metrics metrics, ConsumerRecord record, HashMap<String, Counter.SensorMetric<ConsumerRecord>> results) {
    String name = "consumer-" + id + "-" +record.topic() + "-rate-per-sec";
    Sensor sensor = metrics.sensor(name);
    MetricName consumerRate = new MetricName(name + "-rate-per-sec", name, "consumer-statsAsString");
    sensor.add(consumerRate, new Rate(TimeUnit.SECONDS));
    KafkaMetric rate = metrics.metrics().get(consumerRate);

    results.put(consumerRate.name(), new Counter.SensorMetric<ConsumerRecord>(sensor, rate){
      void record(ConsumerRecord record) {
        sensor.record(1);
      }
    });
  }
  private void addConsumerBandwidthSensor(Metrics metrics, ConsumerRecord record, HashMap<String, Counter.SensorMetric<ConsumerRecord>> results) {
    String name = "consumer-" + id + "-" +record.topic() + "-bytes-per-sec";
    Sensor sensor = metrics.sensor(name);
    MetricName consumerRate = new MetricName(name + "-bytes-per-sec", name, "consumer-statsAsString");
    sensor.add(consumerRate, new Rate(TimeUnit.SECONDS));
    KafkaMetric rate = metrics.metrics().get(consumerRate);

    results.put(consumerRate.name(), new Counter.SensorMetric<ConsumerRecord>(sensor, rate){
      void record(ConsumerRecord record) {
        sensor.record(record.serializedValueSize());
      }
    });
  }

  public void close() {
  }

  public String statsForTopic(final String topic) {
    return topicPartitionCounters.values().stream().filter(counter -> counter.isTopic(topic)).map(record -> record.statsAsString()).collect(Collectors.joining(", "));
  }

}
