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

import io.confluent.common.metrics.KafkaMetric;
import io.confluent.common.metrics.MetricName;
import io.confluent.common.metrics.Metrics;
import io.confluent.common.metrics.Sensor;
import io.confluent.common.metrics.stats.Rate;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ConsumerCollector {

  final Map<String, Counter> topicCounters = new HashMap<>();
  private Metrics metrics;
  private String id;

  public ConsumerCollector(Metrics metrics) {
    this.metrics = metrics;
  }


  public ConsumerRecords onConsume(ConsumerRecords records) {
    if (id == null) {
      throw new RuntimeException(new IllegalStateException("Cannot run without 'id' being set, use ConsumerCollector.configure(map)"));
    }

    collect(records);
    return records;
  }

  private void collect(ConsumerRecords consumerRecords) {
    Stream<ConsumerRecord> stream = StreamSupport.stream(consumerRecords.spliterator(), false);
    stream.forEach((record) -> {
      topicCounters.computeIfAbsent(record.topic(), k -> new Counter(id, metrics)).increment(record);
    });
  }
  public void configure(Map<String, ?> map) {
    this.id = (String) map.get(ConsumerConfig.GROUP_ID_CONFIG);
  }

  public void close() {
  }

  public String statsForTopic(String topic) {
    return topicCounters.containsKey(topic) ? "Consumer:" + topicCounters.get(topic).toString() : "";
  }

  public static class Counter {
    private final String id;
    private final Metrics metrics;
    private Sensor sensor;
    private KafkaMetric rate;

    public Counter(String id, Metrics metrics) {
      this.id = id;
      this.metrics = metrics;
    }

    Map<Integer, PartitionCounter> partitionCounter = new HashMap<>();
    long records;
    long valueBytes;

    public void increment(ConsumerRecord record) {
      records +=1;
      valueBytes += record.serializedValueSize();

      if (sensor == null) {
        String name = "consumer-" + id + "-" +record.topic() + "-" + record.partition();
        sensor = metrics.sensor(name);
        System.out.println("Registering:" + name);
        new RuntimeException("Register:" + name).printStackTrace();
        MetricName producerRate = new MetricName(name + "-rate-per-sec", name, "consumer-stats");
        sensor.add(producerRate, new Rate(TimeUnit.SECONDS));
        rate = metrics.metrics().get(producerRate);
      }
      sensor.record(1);

      partitionCounter.computeIfAbsent(record.partition(), k -> new PartitionCounter()).increment(record);
    }

    @Override
    public String toString() {
      return "Sensor: " + sensor.name() + " Metric: " + rate.metricName() + " Rate:" + rate.value() + " " + partitionCounter;
    }


    public static class PartitionCounter {
      long records;
      long valueBytes;
      void increment(ConsumerRecord record) {
        records++;
        valueBytes += record.serializedValueSize();
      }

      @Override
      public String toString() {
        return "Records:" + records + " bytes: " + valueBytes;
      }
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " Metrics:" + topicCounters.toString();
  }

}
