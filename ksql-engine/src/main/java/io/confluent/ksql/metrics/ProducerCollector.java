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

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.Total;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@SuppressWarnings("unchecked")
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
    topicPartitionCounters.computeIfAbsent(getKey(record.topic()), k ->
            new Counter<>(record.topic(), buildSensors(k, metrics))
    ).increment(record);
  }

  private Map<String, Counter.SensorMetric<ProducerRecord>> buildSensors(String key, Metrics metrics) {
    HashMap<String, Counter.SensorMetric<ProducerRecord>> sensors = new HashMap<>();

    // Note: syncronized due to metrics registry not handling concurrent add/check-exists activity in a reliable way
    synchronized (metrics) {
      addRatePerSecond(key, metrics, sensors);
      addTotalSensor(key, metrics, sensors);
    }
    return sensors;
  }

  @SuppressWarnings("unchecked")
  private void addRatePerSecond(String key, Metrics metrics, HashMap<String, Counter.SensorMetric<ProducerRecord>> sensors) {
    String name = "prod-" + key + "-" + "-rate-per-sec";

    MetricName producerRate = new MetricName("produce rate-per-sec", name, "producer-statsAsString",Collections.EMPTY_MAP);
    Sensor existingSensor = metrics.getSensor(name);
    Sensor sensor = metrics.sensor(name);

    if (existingSensor == null) {
      sensor.add(producerRate, new Rate(TimeUnit.SECONDS));
    }

    KafkaMetric rate = metrics.metrics().get(producerRate);
    sensors.put(sensor.name(), new Counter.SensorMetric<ProducerRecord>(sensor, rate) {
      void record(ProducerRecord record) {
        sensor.record(1);
        super.record(record);
      }
    });
  }


  private void addTotalSensor(String key, Metrics metrics, HashMap<String, Counter.SensorMetric<ProducerRecord>> results) {
    String name = "prod-" + key + "-total-events";

    MetricName metricName = new MetricName("total-events", name, "producer-total-events",Collections.EMPTY_MAP);
    Sensor existingSensor = metrics.getSensor(name);
    Sensor sensor = metrics.sensor(name);

    if (existingSensor == null) {
      sensor.add(metricName, new Total());
    }
    KafkaMetric metric = metrics.metrics().get(metricName);

    results.put(metricName.name(), new Counter.SensorMetric<ProducerRecord>(sensor, metric) {
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
    topicPartitionCounters.values().forEach(v -> v.close(metrics));
  }

  // TODO: use this:
  // https://stackoverflow.com/questions/25439277/lambdas-multiple-foreach-with-casting
  public String statsForTopic(final String topic, final boolean verbose) {

    List<Counter> last = new ArrayList<>();

    String stats = topicPartitionCounters.values().stream().filter(counter -> (counter.isTopic(topic) ? last.add(counter) || true  : false)).map(record -> record.statsAsString(verbose)).collect(Collectors.joining(", "));

    if (!last.isEmpty()) {
      // Add timestamp information
      Counter.SensorMetric sensor = (Counter.SensorMetric) last.stream().findFirst().get().sensors.values().stream().findFirst().get();

      if (sensor != null) {
        stats += " " + sensor.lastEventTime();
      }
    }
    return stats;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " " + this.topicPartitionCounters.toString();
  }
}
