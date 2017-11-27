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

import io.confluent.common.metrics.stats.Rate;
import io.confluent.common.metrics.stats.Total;
import org.apache.kafka.clients.producer.ProducerRecord;
import io.confluent.common.metrics.*;


import java.util.*;
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
            new Counter<>(k, record.topic(), record.partition(), buildSensors(k, metrics, record))
    ).increment(record);
  }

  private Map<String, Counter.SensorMetric<ProducerRecord>> buildSensors(String key, Metrics metrics, ProducerRecord record) {
    HashMap<String, Counter.SensorMetric<ProducerRecord>> sensors = new HashMap<>();

    addRatePerSecond(key, metrics, sensors);
    addTotalSensor(key, metrics, sensors);
    return sensors;
  }

  private void addRatePerSecond(String key, Metrics metrics, HashMap<String, Counter.SensorMetric<ProducerRecord>> sensors) {
    String name = "prod-" + key + "-" + "-rate-per-sec";

    Sensor existingSensor = metrics.getSensor(name);

    if (existingSensor != null) {

      System.out.println("prod Existing...." + name);
      // hook into existing sensor?
      MetricName producerRate = new MetricName("rate-per-sec", name, "producer-statsAsString");
      KafkaMetric rate = metrics.metrics().get(producerRate);
      sensors.put(existingSensor.name(), new Counter.SensorMetric<ProducerRecord>(existingSensor, rate) {
        void record(ProducerRecord record) {
          existingSensor.record(1);
          super.record(record);
        }
      });

    } else {
      System.out.println("prod new....:" + name + " id:" + id);
      Sensor sensor = metrics.sensor(name);
      MetricName producerRate = new MetricName("rate-per-sec", name, "producer-statsAsString");
      sensor.add(producerRate, new Rate(TimeUnit.SECONDS));
      KafkaMetric rate = metrics.metrics().get(producerRate);
      sensors.put(producerRate.name(), new Counter.SensorMetric<ProducerRecord>(sensor, rate) {
        void record(ProducerRecord record) {
          sensor.record(1);
          super.record(record);
        }
      });
    }
  }


  private void addTotalSensor(String key, Metrics metrics, HashMap<String, Counter.SensorMetric<ProducerRecord>> results) {
    String name = "prod-" + key + "-total-events";
    Sensor sensor = metrics.sensor(name);
    MetricName metricName = new MetricName("total-events", name, "producer-total-events");
    sensor.add(metricName, new Total());
    KafkaMetric metric = metrics.metrics().get(metricName);

    results.put(metricName.name(), new Counter.SensorMetric<ProducerRecord>(sensor, metric) {
      void record(ProducerRecord record) {
        sensor.record(1);
        super.record(record);
      }
    });
  }


  private String getKey(String topic, Integer partition) {
    // dont use the producer-id - streams uses a unique id per thread - we need to agg across the producer and partitions at write time because sensors etc are immutable and PITA to reconstruct
    // return id + "_" + topic + "_" + partition;
    return topic;// + "_" + partition;// + "-" + registered++;
  }

  public void close() {
    topicPartitionCounters.values().stream().forEach(v -> {
      v.close(metrics);
    });

    // how do I release metrics if they are shared??? FFS!
    //topicPartitionCounters.values().stream().forEach();
  }

  // TODO: use this:
  // https://stackoverflow.com/questions/25439277/lambdas-multiple-foreach-with-casting
  public String statsForTopic(final String topic, final boolean verbose) {
    List<Counter> last = new ArrayList<>();

    String collect = topicPartitionCounters.values().stream().filter(counter -> (counter.isTopic(topic) ? last.add(counter) || true  : false)).map(record -> record.statsAsString(verbose)).collect(Collectors.joining(", "));

    if (last.size() > 0) {
      // Add the sensor timestamp information
      Counter.SensorMetric sensor = (Counter.SensorMetric) last.stream().findFirst().get().sensors.values().stream().findFirst().get();

      if (sensor != null) {
        collect += " " + sensor.lastEventTime();
      }
    }
    return collect;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " " + this.topicPartitionCounters.toString();
  }
}
