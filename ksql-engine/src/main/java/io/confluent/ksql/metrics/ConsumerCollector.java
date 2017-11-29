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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.Total;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Interceptor for collecting ksql-stats
 * Design notes:
 * </p>
 * Measurable stats are shared across all partitions as well as consumers.
 * </p>
 * Limitations:
 * Stats are only collected for a consumer's topic(s). As such they are aggregated across all partitions at intercept-time; the
 * alternative would be to aggregate on read, and also store records at the partition level.
 * </p>
 * The downside to this approach is:
 * - Potential performance penalty during interception due to sharing of AKMetrics across Consumer threads
 * - Accuracy of stats, AK Metrics are not thread safe, as such stats are indicative
 * - Lack of partition level metrics
 * - collected at process level - a higher order aggregator would be needed to collect across multiple processed
 * </p>
 * Potential issues:
 * - With metric leaks where a client interceptor is closed(), De-registration of sensors and metrics where other clients are still connected (in the same process).
 * - Around dynamic resource allocation (ie. stream threads are added/removed)
 * </p>
 * Benefits:
 * - Smaller memory footprint due to agg on intercept
 */
public class ConsumerCollector implements MetricCollector {

  private final Map<String, Counter> topicPartitionCounters = new HashMap<>();
  private Metrics metrics;
  private String id;

  public void configure(Map<String, ?> map) {
    String id = (String) map.get(ConsumerConfig.GROUP_ID_CONFIG);
    configure(MetricCollectors.addCollector(id, this), id);
  }

  ConsumerCollector configure(final Metrics metrics, final String id) {
    this.id = id;
    this.metrics = metrics;
    return this;
  }

  @Override
  public String getId() {
    return id;
  }

  public ConsumerRecords onConsume(ConsumerRecords records) {
    collect(records);
    return records;
  }

  @SuppressWarnings("unchecked")
  private void collect(ConsumerRecords consumerRecords) {
    Stream<ConsumerRecord> stream = StreamSupport.stream(consumerRecords.spliterator(), false);
    stream.forEach((record) -> topicPartitionCounters.computeIfAbsent(getKey(record.topic().toLowerCase()), k ->
            new Counter<>(record.topic().toLowerCase(), buildSensors(k))
    ).increment(record));
  }

  private String getKey(String topic) {
    return topic;
  }

  private Map<String, Counter.SensorMetric<ConsumerRecord>> buildSensors(String key) {

    HashMap<String, Counter.SensorMetric<ConsumerRecord>> results = new HashMap<>();

    // Note: synchronized due to metrics registry not handling concurrent add/check-exists activity in a reliable way
    synchronized (this.metrics) {
      addRateSensor(key, results);
      addBandwidthSensor(key, results);
      addTotalSensor(key, results);
    }
    return results;
  }

  private void addRateSensor(String key, HashMap<String, Counter.SensorMetric<ConsumerRecord>> results) {
    String name = "cons-" + key + "-rate-per-sec";

    //noinspection unchecked
    MetricName metricName = new MetricName("consume rate-per-sec", name, "consumer-rate-per-sec",  Collections.EMPTY_MAP);
    Sensor existingSensor = metrics.getSensor(name);
    Sensor sensor = metrics.sensor(name);

    // re-use the existing measurable stats to share between consumers
    if (existingSensor == null) {
      sensor.add(metricName, new Rate(TimeUnit.SECONDS));
    }

    KafkaMetric rate = metrics.metrics().get(metricName);
    results.put(metricName.name(), new Counter.SensorMetric<ConsumerRecord>(sensor, rate) {
      void record(ConsumerRecord record) {
        sensor.record(1);
        super.record(record);
      }
    });
  }

  private void addBandwidthSensor(String key, HashMap<String, Counter.SensorMetric<ConsumerRecord>> results) {
    String name = "cons-" + key + "-bytes-per-sec";

    //noinspection unchecked
    MetricName metricName = new MetricName("bytes-per-sec", name, "consumer-bytes-per-sec", Collections.EMPTY_MAP);
    Sensor existingSensor = metrics.getSensor(name);
    Sensor sensor = metrics.sensor(name);

    // re-use the existing measurable stats to share between consumers
    if (existingSensor == null) {
      sensor.add(metricName, new Rate(TimeUnit.SECONDS));
    }
    KafkaMetric metric = metrics.metrics().get(metricName);

    results.put(metricName.name(), new Counter.SensorMetric<ConsumerRecord>(sensor, metric) {
      void record(ConsumerRecord record) {
        sensor.record(record.serializedValueSize());
        super.record(record);
      }
    });
  }

  private void addTotalSensor(String key, HashMap<String, Counter.SensorMetric<ConsumerRecord>> sensors) {
    String name = "cons-" + key + "-total-events";

    //noinspection unchecked
    MetricName metricName = new MetricName("total-events", name, "consumer-total-events", Collections.EMPTY_MAP);
    Sensor existingSensor = metrics.getSensor(name);
    Sensor sensor = metrics.sensor(name);

    // re-use the existing measurable stats to share between consumers
    if (existingSensor == null) {
      sensor.add(metricName, new Total());
    }
    KafkaMetric metric = metrics.metrics().get(metricName);

    sensors.put(metricName.name(), new Counter.SensorMetric<ConsumerRecord>(sensor, metric) {
      void record(ConsumerRecord record) {
        sensor.record(1);
        super.record(record);
      }
    });
  }

  public void close() {
    MetricCollectors.remove(this.id);
    topicPartitionCounters.values().forEach(v -> v.close(metrics));
  }

  public String statsForTopic(String topic) {
    return statsForTopic(topic, false);
  }

  public String statsForTopic(final String topic, boolean verbose) {
    List<Counter> last = new ArrayList<>();

    String stats = topicPartitionCounters.values().stream().filter(counter -> (counter.isTopic(topic)  && last.add(counter))).map(record -> record.statsAsString(verbose)).collect(Collectors.joining(", "));

    // Add timestamp information
    if (!last.isEmpty()) {
      Counter.SensorMetric sensor = (Counter.SensorMetric) last.stream().findFirst().get().sensors.values().stream().findFirst().get();
      stats += " " + sensor.lastEventTime();
    }
    return stats;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " id:" + this.id + " " + topicPartitionCounters.keySet();
  }
}
