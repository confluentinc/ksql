/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.metrics;

import com.google.common.collect.ImmutableMap;
import io.confluent.common.utils.Time;
import io.confluent.ksql.metrics.TopicSensors.SensorMetric;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MeasurableStat;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Rate;

public class ConsumerCollector implements MetricCollector, ConsumerInterceptor<Object, Object> {
  public static final String CONSUMER_MESSAGES_PER_SEC = "consumer-messages-per-sec";
  public static final String CONSUMER_TOTAL_MESSAGES = "consumer-total-messages";
  public static final String CONSUMER_TOTAL_BYTES = "consumer-total-bytes";

  private final Map<String, TopicSensors<ConsumerRecord<Object, Object>>> topicSensors =
      new HashMap<>();
  private Metrics metrics;
  private String id;
  private String groupId;
  private Time time;

  public void configure(final Map<String, ?> map) {
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
  public void onCommit(final Map<TopicPartition, OffsetAndMetadata> map) {
  }

  @Override
  public String getGroupId() {
    return this.groupId;
  }

  @Override
  public ConsumerRecords<Object, Object> onConsume(final ConsumerRecords<Object, Object> records) {
    collect(records);
    return records;
  }

  private void collect(final ConsumerRecords<Object, Object> consumerRecords) {
    final Stream<ConsumerRecord<Object, Object>> stream =
        StreamSupport.stream(consumerRecords.spliterator(), false);
    stream.forEach(record -> record(record.topic().toLowerCase(), false, record));
  }

  private void record(
      final String topic,
      final boolean isError,
      final ConsumerRecord<Object, Object> record
  ) {
    topicSensors.computeIfAbsent(getCounterKey(topic), k ->
        new TopicSensors<>(topic, buildSensors(k))
    ).increment(record, isError);
  }

  private String getCounterKey(final String topic) {
    return topic;
  }

  private List<SensorMetric<ConsumerRecord<Object, Object>>> buildSensors(
      final String key
  ) {
    final List<SensorMetric<ConsumerRecord<Object, Object>>> sensors = new ArrayList<>();

    // Note: synchronized due to metrics registry not handling concurrent add/check-exists
    // activity in a reliable way
    synchronized (this.metrics) {
      addSensor(key, CONSUMER_MESSAGES_PER_SEC, new Rate(), sensors, false);
      addSensor(key, CONSUMER_TOTAL_MESSAGES, new CumulativeSum(), sensors, false);
      addSensor(key, CONSUMER_TOTAL_BYTES, new CumulativeSum(), sensors, false,
          (r) -> {
            if (r == null) {
              return 0.0;
            } else {
              return ((double) r.serializedValueSize() + r.serializedKeySize());
            }
          });
    }
    return sensors;
  }

  private void addSensor(
      final String key,
      final String metricNameString,
      final MeasurableStat stat,
      final List<SensorMetric<ConsumerRecord<Object, Object>>> sensors,
      final boolean isError
  ) {
    addSensor(key, metricNameString, stat, sensors, isError, (r) -> (double)1);
  }

  private void addSensor(
      final String key,
      final String metricNameString,
      final MeasurableStat stat,
      final List<SensorMetric<ConsumerRecord<Object, Object>>> sensors,
      final boolean isError,
      final Function<ConsumerRecord<Object, Object>, Double> recordValue
  ) {
    final String name = "cons-" + key + "-" + metricNameString + "-" + id;

    final MetricName metricName = new MetricName(
        metricNameString,
        "consumer-metrics",
        "consumer-" + name,
        ImmutableMap.of("key", key, "id", id)
    );
    final Sensor existingSensor = metrics.getSensor(name);
    final Sensor sensor = metrics.sensor(name);

    // re-use the existing measurable stats to share between consumers
    if (existingSensor == null || metrics.metrics().get(metricName) == null) {
      sensor.add(metricName, stat);
    }

    final KafkaMetric metric = metrics.metrics().get(metricName);

    sensors.add(new SensorMetric<ConsumerRecord<Object, Object>>(sensor, metric, time, isError) {
      void record(final ConsumerRecord<Object, Object> record) {
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
  public Collection<TopicSensors.Stat> stats(final String topic, final boolean isError) {
    return MetricUtils.stats(topic, isError, topicSensors.values());
  }

  @Override
  public double aggregateStat(final String name, final boolean isError) {
    return MetricUtils.aggregateStat(name, isError, topicSensors.values());
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " id:" + this.id + " " + topicSensors.keySet();
  }
}
