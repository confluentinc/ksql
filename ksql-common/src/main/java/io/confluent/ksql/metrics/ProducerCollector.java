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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MeasurableStat;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.Total;

public class ProducerCollector implements MetricCollector, ProducerInterceptor {
  public static final String PRODUCER_MESSAGES_PER_SEC = "messages-per-sec";
  public static final String PRODUCER_TOTAL_MESSAGES = "total-messages";

  private final Map<String, TopicSensors<ProducerRecord>> topicSensors = new HashMap<>();
  private Metrics metrics;
  private String id;
  private Time time;

  public void configure(final Map<String, ?> map) {
    final String id = (String) map.get(ProducerConfig.CLIENT_ID_CONFIG);
    configure(
        MetricCollectors.getMetrics(),
        MetricCollectors.addCollector(id, this),
        MetricCollectors.getTime()
    );
  }

  ProducerCollector configure(final Metrics metrics, final String id, final Time time) {
    this.id = id;
    this.metrics = metrics;
    this.time = time;
    return this;
  }

  @Override
  public void onAcknowledgement(final RecordMetadata recordMetadata, final Exception e) {
  }

  @Override
  public ProducerRecord onSend(final ProducerRecord record) {
    collect(record, false);
    return record;
  }

  private void collect(final ProducerRecord record, final boolean isError) {
    collect(isError, record.topic().toLowerCase());
  }

  private void collect(final boolean isError, final String topic) {
    topicSensors
        .computeIfAbsent(getKey(topic), k -> new TopicSensors<>(topic, buildSensors(k)))
        .increment(null, isError);
  }


  private List<TopicSensors.SensorMetric<ProducerRecord>> buildSensors(final String key) {
    final List<TopicSensors.SensorMetric<ProducerRecord>> sensors = new ArrayList<>();

    // Note: synchronized due to metrics registry not handling concurrent add/check-exists
    // activity in a reliable way
    synchronized (metrics) {
      addSensor(key, PRODUCER_MESSAGES_PER_SEC, new Rate(), sensors);
      addSensor(key, PRODUCER_TOTAL_MESSAGES, new Total(), sensors);
    }
    return sensors;
  }

  private void addSensor(
      final String key,
      final String metricNameString,
      final MeasurableStat stat,
      final List<TopicSensors.SensorMetric<ProducerRecord>> results
  ) {
    final String name = "prod-" + key + "-" + metricNameString + "-" + id;

    final MetricName metricName = new MetricName(
        metricNameString,
        "producer-metrics",
        "producer-" + name,
        ImmutableMap.of("key", key, "id", id)
    );
    final Sensor existingSensor = metrics.getSensor(name);
    final Sensor sensor = metrics.sensor(name);

    // either a new sensor or a new metric with different id
    if (existingSensor == null || metrics.metrics().get(metricName) == null) {
      sensor.add(metricName, stat);
    }
    final KafkaMetric metric = metrics.metrics().get(metricName);

    results.add(new TopicSensors.SensorMetric<ProducerRecord>(sensor, metric, time, false) {
      void record(final ProducerRecord record) {
        sensor.record(1);
        super.record(record);
      }
    });
  }


  private String getKey(final String topic) {
    return topic;
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
    return getClass().getSimpleName() + " " + this.id + " " + this.topicSensors.toString();
  }
}
