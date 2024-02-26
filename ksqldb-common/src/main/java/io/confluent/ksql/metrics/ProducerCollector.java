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
import io.confluent.ksql.util.KsqlConfig;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MeasurableStat;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Rate;

public class ProducerCollector implements MetricCollector, ProducerInterceptor<Object, Object> {
  public static final String PRODUCER_MESSAGES_PER_SEC = "messages-per-sec";
  public static final String PRODUCER_TOTAL_MESSAGES = "total-messages";

  private MetricCollectors metricsCollectors;
  private Metrics metrics;
  private final Map<String, TopicSensors<ProducerRecord<Object, Object>>> topicSensors =
      new HashMap<>();
  private String id;
  private Time time;

  public void configure(final Map<String, ?> map) {
    final String id = (String) map.get(ProducerConfig.CLIENT_ID_CONFIG);
    final MetricCollectors collectors = (MetricCollectors) Objects.requireNonNull(
        map.get(KsqlConfig.KSQL_INTERNAL_METRIC_COLLECTORS_CONFIG)
    );
    configure(id, collectors);
  }

  void configure(final String id, final MetricCollectors metricCollectors) {
    this.metricsCollectors = metricCollectors;
    this.metrics = metricCollectors.getMetrics();
    this.id = metricCollectors.addCollector(id, this);
    this.time = metricCollectors.getTime();
  }

  @Override
  public void onAcknowledgement(final RecordMetadata recordMetadata, final Exception e) {
  }

  @Override
  public ProducerRecord<Object, Object> onSend(final ProducerRecord<Object, Object> record) {
    collect(record, false);
    return record;
  }

  private void collect(final ProducerRecord<Object, Object> record, final boolean isError) {
    collect(isError, record.topic().toLowerCase());
  }

  private void collect(final boolean isError, final String topic) {
    topicSensors
        .computeIfAbsent(getKey(topic), k -> new TopicSensors<>(topic, buildSensors(k)))
        .increment(null, isError);
  }


  private List<SensorMetric<ProducerRecord<Object, Object>>> buildSensors(final String key) {
    final List<SensorMetric<ProducerRecord<Object, Object>>> sensors = new ArrayList<>();

    // Note: synchronized due to metrics registry not handling concurrent add/check-exists
    // activity in a reliable way
    synchronized (metrics) {
      addSensor(key, PRODUCER_MESSAGES_PER_SEC, new Rate(), sensors);
      addSensor(key, PRODUCER_TOTAL_MESSAGES, new CumulativeSum(), sensors);
    }
    return sensors;
  }

  private void addSensor(
      final String key,
      final String metricNameString,
      final MeasurableStat stat,
      final List<SensorMetric<ProducerRecord<Object, Object>>> results
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

    results.add(
        new SensorMetric<ProducerRecord<Object, Object>>(sensor, metric, time, false) {
          void record(final ProducerRecord<Object, Object> record) {
            sensor.record(1);
            super.record(record);
          }
        });
  }


  private String getKey(final String topic) {
    return topic;
  }

  public void close() {
    metricsCollectors.remove(this.id);
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
