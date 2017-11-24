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

import io.confluent.common.metrics.KafkaMetric;
import io.confluent.common.metrics.Metrics;
import io.confluent.common.metrics.Sensor;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public class Counter<R> {

  private final String id;
  private String topic;
  private final Integer partition;
  Map<String, SensorMetric<R>> sensors = new HashMap<>();

  Counter(final String id, String topic, Integer partition, final Map<String, SensorMetric<R>> sensors) {
    this.id = id;
    this.topic = topic;
    this.partition = partition;
    this.sensors = sensors;
  }

  void increment(R record) {
    sensors.values().stream().forEach(v -> v.record(record));
  }

  public void close(Metrics metrics) {
    sensors.values().stream().forEach(v -> v.close(metrics));
  }

  public boolean isTopic(String topic) {
    return this.topic.equals(topic);
  }

  public String statsAsString(boolean verbose) {
    return sensors.values().stream().map(sensor -> sensor.toString(verbose)).collect(Collectors.joining("  "));
  }

  public Map<String, SensorMetric<R>> stats() {
    return sensors;
  }

  abstract static class SensorMetric<P> {
    private final Sensor sensor;
    private final KafkaMetric metric;
    private long lastEvent = 0;

    SensorMetric(Sensor sensor, KafkaMetric metric) {
      this.sensor = sensor;
      this.metric = metric;
    }

    /**
     * Anon class must call down to this for timestamp recording
     * @param object
     */
    void record(P object) {
      this.lastEvent = System.currentTimeMillis();
    }

    public KafkaMetric metric() {
      return metric;
    }

    public Sensor sensor() {
      return sensor;
    }

    public String lastEventTime() {
      if (lastEvent == 0) return "No-events";
      return "Last-event: " + SimpleDateFormat.getDateTimeInstance(3, 1, Locale.getDefault()).format(new Date(lastEvent));
    }

    public void close(Metrics metrics) {
      // TODO: not yet supported in commons-metrics
      // metrics.removeSensor(sensor.name());
    }

    public String toString(boolean verbose) {
      if (verbose) {
        return metric.metricName().group() + "." + metric.metricName().name() + ":" + metric.value();
      } else {
        return metric.metricName().name() + ":" + String.format("%10.2f", metric.value());
      }
    }

    @Override
    public String toString() {
      return toString(false);
    }
  }
}
