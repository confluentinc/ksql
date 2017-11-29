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

import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

class Counter<R> {

  private final String topic;
  Map<String, SensorMetric<R>> sensors = new HashMap<>();

  Counter(String topic, final Map<String, SensorMetric<R>> sensors) {
    this.topic = topic.toLowerCase();
    this.sensors = sensors;
  }

  void increment(R record) {
    sensors.values().forEach(v -> v.record(record));
  }

  public void close(Metrics metrics) {
    sensors.values().forEach(v -> v.close(metrics));
  }

  public boolean isTopic(String topic) {
    return this.topic.equals(topic);
  }

  public String statsAsString(boolean verbose) {
    return sensors.values().stream().map(sensor -> sensor.toString(verbose)).collect(Collectors.joining("  "));
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

    public double value() {
      return metric.measurable().measure(metric.config(), System.currentTimeMillis());
    }

    public String lastEventTime() {
      if (lastEvent == 0) return "No-events";
      return "Last-event: " + SimpleDateFormat.getDateTimeInstance(3, 1, Locale.getDefault()).format(new Date(lastEvent));
    }

    public void close(Metrics metrics) {
      metrics.removeSensor(sensor.name());
      metrics.removeMetric(metric.metricName());
    }

    public String toString(boolean verbose) {
      if (verbose) {
        return metric.metricName().group() + "." + metric.metricName().name() + ":" +  String.format("%10.2f", value());
      } else {
        return metric.metricName().name() + ":" + String.format("%10.2f", value());
      }
    }

    @Override
    public String toString() {
      return toString(false);
    }
  }
}
