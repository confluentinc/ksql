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

import io.confluent.common.utils.Time;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

class TopicSensors<R> {

  private final String topic;
  private final List<SensorMetric<R>> sensors;

  TopicSensors(String topic, final List<SensorMetric<R>> sensors) {
    this.topic = topic.toLowerCase();
    this.sensors = sensors;
  }

  void increment(R record) {
    sensors.forEach(v -> v.record(record));
  }

  public void close(Metrics metrics) {
    sensors.forEach(v -> v.close(metrics));
  }

  boolean isTopic(String topic) {
    return this.topic.equals(topic);
  }

  Collection<Stat> stats() {
    return sensors.stream().map(sensor -> sensor.asStat()).collect(Collectors.toList());
  }

  static class Stat {
    private final String name;
    private double value;
    private final long timestamp;


    public Stat(String name, double value, long timestamp) {
      this.name = name;
      this.value = value;
      this.timestamp = timestamp;
    }
    public String formatted() {
      return  String.format("%s:%10.2f", name, value);
    }

    public String timestamp() {
      return SimpleDateFormat.getDateTimeInstance(3, 1, Locale.getDefault()).format(new Date(timestamp));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Stat stat = (Stat) o;

      if (Double.compare(stat.value, value) != 0) return false;
      if (Double.compare(stat.timestamp, timestamp) != 0) return false;
      return name != null ? name.equals(stat.name) : stat.name == null;
    }

    @Override
    public int hashCode() {
      int result;
      long temp;
      result = name != null ? name.hashCode() : 0;
      temp = Double.doubleToLongBits(value);
      result = 31 * result + (int) (temp ^ (temp >>> 32));
      temp = Double.doubleToLongBits(timestamp);
      result = 31 * result + (int) (temp ^ (temp >>> 32));
      return result;
    }


    @Override
    public String toString() {
      return "Stat{" +
              "name='" + name + '\'' +
              ", value=" + value +
              ", timestamp=" + timestamp +
              '}';
    }

    public String name() {
      return name;
    }

    public double getValue() {
      return value;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public Stat aggregate(double value) {
      this.value += value;
      return this;
    }
  }

  abstract static class SensorMetric<P> {
    private final Sensor sensor;
    private final KafkaMetric metric;
    private Time time;
    private long lastEvent = 0;

    SensorMetric(Sensor sensor, KafkaMetric metric, Time time) {
      this.sensor = sensor;
      this.metric = metric;
      this.time = time;
    }

    /**
     * Anon class must call down to this for timestamp recording
     * @param object
     */
    void record(P object) {
      this.lastEvent = time.milliseconds();
    }

    public double value() {
      return metric.measurable().measure(metric.config(), time.milliseconds());
    }

    public void close(Metrics metrics) {
      metrics.removeSensor(sensor.name());
      metrics.removeMetric(metric.metricName());
    }

    @Override
    public String toString() {
      return super.toString() + " " + asStat().toString();
    }

    public Stat asStat() {
      return new Stat(metric.metricName().name(), value(), lastEvent);
    }
  }
}
