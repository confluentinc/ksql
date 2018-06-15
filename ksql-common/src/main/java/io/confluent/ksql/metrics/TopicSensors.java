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

import com.google.common.base.MoreObjects;

import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Rate;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.common.utils.Time;

class TopicSensors<R> {

  private final String topic;
  private final List<SensorMetric<R>> sensors;

  TopicSensors(String topic, final List<SensorMetric<R>> sensors) {
    this.topic = topic.toLowerCase();
    this.sensors = sensors;
  }

  void increment(R record, boolean isError) {
    sensors.forEach((SensorMetric<R> v) -> {
      if (v.isError() == isError) {
        v.record(record);
      }
    });
  }

  public void close(Metrics metrics) {
    sensors.forEach(v -> v.close(metrics));
  }

  boolean isTopic(String topic) {
    return this.topic.equals(topic);
  }

  Collection<Stat> stats(boolean isError) {
    return sensors
        .stream()
        .filter(sensor -> sensor.errorMetric == isError)
        .map(sensor -> sensor.asStat())
        .collect(Collectors.toList());
  }

  Collection<Stat> errorRateStats() {
    return sensors.stream()
        .filter(sensor -> sensor.isError() && sensor.isRate())
        .map(SensorMetric::asStat)
        .collect(Collectors.toList());
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

    @SuppressFBWarnings("FE_FLOATING_POINT_EQUALITY")
    public String formatted() {
      if (value == Math.round(value)) {
        return String.format("%16s:%10.0f", name, value);
      } else {
        return String.format("%16s:%10.2f", name, value);
      }
    }

    public String timestamp() {
      if (timestamp == 0) {
        return "n/a";
      }
      return SimpleDateFormat.getDateTimeInstance(
          3,
          1,
          Locale.getDefault()
      ).format(new Date(timestamp)
      );
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Stat stat = (Stat) o;

      if (Double.compare(stat.value, value) != 0) {
        return false;
      }
      if (Double.compare(stat.timestamp, timestamp) != 0) {
        return false;
      }
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
      return MoreObjects
          .toStringHelper(this)
          .add("name", name)
          .add("value", value)
          .add("timestamp", timestamp)
          .toString();
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

  static class SensorMetric<P> {

    private final Sensor sensor;
    private final KafkaMetric metric;
    private Time time;
    private boolean errorMetric;
    private long lastEvent = 0;

    SensorMetric(Sensor sensor, KafkaMetric metric, Time time, boolean errorMetric) {
      this.sensor = sensor;
      this.metric = metric;
      this.time = time;
      this.errorMetric = errorMetric;
    }

    public boolean isError() {
      return errorMetric;
    }

    /**
     * Anon class must call down to this for timestamp recording
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

    public boolean isRate() {
      return metric.measurable() instanceof Rate;
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
