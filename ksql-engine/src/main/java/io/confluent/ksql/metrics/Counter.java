package io.confluent.ksql.metrics;

import io.confluent.common.metrics.KafkaMetric;
import io.confluent.common.metrics.Metrics;
import io.confluent.common.metrics.Sensor;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class Counter<R> {

  private final String id;
  private String topic;
  private final Integer partition;
  // topic+partition level sensors - rate, events per sec, bandwidth etc
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
    sensors.values().stream().forEach(v ->  v.close(metrics));
  }

  public boolean isTopic(String topic) {
    return this.topic.equals(topic);
  }

  public String statsAsString() {
    return  "partition:" + partition + " " + sensors.values().stream().map(sensor -> sensor.toString()).collect(Collectors.joining(", "));
  }

  abstract static class SensorMetric<P> {
    private final Sensor sensor;
    private final KafkaMetric metric;

    SensorMetric(Sensor sensor, KafkaMetric metric) {
      this.sensor = sensor;
      this.metric = metric;
    }
    void record(P object) {
    }

    public void close(Metrics metrics) {
      // TODO: not yet supported in commons-metrics
      // metrics.removeSensor(sensor.name());
    }
    public String toString() {
      return metric.metricName().name() + ":" + metric.value();
    }
  }
}
