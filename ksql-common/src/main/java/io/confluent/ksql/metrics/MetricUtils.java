package io.confluent.ksql.metrics;

import io.confluent.ksql.metrics.TopicSensors.Stat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class MetricUtils {
  public static <T> double aggregateStat(
      final String name,
      final boolean isError,
      final Collection<TopicSensors<T>> sensors) {
    final List<Stat> allStats = new ArrayList<>();
    sensors.forEach(record -> allStats.addAll(record.stats(isError)));
    return allStats
        .stream()
        .filter(stat -> stat.name().equals(name))
        .mapToDouble(TopicSensors.Stat::getValue)
        .sum();
  }

  public static <T> Collection<TopicSensors.Stat> stats(
      final String topic,
      final boolean isError,
      final Collection<TopicSensors<T>> sensors) {
    final List<TopicSensors.Stat> list = new ArrayList<>();
    sensors.stream()
        .filter(counter -> counter.isTopic(topic))
        .forEach(record -> list.addAll(record.stats(isError)));
    return list;
  }
}
