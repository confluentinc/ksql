package io.confluent.ksql.metrics;

import java.util.Collection;
import java.util.stream.Collectors;

public final class MetricUtils {
  private MetricUtils() {
  }

  public static <T> double aggregateStat(
      final String name,
      final boolean isError,
      final Collection<TopicSensors<T>> sensors) {
    return sensors.stream()
        .flatMap(r -> r.stats(isError).stream())
        .filter(s -> s.name().equals(name))
        .mapToDouble(TopicSensors.Stat::getValue)
        .sum();
  }

  public static <T> Collection<TopicSensors.Stat> stats(
      final String topic,
      final boolean isError,
      final Collection<TopicSensors<T>> sensors) {
    return sensors.stream()
        .filter(counter -> counter.isTopic(topic))
        .flatMap(r -> r.stats(isError).stream())
        .collect(Collectors.toList());
  }
}
