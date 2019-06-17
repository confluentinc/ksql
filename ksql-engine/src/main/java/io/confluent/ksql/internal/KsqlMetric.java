package io.confluent.ksql.internal;

import java.util.Objects;
import java.util.function.Supplier;
import org.apache.kafka.common.metrics.MeasurableStat;

public class KsqlMetric {
  private String name;
  private String description;
  private Supplier<MeasurableStat> statSupplier;

  public static KsqlMetric of(
      final String name,
      final String description,
      final Supplier<MeasurableStat> statSupplier) {
    return new KsqlMetric(name, description, statSupplier);
  }

  private KsqlMetric(
      final String name,
      final String description,
      final Supplier<MeasurableStat> statSupplier) {
    this.name = Objects.requireNonNull(name, "name cannot be null");
    this.description = Objects.requireNonNull(description, "description cannot be null");
    this.statSupplier = Objects.requireNonNull(statSupplier, "statSupplier cannot be null");
  }

  public String name() {
    return name;
  }

  public String description() {
    return description;
  }

  public Supplier<MeasurableStat> statSupplier() {
    return statSupplier;
  }
}