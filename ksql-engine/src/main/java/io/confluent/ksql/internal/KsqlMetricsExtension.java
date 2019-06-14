/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.internal;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.metrics.MeasurableStat;

/**
 * This interface provides a way for users to provide custom metrics that will be emitted alongside
 * the default KSQL engine JMX metrics.
 */
public interface KsqlMetricsExtension extends Configurable {

  @Override
  void configure(Map<String, ?> config);

  /**
   * Returns custom metrics to be emitted alongside the default KSQL engine JMX metrics.
   *
   * @return list of metrics
   */
  List<Metric> getCustomMetrics();

  class Metric {
    private String name;
    private String description;
    private Supplier<MeasurableStat> statSupplier;

    public Metric(
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
}
