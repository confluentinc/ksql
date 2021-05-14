/*
 * Copyright 2021 Confluent Inc.
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

import java.util.Objects;
import java.util.function.Supplier;
import org.apache.kafka.common.metrics.MeasurableStat;

public final class KsqlMetric {
  private final String name;
  private final String description;
  private final Supplier<MeasurableStat> statSupplier;

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