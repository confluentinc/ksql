/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.version.metrics.collector;

import io.confluent.ksql.util.Version;
import io.confluent.ksql.version.metrics.KsqlVersionMetrics;
import io.confluent.support.metrics.common.Collector;
import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class BasicCollector extends Collector {

  private final KsqlModuleType moduleType;
  private final Supplier<Boolean> activenessSupplier;
  private final Clock clock;

  public BasicCollector(
      final KsqlModuleType moduleType,
      final Supplier<Boolean> activenessStatusSupplier
  ) {
    this(moduleType, activenessStatusSupplier, Clock.systemDefaultZone());
  }

  BasicCollector(
      final KsqlModuleType moduleType,
      final Supplier<Boolean> activenessSupplier,
      final Clock clock
  ) {
    this.moduleType = moduleType;
    this.activenessSupplier = Objects.requireNonNull(activenessSupplier, "activenessSupplier");
    this.clock = Objects.requireNonNull(clock, "clock");
  }

  @Override
  public KsqlVersionMetrics collectMetrics() {
    final KsqlVersionMetrics metricsRecord = new KsqlVersionMetrics();
    metricsRecord.setTimestamp(TimeUnit.MILLISECONDS.toSeconds(clock.millis()));
    metricsRecord.setConfluentPlatformVersion(Version.getVersion());
    metricsRecord.setKsqlComponentType(moduleType.name());
    metricsRecord.setIsActive(activenessSupplier.get());
    return metricsRecord;
  }
}
