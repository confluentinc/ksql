/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.version.metrics.collector;

import io.confluent.ksql.version.metrics.KsqlVersionMetrics;
import io.confluent.support.metrics.common.Collector;
import io.confluent.support.metrics.common.Version;
import io.confluent.support.metrics.common.time.TimeUtils;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.avro.generic.GenericContainer;

public class BasicCollector extends Collector {

  private final KsqlModuleType moduleType;
  private final Supplier<Boolean> activenessStatusSupplier;

  public BasicCollector(
      final KsqlModuleType moduleType,
      final TimeUtils timeUtils,
      final Supplier<Boolean> activenessStatusSupplier) {
    this.moduleType = moduleType;
    Objects.requireNonNull(activenessStatusSupplier);
    this.activenessStatusSupplier = activenessStatusSupplier;
  }

  @Override
  public GenericContainer collectMetrics() {
    final KsqlVersionMetrics metricsRecord = new KsqlVersionMetrics();
    metricsRecord.setTimestamp(System.currentTimeMillis());
    metricsRecord.setConfluentPlatformVersion(Version.getVersion());
    metricsRecord.setKsqlComponentType(moduleType.name());
    metricsRecord.setIsActive(activenessStatusSupplier.get());
    return metricsRecord;
  }

}
