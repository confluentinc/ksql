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

import org.apache.avro.generic.GenericContainer;

import io.confluent.ksql.version.metrics.KsqlVersionMetrics;
import io.confluent.support.metrics.common.Collector;
import io.confluent.support.metrics.common.Version;
import io.confluent.support.metrics.common.time.TimeUtils;

public class BasicCollector extends Collector {

  private final TimeUtils timeUtils;
  private final KsqlModuleType moduleType;

  public BasicCollector(KsqlModuleType moduleType, TimeUtils timeUtils) {
    this.timeUtils = timeUtils;
    this.moduleType = moduleType;
  }

  @Override
  public GenericContainer collectMetrics() {
    KsqlVersionMetrics metricsRecord = new KsqlVersionMetrics();
    metricsRecord.setTimestamp(timeUtils.nowInUnixTime());
    metricsRecord.setConfluentPlatformVersion(Version.getVersion());
    metricsRecord.setKsqlComponentType(moduleType.name());
    return metricsRecord;
  }
}
