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

package io.confluent.ksql.support.metrics.collector;

import org.apache.avro.generic.GenericContainer;

import io.confluent.ksql.support.metrics.KsqlBasicMetrics;
import io.confluent.ksql.support.metrics.KsqlSupportConfig;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.support.metrics.common.Collector;
import io.confluent.support.metrics.common.Uuid;
import io.confluent.support.metrics.common.Version;
import io.confluent.support.metrics.common.time.TimeUtils;

public class BasicCollector extends Collector {

  private final TimeUtils time;
  private final Uuid uuid;
  private final KsqlModuleType moduleType;
  private final String serviceId;

  public BasicCollector(
      KsqlModuleType moduleType,
      KsqlSupportConfig ksqlSupportConfig
  ) {
    uuid = new Uuid();
    time = new TimeUtils();
    this.moduleType = moduleType;
    this.serviceId = ksqlSupportConfig.getProperties()
        .getProperty(KsqlConfig.KSQL_SERVICE_ID_CONFIG);
  }

  @Override
  public GenericContainer collectMetrics() {
    KsqlBasicMetrics metricsRecord = new KsqlBasicMetrics();
    metricsRecord.setTimestamp(time.nowInUnixTime());
    metricsRecord.setConfluentPlatformVersion(Version.getVersion());
    metricsRecord.setCollectorState(this.getRuntimeState().stateId());
    metricsRecord.setKsqlComponentUUID(uuid.toString());
    metricsRecord.setKsqlComponentType(moduleType.name());
    metricsRecord.setServiceId(serviceId);
    return metricsRecord;
  }
}
