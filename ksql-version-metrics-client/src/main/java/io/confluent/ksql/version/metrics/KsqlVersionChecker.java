/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.confluent.ksql.version.metrics;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import io.confluent.ksql.version.metrics.collector.BasicCollector;
import io.confluent.ksql.version.metrics.collector.KsqlModuleType;
import io.confluent.support.metrics.BaseMetricsReporter;
import io.confluent.support.metrics.BaseSupportConfig;
import io.confluent.support.metrics.common.Collector;
import io.confluent.support.metrics.common.kafka.ZkClientProvider;
import io.confluent.support.metrics.common.time.TimeUtils;

public class KsqlVersionChecker extends BaseMetricsReporter {

  private final Collector metricsCollector;

  private AtomicBoolean shuttingDown = new AtomicBoolean(false);

  public KsqlVersionChecker(
      BaseSupportConfig ksqlVersionCheckerConfig,
      Runtime serverRuntime,
      KsqlModuleType moduleType,
      boolean enableSettlingTime
  ) {
    super(
        ksqlVersionCheckerConfig,
        null,
        new KsqlVersionCheckerResponseHandler(),
        enableSettlingTime
    );
    Objects.requireNonNull(serverRuntime, "serverRuntime is required");
    serverRuntime.addShutdownHook(new Thread(() -> shuttingDown.set(true)));
    this.metricsCollector = new BasicCollector(moduleType, new TimeUtils());
  }

  @Override
  protected ZkClientProvider zkClientProvider() {
    //This is used when collecting metrics in a kafka topic. Since KSQL isn't aware of ZK, we are
    // returning null here and also turning off topic metrics collection in
    // KsqlVersionCheckerConfig.
    return null;
  }

  @Override
  protected Collector metricsCollector() {
    return metricsCollector;
  }

  @Override
  protected boolean isReadyForMetricsCollection() {
    return true;
  }

  @Override
  protected boolean isShuttingDown() {
    return shuttingDown.get();
  }

}
