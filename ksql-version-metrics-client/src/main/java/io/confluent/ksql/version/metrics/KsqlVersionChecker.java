/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.version.metrics;

import io.confluent.ksql.version.metrics.collector.BasicCollector;
import io.confluent.ksql.version.metrics.collector.KsqlModuleType;
import io.confluent.support.metrics.BaseMetricsReporter;
import io.confluent.support.metrics.BaseSupportConfig;
import io.confluent.support.metrics.common.Collector;
import io.confluent.support.metrics.common.kafka.KafkaUtilities;
import io.confluent.support.metrics.common.kafka.ZkClientProvider;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public class KsqlVersionChecker extends BaseMetricsReporter {

  private final Collector metricsCollector;

  private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

  public KsqlVersionChecker(
      final BaseSupportConfig ksqlVersionCheckerConfig,
      final KsqlModuleType moduleType,
      final boolean enableSettlingTime,
      final Supplier<Boolean> activenessStatusSupplier
  ) {
    super(
        "KsqlVersionCheckerAgent",
        true,
        ksqlVersionCheckerConfig,
        new KafkaUtilities(),
        new KsqlVersionCheckerResponseHandler(),
        enableSettlingTime
    );
    final Runtime serverRuntime = Runtime.getRuntime();
    Objects.requireNonNull(serverRuntime, "serverRuntime is required");
    serverRuntime.addShutdownHook(new Thread(() -> shuttingDown.set(true)));
    this.metricsCollector = new BasicCollector(moduleType, activenessStatusSupplier);
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
