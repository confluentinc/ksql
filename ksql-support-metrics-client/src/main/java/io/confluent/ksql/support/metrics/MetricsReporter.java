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
package io.confluent.ksql.support.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

import io.confluent.ksql.support.metrics.collector.BasicCollector;
import io.confluent.ksql.support.metrics.collector.KsqlModuleType;
import io.confluent.support.metrics.BaseMetricsReporter;
import io.confluent.support.metrics.common.Collector;
import io.confluent.support.metrics.common.kafka.KafkaUtilities;
import io.confluent.support.metrics.common.kafka.ZkUtilsProvider;


public class MetricsReporter extends BaseMetricsReporter {

  private static final Logger log = LoggerFactory.getLogger(MetricsReporter.class);


  private final Runtime serverRuntime;
  private final Collector metricsCollector;

  private AtomicBoolean shuttingDown = new AtomicBoolean(false);

  public MetricsReporter(KsqlSupportConfig ksqlSupportConfig,
                         Runtime serverRuntime, KsqlModuleType moduleType) {

    super(ksqlSupportConfig, new KafkaUtilities());
    if (ksqlSupportConfig == null || serverRuntime == null) {
      throw new IllegalArgumentException("some arguments are null");
    }
    this.serverRuntime = serverRuntime;
    this.serverRuntime.addShutdownHook(new Thread(() -> shuttingDown.set(true)));
    this.metricsCollector = new BasicCollector(moduleType, ksqlSupportConfig);
  }


  @Override
  protected ZkUtilsProvider zkUtilsProvider() {
    //This is used when collecting metrics in a kafka topic. Since KSQL isn't aware of ZK, we are
    // returning null here and also turning off topic metrics collection in KsqlSupportConfig.
    return () -> null;
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