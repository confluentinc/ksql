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

package io.confluent.ksql.rest.server;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.ServiceInfo;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.function.MutableFunctionRegistry;
import io.confluent.ksql.function.UserFunctionLoader;
import io.confluent.ksql.logging.processing.ProcessingLogConfig;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.query.id.SequentialQueryIdGenerator;
import io.confluent.ksql.rest.server.computation.ConfigStore;
import io.confluent.ksql.rest.server.computation.KafkaConfigStore;
import io.confluent.ksql.rest.util.KsqlInternalTopicUtils;
import io.confluent.ksql.rest.util.RocksDBConfigSetterHandler;
import io.confluent.ksql.services.DisabledKsqlClient;
import io.confluent.ksql.services.KafkaClusterUtil;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.ServiceContextFactory;
import io.confluent.ksql.statement.Injector;
import io.confluent.ksql.statement.Injectors;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.ReservedInternalTopics;
import io.confluent.ksql.version.metrics.KsqlVersionCheckerAgent;
import io.confluent.ksql.version.metrics.VersionCheckerAgent;
import java.util.Collections;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public final class StandaloneExecutorFactory {

  private StandaloneExecutorFactory() {
  }

  public static StandaloneExecutor create(
      final Map<String, String> properties,
      final String queriesFile,
      final String installDir,
      final MetricCollectors metricCollectors
  ) {
    final KsqlConfig tempConfig = new KsqlConfig(properties);
    
    final Function<KsqlConfig, ServiceContext> serviceContextFactory = 
        config -> ServiceContextFactory.create(config, DisabledKsqlClient::instance);
    final ServiceContext tempServiceContext =
        serviceContextFactory.apply(tempConfig);

    final String kafkaClusterId = KafkaClusterUtil.getKafkaClusterId(tempServiceContext);
    final String ksqlServerId = tempConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG);
    final Map<String, Object> updatedProperties = tempConfig.originals();
    updatedProperties.putAll(
        metricCollectors.addConfluentMetricsContextConfigs(ksqlServerId, kafkaClusterId));

    final Consumer<KsqlConfig> rocksDBConfigSetterHandler =
        RocksDBConfigSetterHandler::maybeConfigureRocksDBConfigSetter;

    return create(
        updatedProperties,
        queriesFile,
        installDir,
        serviceContextFactory,
        KafkaConfigStore::new,
        KsqlVersionCheckerAgent::new,
        StandaloneExecutor::new,
        metricCollectors,
        rocksDBConfigSetterHandler
    );
  }

  interface StandaloneExecutorConstructor {

    @SuppressWarnings({"checkstyle:ParameterNumber"})
    StandaloneExecutor create(
        ServiceContext serviceContext,
        ProcessingLogConfig processingLogConfig,
        KsqlConfig ksqlConfig,
        KsqlEngine ksqlEngine,
        String queriesFile,
        UserFunctionLoader udfLoader,
        boolean failOnNoQueries,
        VersionCheckerAgent versionChecker,
        BiFunction<KsqlExecutionContext, ServiceContext, Injector> injectorFactory,
        MetricCollectors metricCollectors,
        Consumer<KsqlConfig> rocksDBConfigSetterHandler
    );
  }

  @VisibleForTesting
  static StandaloneExecutor create(
      final Map<String, Object> properties,
      final String queriesFile,
      final String installDir,
      final Function<KsqlConfig, ServiceContext> serviceContextFactory,
      final BiFunction<String, KsqlConfig, ConfigStore> configStoreFactory,
      final Function<Supplier<Boolean>, VersionCheckerAgent> versionCheckerFactory,
      final StandaloneExecutorConstructor constructor,
      final MetricCollectors metricCollectors,
      final Consumer<KsqlConfig> rocksDBConfigSetterHandler
  ) {
    final KsqlConfig baseConfig = new KsqlConfig(properties);

    final ServiceContext serviceContext = serviceContextFactory.apply(baseConfig);

    final String configTopicName = ReservedInternalTopics.configsTopic(baseConfig);
    KsqlInternalTopicUtils.ensureTopic(
        configTopicName,
        baseConfig,
        serviceContext.getTopicClient()
    );
    final ConfigStore configStore = configStoreFactory.apply(configTopicName, baseConfig);
    final KsqlConfig ksqlConfig = configStore.getKsqlConfig();

    final ProcessingLogConfig processingLogConfig
        = new ProcessingLogConfig(properties);
    final ProcessingLogContext processingLogContext
        = ProcessingLogContext.create(
            processingLogConfig,
        metricCollectors.getMetrics(),
        ksqlConfig.getStringAsMap(KsqlConfig.KSQL_CUSTOM_METRICS_TAGS)
    );

    final MutableFunctionRegistry functionRegistry = new InternalFunctionRegistry();

    final KsqlEngine ksqlEngine = new KsqlEngine(
        serviceContext,
        processingLogContext,
        functionRegistry,
        ServiceInfo.create(ksqlConfig),
        new SequentialQueryIdGenerator(),
        ksqlConfig,
        Collections.emptyList(),
        metricCollectors
        );

    final UserFunctionLoader udfLoader =
        UserFunctionLoader.newInstance(ksqlConfig, functionRegistry, installDir,
            metricCollectors.getMetrics());

    final VersionCheckerAgent versionChecker = versionCheckerFactory
        .apply(ksqlEngine::hasActiveQueries);

    return constructor.create(
        serviceContext,
        processingLogConfig,
        ksqlConfig,
        ksqlEngine,
        queriesFile,
        udfLoader,
        true,
        versionChecker,
        Injectors.NO_TOPIC_DELETE,
        metricCollectors,
        rocksDBConfigSetterHandler
    );
  }
}
