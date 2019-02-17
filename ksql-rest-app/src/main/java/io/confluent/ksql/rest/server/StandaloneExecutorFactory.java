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

package io.confluent.ksql.rest.server;

import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.function.MutableFunctionRegistry;
import io.confluent.ksql.function.UdfLoader;
import io.confluent.ksql.processing.log.ProcessingLogConfig;
import io.confluent.ksql.processing.log.ProcessingLogContext;
import io.confluent.ksql.rest.server.computation.ConfigStore;
import io.confluent.ksql.rest.server.computation.KafkaConfigStore;
import io.confluent.ksql.rest.util.KsqlInternalTopicUtils;
import io.confluent.ksql.services.DefaultServiceContext;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.version.metrics.KsqlVersionCheckerAgent;
import io.confluent.ksql.version.metrics.VersionCheckerAgent;
import java.util.Properties;
import java.util.function.BiFunction;
import java.util.function.Function;

public final class StandaloneExecutorFactory {
  static final String CONFIG_TOPIC_SUFFIX = "configs";

  private StandaloneExecutorFactory(){
  }

  public static StandaloneExecutor create(
      final Properties properties,
      final String queriesFile,
      final String installDir
  ) {
    return create(
        properties,
        queriesFile,
        installDir,
        DefaultServiceContext::create,
        KafkaConfigStore::new,
        StandaloneExecutor::new
    );
  }

  interface StandaloneExecutorConstructor {
    StandaloneExecutor create(
        ServiceContext serviceContext,
        ProcessingLogConfig processingLogConfig,
        KsqlConfig ksqlConfig,
        KsqlEngine ksqlEngine,
        String queriesFile,
        UdfLoader udfLoader,
        boolean failOnNoQueries,
        VersionCheckerAgent versionCheckerAgent
    );
  }

  static StandaloneExecutor create(
      final Properties properties,
      final String queriesFile,
      final String installDir,
      final Function<KsqlConfig, ServiceContext> serviceContextFactory,
      final BiFunction<String, KsqlConfig, ConfigStore> configStoreFactory,
      final StandaloneExecutorConstructor constructor
  ) {
    final KsqlConfig baseConfig = new KsqlConfig(properties);

    final ServiceContext serviceContext = serviceContextFactory.apply(baseConfig);

    final String configTopicName
        = KsqlInternalTopicUtils.getTopicName(baseConfig, CONFIG_TOPIC_SUFFIX);
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
        = ProcessingLogContext.create(processingLogConfig);

    final MutableFunctionRegistry functionRegistry = new InternalFunctionRegistry();

    final KsqlEngine ksqlEngine = new KsqlEngine(
        serviceContext,
        processingLogContext,
        functionRegistry,
        ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG));

    final UdfLoader udfLoader =
        UdfLoader.newInstance(ksqlConfig, functionRegistry, installDir);

    return constructor.create(
        serviceContext,
        processingLogConfig,
        ksqlConfig,
        ksqlEngine,
        queriesFile,
        udfLoader,
        true,
        new KsqlVersionCheckerAgent(ksqlEngine::hasActiveQueries)
    );
  }
}
