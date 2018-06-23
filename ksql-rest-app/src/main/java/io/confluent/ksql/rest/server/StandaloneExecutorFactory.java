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
import io.confluent.ksql.rest.util.ProcessingLogConfig;
import io.confluent.ksql.services.DefaultServiceContext;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.version.metrics.KsqlVersionCheckerAgent;
import java.util.Properties;

public final class StandaloneExecutorFactory {
  private StandaloneExecutorFactory(){
  }

  public static StandaloneExecutor create(
      final Properties properties,
      final String queriesFile,
      final String installDir
  ) {
    final KsqlConfig baseConfig = new KsqlConfig(properties);

    final ServiceContext serviceContext = DefaultServiceContext.create(baseConfig);

    final ConfigStore configStore = new KafkaConfigStore(
        baseConfig,
        serviceContext.getTopicClient());
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

    return new StandaloneExecutor(
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
