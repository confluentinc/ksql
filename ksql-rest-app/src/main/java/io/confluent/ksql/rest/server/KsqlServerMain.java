/**
 * Copyright 2018 Confluent Inc.
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
 **/

package io.confluent.ksql.rest.server;

import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Properties;
import io.confluent.ksql.version.metrics.KsqlVersionCheckerAgent;

public class KsqlServerMain {

  private static final Logger log = LoggerFactory.getLogger(KsqlServerMain.class);
  private static final String KSQL_REST_SERVER_DEFAULT_APP_ID = "KSQL_REST_SERVER_DEFAULT_APP_ID";

  public static void main(final String[] args) throws Exception {
    final ServerOptions serverOptions = ServerOptions.parse(args);
    if (serverOptions == null) {
      return;
    }

    final Properties properties = serverOptions.loadProperties(System::getProperties);
    final Optional<String> queriesFile = serverOptions.getQueriesFile(properties);
    final Executable executable = createExecutable(properties, queriesFile);
    log.info("Starting server");
    executable.start();
    log.info("Server up and running");
    executable.join();
    log.info("Server shutting down");
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private static Executable createExecutable(
      final Properties properties,
      final Optional<String> queriesFile
  ) throws Exception {
    if (queriesFile.isPresent()) {
      return StandaloneExecutor.create(properties, queriesFile.get());
    }

    if (!properties.containsKey(StreamsConfig.APPLICATION_ID_CONFIG)) {
      properties.put(StreamsConfig.APPLICATION_ID_CONFIG, KSQL_REST_SERVER_DEFAULT_APP_ID);
    }
    final KsqlRestConfig restConfig = new KsqlRestConfig(properties);
    return KsqlRestApplication.buildApplication(
        restConfig,
        restConfig.isUiEnabled(),
        new KsqlVersionCheckerAgent()
    );
  }
}
