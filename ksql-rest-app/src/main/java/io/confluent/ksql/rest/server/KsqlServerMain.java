/*
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

import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import io.confluent.ksql.version.metrics.KsqlVersionCheckerAgent;

public class KsqlServerMain {

  private static final Logger log = LoggerFactory.getLogger(KsqlServerMain.class);
  private static final String KSQL_REST_SERVER_DEFAULT_APP_ID = "KSQL_REST_SERVER_DEFAULT_APP_ID";

  private final Executable executable;

  public static void main(final String[] args) {
    try {
      final ServerOptions serverOptions = ServerOptions.parse(args);
      if (serverOptions == null) {
        return;
      }

      final Properties properties = serverOptions.loadProperties(System::getProperties);
      final String installDir = properties.getProperty("ksql.server.install.dir");
      final Optional<String> queriesFile = serverOptions.getQueriesFile(properties);
      final Executable executable = createExecutable(properties, queriesFile, installDir);
      new KsqlServerMain(executable).tryStartApp();
    } catch (final Exception e) {
      log.error("Failed to start KSQL", e);
      System.exit(-1);
    }
  }

  KsqlServerMain(final Executable executable) {
    this.executable = Objects.requireNonNull(executable, "executable");
  }

  void tryStartApp() throws Exception {
    try {
      log.info("Starting server");
      executable.start();
      log.info("Server up and running");
      executable.join();
    } finally {
      log.info("Server shutting down");
      executable.stop();
    }
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private static Executable createExecutable(
      final Properties properties,
      final Optional<String> queriesFile,
      final String installDir
  ) throws Exception {
    if (queriesFile.isPresent()) {
      return StandaloneExecutor.create(properties, queriesFile.get(), installDir);
    }

    if (!properties.containsKey(StreamsConfig.APPLICATION_ID_CONFIG)) {
      properties.put(StreamsConfig.APPLICATION_ID_CONFIG, KSQL_REST_SERVER_DEFAULT_APP_ID);
    }
    final KsqlRestConfig restConfig = new KsqlRestConfig(properties);
    return KsqlRestApplication.buildApplication(
        restConfig,
        new KsqlVersionCheckerAgent()
    );
  }
}
