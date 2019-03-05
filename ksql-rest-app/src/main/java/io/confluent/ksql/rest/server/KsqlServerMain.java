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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import io.confluent.ksql.properties.PropertiesUtil;
import io.confluent.ksql.version.metrics.KsqlVersionCheckerAgent;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

      final Map<String, String> properties = PropertiesUtil.applyOverrides(
          PropertiesUtil.loadProperties(serverOptions.getPropertiesFile()),
          System.getProperties()
      );

      final String installDir = properties.getOrDefault("ksql.server.install.dir", "");
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
      final Map<String, String> properties,
      final Optional<String> queriesFile,
      final String installDir
  ) {
    if (queriesFile.isPresent()) {
      return StandaloneExecutorFactory.create(properties, queriesFile.get(), installDir);
    }

    final KsqlRestConfig restConfig = new KsqlRestConfig(ensureValidProps(properties));
    return KsqlRestApplication.buildApplication(
        restConfig,
        KsqlVersionCheckerAgent::new,
        Integer.MAX_VALUE
    );
  }

  private static Map<?, ?> ensureValidProps(final Map<String, String> properties) {
    if (properties.containsKey(StreamsConfig.APPLICATION_ID_CONFIG)) {
      return properties;
    }

    final Builder<String, String> builder = ImmutableMap.builder();
    builder.putAll(properties);
    builder.put(StreamsConfig.APPLICATION_ID_CONFIG, KSQL_REST_SERVER_DEFAULT_APP_ID);
    return builder.build();
  }
}
