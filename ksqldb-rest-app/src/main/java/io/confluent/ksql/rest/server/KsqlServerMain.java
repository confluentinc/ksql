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
import io.confluent.ksql.properties.PropertiesUtil;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlServerException;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KsqlServerMain {

  private static final Logger log = LoggerFactory.getLogger(KsqlServerMain.class);

  private final Executor shutdownHandler;
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
      final KsqlConfig ksqlConfig = new KsqlConfig(properties);
      validateConfig(ksqlConfig);

      final Optional<String> queriesFile = serverOptions.getQueriesFile(properties);
      final Executable executable = createExecutable(
          properties, queriesFile, installDir, ksqlConfig);
      new KsqlServerMain(
          executable,
          r -> Runtime.getRuntime().addShutdownHook(new Thread(r))
      ).tryStartApp();
    } catch (final Exception e) {
      log.error("Failed to start KSQL", e);
      System.exit(-1);
    }
  }

  KsqlServerMain(final Executable executable, final Executor shutdownHandler) {
    this.executable = Objects.requireNonNull(executable, "executable");
    this.shutdownHandler = Objects.requireNonNull(shutdownHandler, "shutdownHandler");
  }

  void tryStartApp() throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);
    shutdownHandler.execute(() -> {
      executable.notifyTerminated();
      try {
        latch.await();
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
    try {
      try {
        log.info("Starting server");
        executable.startAsync();
        log.info("Server up and running");
        executable.awaitTerminated();
      } catch (Throwable t) {
        log.error("Unhandled exception in server startup", t);
        throw t;
      } finally {
        log.info("Server shutting down");
        executable.shutdown();
      }
    } finally {
      latch.countDown();
    }
  }

  private static void validateConfig(final KsqlConfig config) {
    validateStateDir(config);
    validateDefaultTopicFormats(config);
  }

  private static void validateStateDir(final KsqlConfig config) {
    final String streamsStateDirPath = config.getKsqlStreamConfigProps().getOrDefault(
        StreamsConfig.STATE_DIR_CONFIG,
        StreamsConfig.configDef().defaultValues().get(StreamsConfig.STATE_DIR_CONFIG)).toString();
    enforceStreamStateDirAvailability(new File(streamsStateDirPath));
  }

  @VisibleForTesting
  static void validateDefaultTopicFormats(final KsqlConfig config) {
    validateTopicFormat(config, KsqlConfig.KSQL_DEFAULT_KEY_FORMAT_CONFIG, "key");
    validateTopicFormat(config, KsqlConfig.KSQL_DEFAULT_VALUE_FORMAT_CONFIG, "value");
  }

  private static void validateTopicFormat(
      final KsqlConfig config,
      final String configName,
      final String type
  ) {
    final String formatName = config.getString(configName);
    if (formatName == null) {
      return;
    }

    try {
      FormatFactory.fromName(formatName);
    } catch (KsqlException e) {
      throw new KsqlException("Invalid value for config '" + configName + "': " + formatName, e);
    }
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private static Executable createExecutable(
      final Map<String, String> properties,
      final Optional<String> queriesFile,
      final String installDir,
      final KsqlConfig ksqlConfig
  ) throws IOException {
    if (queriesFile.isPresent()) {
      return StandaloneExecutorFactory.create(properties, queriesFile.get(), installDir);
    }

    final KsqlRestConfig restConfig = new KsqlRestConfig(properties);
    final Executable restApp = KsqlRestApplication
        .buildApplication(restConfig);

    final String connectConfigFile =
        ksqlConfig.getString(KsqlConfig.CONNECT_WORKER_CONFIG_FILE_PROPERTY);
    if (connectConfigFile.isEmpty()) {
      return restApp;
    }

    final Executable connect = ConnectExecutable.of(connectConfigFile);
    return MultiExecutable.of(connect, restApp);
  }

  @VisibleForTesting
  static void enforceStreamStateDirAvailability(final File streamsStateDir) {
    if (!streamsStateDir.exists()) {
      final boolean mkDirSuccess = streamsStateDir.mkdirs();
      if (!mkDirSuccess) {
        throw new KsqlServerException("Could not create the kafka streams state directory: "
            + streamsStateDir.getPath()
            + "\n Make sure the directory exists and is writable for KSQL server "
            + "\n or its parent directory is writable by KSQL server"
            + "\n or change it to a writable directory by setting '"
            + KsqlConfig.KSQL_STREAMS_PREFIX + StreamsConfig.STATE_DIR_CONFIG
            + "' config in the properties file."
        );
      }
    }
    if (!streamsStateDir.isDirectory()) {
      throw new KsqlServerException(streamsStateDir.getPath()
          + " is not a directory."
          + "\n Make sure the directory exists and is writable for KSQL server "
          + "\n or its parent directory is writable by KSQL server"
          + "\n or change it to a writable directory by setting '"
          + KsqlConfig.KSQL_STREAMS_PREFIX + StreamsConfig.STATE_DIR_CONFIG
          + "' config in the properties file."
      );
    }
    if (!streamsStateDir.canWrite() || !streamsStateDir.canExecute()) {
      throw new KsqlServerException("The kafka streams state directory is not writable "
          + "for KSQL server: "
          + streamsStateDir.getPath()
          + "\n Make sure the directory exists and is writable for KSQL server "
          + "\n or change it to a writable directory by setting '"
          + KsqlConfig.KSQL_STREAMS_PREFIX + StreamsConfig.STATE_DIR_CONFIG
          + "' config in the properties file."
      );
    }
  }
}
