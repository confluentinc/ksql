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

import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.function.UdfLoader;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.Version;
import io.confluent.ksql.util.WelcomeMsgUtils;
import java.io.BufferedReader;
import java.io.Console;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandaloneExecutor implements Executable {

  private static final Logger log = LoggerFactory.getLogger(StandaloneExecutor.class);

  private final KsqlConfig ksqlConfig;
  private final KsqlEngine ksqlEngine;
  private final String queriesFile;
  private final UdfLoader udfLoader;
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);

  StandaloneExecutor(final KsqlConfig ksqlConfig,
                     final KsqlEngine ksqlEngine,
                     final String queriesFile,
                     final UdfLoader udfLoader) {
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig can't be null");
    this.ksqlEngine = Objects.requireNonNull(ksqlEngine, "ksqlEngine can't be null");
    this.queriesFile = Objects.requireNonNull(queriesFile, "queriesFile can't be null");
    this.udfLoader = Objects.requireNonNull(udfLoader, "udfLoader can't be null");
  }

  public void start() {
    try {
      udfLoader.load();
      executeStatements(readQueriesFile(queriesFile));
      showWelcomeMessage();
    } catch (final Exception e) {
      log.error("Failed to start KSQL Server with query file: " + queriesFile, e);
      stop();
      throw e;
    }
  }

  public void stop() {
    try {
      ksqlEngine.close();
    } catch (final Exception e) {
      log.warn("Failed to cleanly shutdown the KSQL Engine", e);
    }
    shutdownLatch.countDown();
  }

  @Override
  public void join() throws InterruptedException {
    shutdownLatch.await();
  }

  public static StandaloneExecutor create(final Properties properties,
                                          final String queriesFile,
                                          final String installDir) {
    final KsqlConfig ksqlConfig = new KsqlConfig(properties);
    final KsqlEngine ksqlEngine = new KsqlEngine(ksqlConfig);
    final UdfLoader udfLoader = UdfLoader.newInstance(ksqlConfig,
        ksqlEngine.getMetaStore(),
        installDir);
    return new StandaloneExecutor(ksqlConfig, ksqlEngine, queriesFile, udfLoader);
  }

  private void showWelcomeMessage() {
    final Console console = System.console();
    if (console == null) {
      return;
    }
    final PrintWriter writer =
        new PrintWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8));

    WelcomeMsgUtils.displayWelcomeMessage(80, writer);
    writer.printf("Server %s started with query file %s. Interactive mode is disabled.%n",
                  Version.getVersion(),
                  queriesFile);

    writer.flush();
  }

  private void executeStatements(final String queries) {
    final List<QueryMetadata> queryMetadataList = ksqlEngine.createQueries(queries, ksqlConfig);
    for (final QueryMetadata queryMetadata : queryMetadataList) {
      if (queryMetadata instanceof PersistentQueryMetadata) {
        final PersistentQueryMetadata persistentQueryMd = (PersistentQueryMetadata) queryMetadata;
        persistentQueryMd.start();
      } else {
        final String message = String.format(
            "Ignoring statements: %s"
            + "%nOnly CREATE statements can run in standalone mode.",
            queryMetadata.getStatementString()
        );
        System.err.println(message);
        log.warn(message);
      }
    }
  }

  private static String readQueriesFile(final String queryFilePath) {
    final StringBuilder sb = new StringBuilder();
    try (BufferedReader br = new BufferedReader(new InputStreamReader(
        new FileInputStream(queryFilePath), StandardCharsets.UTF_8))) {
      String line = br.readLine();
      while (line != null) {
        sb.append(line);
        sb.append(System.lineSeparator());
        line = br.readLine();
      }
    } catch (final IOException e) {
      throw new KsqlException("Could not read the query file. Details: " + e.getMessage(), e);
    }
    return sb.toString();
  }
}
