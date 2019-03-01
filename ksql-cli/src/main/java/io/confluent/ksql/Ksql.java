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

package io.confluent.ksql;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.cli.Cli;
import io.confluent.ksql.cli.Options;
import io.confluent.ksql.cli.console.OutputFormat;
import io.confluent.ksql.properties.PropertiesUtil;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.util.ErrorMessageUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.version.metrics.KsqlVersionCheckerAgent;
import io.confluent.ksql.version.metrics.collector.KsqlModuleType;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Ksql {
  private static final Logger LOGGER = LoggerFactory.getLogger(Ksql.class);

  private final Options options;
  private final KsqlClientBuilder clientBuilder;
  private final Properties systemProps;
  private final CliBuilder cliBuilder;

  @VisibleForTesting
  Ksql(
      final Options options,
      final Properties systemProps,
      final KsqlClientBuilder clientBuilder,
      final CliBuilder cliBuilder
  ) {
    this.options = Objects.requireNonNull(options, "options");
    this.systemProps = Objects.requireNonNull(systemProps, "systemProps");
    this.clientBuilder = Objects.requireNonNull(clientBuilder, "clientBuilder");
    this.cliBuilder = Objects.requireNonNull(cliBuilder, "cliBuilder");
  }

  public static void main(final String[] args) throws IOException {
    final Options options = args.length == 0
        ? Options.parse("http://localhost:8088")
        : Options.parse(args);

    if (options == null) {
      System.exit(-1);
    }

    try {
      new Ksql(options, System.getProperties(), KsqlRestClient::new, Cli::build).run();
    } catch (final Exception e) {
      final String msg = ErrorMessageUtil.buildErrorMessage(e);
      LOGGER.error(msg);
      System.err.println(msg);
      System.exit(-1);
    }
  }

  void run() {
    final Map<String, String> configProps = options.getConfigFile()
        .map(Ksql::loadProperties)
        .orElseGet(Collections::emptyMap);

    final Map<String, String> localProps = stripClientSideProperties(configProps);
    final Map<String, String> clientProps = PropertiesUtil.applyOverrides(configProps, systemProps);
    final String server = options.getServer();

    try (KsqlRestClient restClient = clientBuilder.build(server, localProps, clientProps)) {

      options.getUserNameAndPassword().ifPresent(
          creds -> restClient.setupAuthenticationCredentials(creds.left, creds.right)
      );

      final KsqlVersionCheckerAgent versionChecker = new KsqlVersionCheckerAgent(() -> false);
      versionChecker.start(KsqlModuleType.CLI, PropertiesUtil.asProperties(configProps));
      try (Cli cli = cliBuilder.build(
          options.getStreamedQueryRowLimit(),
          options.getStreamedQueryTimeoutMs(),
          options.getOutputFormat(),
          restClient)
      ) {
        cli.runInteractively();
      }
    }
  }

  private static Map<String, String> stripClientSideProperties(final Map<String, String> props) {
    return props.entrySet()
        .stream()
        .filter(e -> !e.getKey().startsWith("ssl."))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private static Map<String, String> loadProperties(final String propertiesFile) {
    final Map<String, String> properties = PropertiesUtil
        .loadProperties(new File(propertiesFile));

    final String serviceId = properties.get(KsqlConfig.KSQL_SERVICE_ID_CONFIG);
    if (serviceId != null) {
      properties.put(StreamsConfig.APPLICATION_ID_CONFIG, serviceId);
    }

    return properties;
  }

  interface KsqlClientBuilder {
    KsqlRestClient build(
        String serverAddress,
        Map<String, ?> localProperties,
        Map<String, String> clientProps);
  }

  interface CliBuilder {
    Cli build(
        Long streamedQueryRowLimit,
        Long streamedQueryTimeoutMs,
        OutputFormat outputFormat,
        KsqlRestClient restClient);
  }
}
