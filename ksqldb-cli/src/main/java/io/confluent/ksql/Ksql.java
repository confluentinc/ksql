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
import io.confluent.ksql.rest.client.BasicCredentials;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.util.ErrorMessageUtil;
import io.confluent.ksql.util.KsqlException;
import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Ksql {
  private static final Logger LOGGER = LoggerFactory.getLogger(Ksql.class);
  private static final Predicate<String> NOT_CLIENT_SIDE_CONFIG = key -> !key.startsWith("ssl.");

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
    final Options options = Options.parse(args);
    if (options == null) {
      System.exit(-1);
    }

    // ask for password if not set through command parameters
    if (options.requiresPassword()) {
      options.setPassword(readPassword());
    }

    int errorCode = 0;
    try {
      errorCode = new Ksql(
          options,
          System.getProperties(),
          KsqlRestClient::create,
          Cli::build
      ).run();
    } catch (final Exception e) {
      final String msg = ErrorMessageUtil.buildErrorMessage(e);
      LOGGER.error(msg);
      System.err.println(msg);
      System.exit(-1);
    }

    System.exit(errorCode);
  }

  private static String readPassword() {
    final Console console = System.console();
    if (console == null) {
      System.err.println("Could not get console for enter password; use -p option instead.");
      System.exit(-1);
    }

    String password = "";
    while (password.isEmpty()) {
      password = new String(console.readPassword("Enter password: "));
      if (password.isEmpty()) {
        console.writer().println("Error: password can not be empty");
      }
    }
    return password;
  }

  int run() {
    final Map<String, String> configProps = options.getConfigFile()
        .map(Ksql::loadProperties)
        .orElseGet(Collections::emptyMap);

    final Map<String, String> sessionVariables = options.getVariables();

    try (KsqlRestClient restClient = buildClient(configProps)) {
      try (Cli cli = cliBuilder.build(
          options.getStreamedQueryRowLimit(),
          options.getStreamedQueryTimeoutMs(),
          options.getOutputFormat(),
          restClient)
      ) {
        // Add CLI variables If defined by parameters
        cli.addSessionVariables(sessionVariables);

        if (options.getExecute().isPresent()) {
          return cli.runCommand(options.getExecute().get());
        } else if (options.getScriptFile().isPresent()) {
          final File scriptFile = new File(options.getScriptFile().get());
          if (scriptFile.exists() && scriptFile.isFile()) {
            return cli.runScript(scriptFile.getPath());
          } else {
            throw new KsqlException("No such script file: " + scriptFile.getPath());
          }
        } else {
          return cli.runInteractively();
        }
      }
    }
  }

  private KsqlRestClient buildClient(
      final Map<String, String> configProps
  ) {
    final Map<String, String> localProps = stripClientSideProperties(configProps);
    final Map<String, String> clientProps = PropertiesUtil.applyOverrides(configProps, systemProps);
    final String server = options.getServer();
    final Optional<BasicCredentials> creds = options.getUserNameAndPassword();
    final Optional<BasicCredentials> ccloudApiKey = options.getCCloudApiKey();

    return clientBuilder.build(
        server, localProps, clientProps, creds, ccloudApiKey);
  }

  private static Map<String, String> stripClientSideProperties(final Map<String, String> props) {
    return PropertiesUtil.filterByKey(props, NOT_CLIENT_SIDE_CONFIG);
  }

  private static Map<String, String> loadProperties(final String propertiesFile) {
    return PropertiesUtil.loadProperties(new File(propertiesFile));
  }

  interface KsqlClientBuilder {
    KsqlRestClient build(
        String serverAddress,
        Map<String, ?> localProperties,
        Map<String, String> clientProps,
        Optional<BasicCredentials> creds,
        Optional<BasicCredentials> ccloudApiKey
    );
  }

  interface CliBuilder {
    Cli build(
        Long streamedQueryRowLimit,
        Long streamedQueryTimeoutMs,
        OutputFormat outputFormat,
        KsqlRestClient restClient);
  }
}
