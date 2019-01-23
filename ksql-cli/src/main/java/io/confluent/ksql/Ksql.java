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

package io.confluent.ksql;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.cli.Cli;
import io.confluent.ksql.cli.Options;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.util.ErrorMessageUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.version.metrics.KsqlVersionCheckerAgent;
import io.confluent.ksql.version.metrics.collector.KsqlModuleType;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Ksql {
  private static final Logger LOGGER = LoggerFactory.getLogger(Ksql.class);

  private Ksql() {
  }

  public static void main(final String[] args) throws IOException {
    final Options options = args.length == 0 ? Options.parse("http://localhost:8088")
                                             : Options.parse(args);
    if (options == null) {
      System.exit(-1);
    }

    try {
      final Properties properties = loadProperties(options.getConfigFile());
      try (KsqlRestClient restClient = new KsqlRestClient(options.getServer(), properties)) {

        options.getUserNameAndPassword().ifPresent(
            creds -> restClient.setupAuthenticationCredentials(creds.left, creds.right)
        );

        final KsqlVersionCheckerAgent versionChecker = new KsqlVersionCheckerAgent(() -> false);
        versionChecker.start(KsqlModuleType.CLI, properties);
        try (Cli cli = Cli.build(
            options.getStreamedQueryRowLimit(),
            options.getStreamedQueryTimeoutMs(),
            options.getOutputFormat(),
            restClient)
        ) {
          cli.runInteractively();
        }
      }
    } catch (final Exception e) {
      final String msg = ErrorMessageUtil.buildErrorMessage(e);
      LOGGER.error(msg);
      System.err.println(msg);
      System.exit(-1);
    }
  }

  @SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private static Properties loadProperties(final Optional<String> propertiesFile) {
    final Properties properties = new Properties();
    if (!propertiesFile.isPresent()) {
      return properties;
    }

    try (InputStream input = new FileInputStream(propertiesFile.get())) {
      properties.load(input);
      if (properties.containsKey(KsqlConfig.KSQL_SERVICE_ID_CONFIG)) {
        properties.put(
            StreamsConfig.APPLICATION_ID_CONFIG,
            properties.getProperty(KsqlConfig.KSQL_SERVICE_ID_CONFIG)
        );
      }
      return properties;
    } catch (final IOException e) {
      throw new KsqlException("failed to load properties file: " + propertiesFile.get(), e);
    }
  }
}
