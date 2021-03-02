/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.tools.migrations.commands;

import static io.confluent.ksql.tools.migrations.util.MigrationsDirectoryUtil.getMigrationsDirFromConfigFile;
import static io.confluent.ksql.tools.migrations.util.ServerVersionUtil.getServerInfo;
import static io.confluent.ksql.tools.migrations.util.ServerVersionUtil.versionSupportsMultiKeyPullQuery;

import com.github.rvesse.airline.annotations.Command;
import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ServerInfo;
import io.confluent.ksql.tools.migrations.MigrationConfig;
import io.confluent.ksql.tools.migrations.MigrationException;
import io.confluent.ksql.tools.migrations.util.MigrationsUtil;
import io.confluent.ksql.util.KsqlException;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Command(
    name = "info",
    description = "Displays information about the current and available migrations"
)
public class MigrationInfoCommand extends BaseCommand {

  private static final Logger LOGGER = LoggerFactory.getLogger(MigrationInfoCommand.class);

  @Override
  protected int command() {
    if (!validateConfigFilePresent()) {
      return 1;
    }

    final MigrationConfig config;
    try {
      config = MigrationConfig.load(configFile);
    } catch (KsqlException | MigrationException e) {
      LOGGER.error(e.getMessage());
      return 1;
    }

    return command(
        config,
        MigrationsUtil::getKsqlClient,
        getMigrationsDirFromConfigFile(configFile)
    );
  }

  @VisibleForTesting
  int command(
      final MigrationConfig config,
      final Function<MigrationConfig, Client> clientSupplier,
      final String migrationsDir
  ) {
    final Client ksqlClient;
    try {
      ksqlClient = clientSupplier.apply(config);
    } catch (MigrationException e) {
      LOGGER.error(e.getMessage());
      return 1;
    }

    if (!validateMetadataInitialized(ksqlClient, config)) {
      ksqlClient.close();
      return 1;
    }

    boolean success;
    try {
      // find all files

      // issue either a single pull query or multiple pull queries to get status
      if (serverSupportsMultiKeyPullQuery(ksqlClient, config)) {

      } else {

      }

      // format into table and print

      success = true;
    } catch (MigrationException e) {
      LOGGER.error(e.getMessage());
      success = false;
    } finally {
      ksqlClient.close();
    }

    return success ? 0 : 1;
  }

  @Override
  protected Logger getLogger() {
    return LOGGER;
  }

  private static boolean serverSupportsMultiKeyPullQuery(
      final Client ksqlClient,
      final MigrationConfig config
  ) {
    final String ksqlServerUrl = config.getString(MigrationConfig.KSQL_SERVER_URL);
    final ServerInfo serverInfo = getServerInfo(ksqlClient, ksqlServerUrl);
    return versionSupportsMultiKeyPullQuery(serverInfo.getServerVersion());
  }

}
