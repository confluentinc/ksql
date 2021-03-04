/*
 * Copyright 2021 Confluent Inc.
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

import com.github.rvesse.airline.annotations.Command;
import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.tools.migrations.MigrationConfig;
import io.confluent.ksql.tools.migrations.MigrationException;
import io.confluent.ksql.tools.migrations.util.MigrationsUtil;
import io.confluent.ksql.tools.migrations.util.ServerVersionUtil;
import io.confluent.ksql.util.KsqlException;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Command(
    name = InitializeMigrationCommand.INITIALIZE_COMMAND_NAME,
    description = "Initializes the schema metadata (stream and table)."
)
public class InitializeMigrationCommand extends BaseCommand {

  static final String INITIALIZE_COMMAND_NAME = "initialize";

  private static final Logger LOGGER = LoggerFactory.getLogger(InitializeMigrationCommand.class);

  private String createEventStream(final String name, final String topic, final int replicas) {
    return "CREATE STREAM " + name + " (\n"
        + "  version_key  STRING KEY,\n"
        + "  version      STRING,\n"
        + "  name         STRING,\n"
        + "  state        STRING,  \n"
        + "  checksum     STRING,\n"
        + "  started_on   STRING,\n"
        + "  completed_on STRING,\n"
        + "  previous     STRING,\n"
        + "  error_reason STRING\n"
        + ") WITH (  \n"
        + "  KAFKA_TOPIC='" + topic + "',\n"
        + "  VALUE_FORMAT='JSON',\n"
        + "  PARTITIONS=1,\n"
        + "  REPLICAS= " + replicas + " \n"
        + ");\n";
  }

  private String createVersionTable(
      final String tableName,
      final String streamName,
      final String topic
  ) {
    return "CREATE TABLE " + tableName + "\n"
        + "  WITH (\n"
        + "    KAFKA_TOPIC='" + topic + "'\n"
        + "  )\n"
        + "  AS SELECT \n"
        + "    version_key, \n"
        + "    latest_by_offset(version) as version, \n"
        + "    latest_by_offset(name) AS name, \n"
        + "    latest_by_offset(state) AS state,     \n"
        + "    latest_by_offset(checksum) AS checksum, \n"
        + "    latest_by_offset(started_on) AS started_on, \n"
        + "    latest_by_offset(completed_on) AS completed_on, \n"
        + "    latest_by_offset(previous) AS previous, \n"
        + "    latest_by_offset(error_reason) AS error_reason \n"
        + "  FROM " + streamName + " \n"
        + "  GROUP BY version_key;\n";
  }

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

    return command(config, MigrationsUtil::getKsqlClient);
  }

  @VisibleForTesting
  int command(
      final MigrationConfig config,
      final Function<MigrationConfig, Client> clientSupplier
  ) {
    final String streamName = config.getString(MigrationConfig.KSQL_MIGRATIONS_STREAM_NAME);
    final String tableName = config.getString(MigrationConfig.KSQL_MIGRATIONS_TABLE_NAME);
    final String eventStreamCommand = createEventStream(
        streamName,
        config.getString(MigrationConfig.KSQL_MIGRATIONS_STREAM_TOPIC_NAME),
        config.getInt(MigrationConfig.KSQL_MIGRATIONS_TOPIC_REPLICAS)
    );
    final String versionTableCommand = createVersionTable(
        tableName,
        streamName,
        config.getString(MigrationConfig.KSQL_MIGRATIONS_TABLE_TOPIC_NAME)
    );

    final Client ksqlClient;
    try {
      ksqlClient = clientSupplier.apply(config);
    } catch (MigrationException e) {
      LOGGER.error(e.getMessage());
      return 1;
    }

    if (ServerVersionUtil.serverVersionCompatible(ksqlClient, config)
        && tryCreate(ksqlClient, eventStreamCommand, streamName, true)
        && tryCreate(ksqlClient, versionTableCommand, tableName, false)) {
      LOGGER.info("Migrations metadata initialized successfully");
      ksqlClient.close();
    } else {
      ksqlClient.close();
      return 1;
    }

    return 0;
  }

  private static boolean tryCreate(
      final Client client,
      final String command,
      final String name,
      final boolean isStream
  ) {
    final String type = isStream ? "stream" : "table";
    try {
      LOGGER.info("Creating " + type + ": " + name);
      client.executeStatement(command).get();
    } catch (InterruptedException | ExecutionException e) {
      LOGGER.error(String.format("Failed to create %s %s: %s", type, name, e.getMessage()));
      return false;
    }
    return true;
  }

  @Override
  protected Logger getLogger() {
    return LOGGER;
  }
}
