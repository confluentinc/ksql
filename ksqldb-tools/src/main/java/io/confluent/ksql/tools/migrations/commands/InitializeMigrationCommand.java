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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.tools.migrations.MigrationConfig;
import io.confluent.ksql.tools.migrations.MigrationException;
import io.confluent.ksql.tools.migrations.MigrationsUtil;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Command(
    name = "initialize",
    description = "Initializes the schema metadata (stream and table)."
)
public class InitializeMigrationCommand extends BaseCommand {

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
        + "  previous     STRING\n"
        + ") WITH (  \n"
        + "  KAFKA_TOPIC='" + topic + "',\n"
        + "  VALUE_FORMAT='JSON',\n"
        + "  PARTITIONS=1,\n"
        + "  REPLICAS= " + replicas + " \n"
        + ");\n";
  }

  private String createVersionTable(final String name, final String topic) {
    return "CREATE TABLE " + name + "\n"
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
        + "    latest_by_offset(previous) AS previous\n"
        + "  FROM migration_events \n"
        + "  GROUP BY version_key;\n";
  }

  @Override
  @SuppressFBWarnings("DM_EXIT")
  protected void command() {
    final MigrationConfig properties = MigrationConfig.load();
    final String streamName = properties.getString(MigrationConfig.KSQL_MIGRATIONS_STREAM_NAME);
    final String tableName = properties.getString(MigrationConfig.KSQL_MIGRATIONS_TABLE_NAME);
    final String eventStreamCommand = createEventStream(
        streamName,
        properties.getString(MigrationConfig.KSQL_MIGRATIONS_STREAM_TOPIC_NAME),
        properties.getInt(MigrationConfig.KSQL_MIGRATIONS_TOPIC_REPLICAS)
    );
    final String versionTableCommand = createVersionTable(
        tableName,
        properties.getString(MigrationConfig.KSQL_MIGRATIONS_TABLE_TOPIC_NAME)
    );
    final String ksqlServerUrl = properties.getString(MigrationConfig.KSQL_SERVER_URL);
    final Client ksqlClient;

    try {
      ksqlClient = MigrationsUtil.getKsqlClient(ksqlServerUrl);
    } catch (MigrationException e) {
      LOGGER.error(e.getMessage());
      System.exit(1);
      return;
    }

    if (tryCreate(ksqlClient, eventStreamCommand, streamName, true)
        && tryCreate(ksqlClient, versionTableCommand, tableName, false)) {
      LOGGER.info("Schema metadata initialized successfully");
      ksqlClient.close();
    } else {
      ksqlClient.close();
      System.exit(1);
    }
  }

  private boolean tryCreate(
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
