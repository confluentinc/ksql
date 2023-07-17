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

import com.github.rvesse.airline.annotations.Command;
import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.QueryInfo;
import io.confluent.ksql.api.client.SourceDescription;
import io.confluent.ksql.api.client.StreamInfo;
import io.confluent.ksql.api.client.TableInfo;
import io.confluent.ksql.tools.migrations.MigrationConfig;
import io.confluent.ksql.tools.migrations.MigrationException;
import io.confluent.ksql.tools.migrations.util.MigrationsUtil;
import io.confluent.ksql.tools.migrations.util.ServerVersionUtil;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Command(
    name = "destroy-metadata",
    description = "Destroys all ksqlDB server resources related to migrations, including "
        + "the migrations metadata stream and table and their underlying Kafka topics. "
        + "WARNING: this is not reversible!"
)
public class DestroyMigrationsCommand extends BaseCommand {

  private static final Logger LOGGER = LoggerFactory.getLogger(DestroyMigrationsCommand.class);

  @Override
  protected int command() {
    if (!validateConfigFilePresent()) {
      return 1;
    }

    final MigrationConfig config;
    try {
      config = MigrationConfig.load(getConfigFile());
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

    final Client ksqlClient;
    try {
      ksqlClient = clientSupplier.apply(config);
    } catch (MigrationException e) {
      LOGGER.error(e.getMessage());
      return 1;
    }

    LOGGER.info("Cleaning migrations metadata stream and table from ksqlDB server");
    if (ServerVersionUtil.serverVersionCompatible(ksqlClient, config)
        && deleteMigrationsTable(ksqlClient, tableName)
        && deleteMigrationsStream(ksqlClient, streamName)) {
      LOGGER.info("Migrations metadata cleaned successfully");
      ksqlClient.close();
      return 0;
    } else {
      ksqlClient.close();
      return 1;
    }
  }

  @Override
  protected Logger getLogger() {
    return LOGGER;
  }

  private boolean deleteMigrationsTable(final Client ksqlClient, final String tableName) {
    try {
      if (!sourceExists(ksqlClient, tableName, true)) {
        LOGGER.info("Metadata table does not exist. Skipping cleanup.");
        return true;
      }

      final SourceDescription tableInfo = getSourceInfo(ksqlClient, tableName, true);
      terminateQueryForTable(ksqlClient, tableInfo);
      dropSource(ksqlClient, tableName, true);

      return true;
    } catch (MigrationException e) {
      LOGGER.error(e.getMessage());
      return false;
    }
  }

  private boolean deleteMigrationsStream(final Client ksqlClient, final String streamName) {
    try {
      if (!sourceExists(ksqlClient, streamName, false)) {
        LOGGER.info("Metadata stream does not exist. Skipping cleanup.");
        return true;
      }

      dropSource(ksqlClient, streamName, false);
      return true;
    } catch (MigrationException e) {
      LOGGER.error(e.getMessage());
      return false;
    }
  }

  private static boolean sourceExists(
      final Client ksqlClient,
      final String sourceName,
      final boolean isTable
  ) {
    try {
      if (isTable) {
        final List<TableInfo> tables = ksqlClient.listTables().get();
        return tables.stream()
            .anyMatch(tableInfo -> tableInfo.getName().equalsIgnoreCase(sourceName));
      } else {
        final List<StreamInfo> streams = ksqlClient.listStreams().get();
        return streams.stream()
            .anyMatch(streamInfo -> streamInfo.getName().equalsIgnoreCase(sourceName));
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new MigrationException(String.format(
          "Failed to check for presence of metadata %s '%s': %s",
              isTable ? "table" : "stream",
              sourceName,
              e.getMessage()));
    }
  }

  /**
   * Returns the source description, assuming the source exists.
   */
  private static SourceDescription getSourceInfo(
      final Client ksqlClient,
      final String sourceName,
      final boolean isTable
  ) {
    try {
      return ksqlClient.describeSource(sourceName).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new MigrationException(String.format("Failed to describe metadata %s '%s': %s",
          isTable ? "table" : "stream",
          sourceName,
          e.getMessage()));
    }
  }

  private static void terminateQueryForTable(
      final Client ksqlClient,
      final SourceDescription tableDesc
  ) {
    final List<QueryInfo> queries = tableDesc.writeQueries();
    if (queries.size() == 0) {
      LOGGER.info("Found 0 queries writing to the metadata table");
      return;
    }

    if (queries.size() > 1) {
      throw new MigrationException(
              "Found multiple queries writing to the metadata table. Query IDs: "
          + queries.stream()
              .map(QueryInfo::getId)
              .collect(Collectors.joining("', '", "'", "'.")));
    }

    final String queryId = queries.get(0).getId();
    LOGGER.info("Found 1 query writing to the metadata table. Query ID: {}", queryId);

    LOGGER.info("Terminating query with ID: {}", queryId);
    try {
      ksqlClient.executeStatement("TERMINATE " + queryId + ";").get();
    } catch (InterruptedException | ExecutionException e) {
      throw new MigrationException(String.format(
          "Failed to terminate query populating metadata table. Query ID: %s. Error: %s",
              queryId, e.getMessage()));
    }
  }

  private static void dropSource(
      final Client ksqlClient,
      final String sourceName,
      final boolean isTable
  ) {
    final String sourceType = isTable ? "table" : "stream";
    LOGGER.info("Dropping migrations metadata {}: {}", sourceType, sourceName);
    try {
      final String sql = String.format("DROP %s %s DELETE TOPIC;",
          sourceType.toUpperCase(), sourceName);
      ksqlClient.executeStatement(sql).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new MigrationException(String.format("Failed to drop metadata %s '%s': %s",
          sourceType,
          sourceName,
          e.getMessage()));
    }
  }
}
