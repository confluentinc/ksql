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

import static io.confluent.ksql.tools.migrations.util.MetadataUtil.getOptionalInfoForVersions;
import static io.confluent.ksql.tools.migrations.util.MigrationsDirectoryUtil.getMigrationsDir;

import com.github.rvesse.airline.annotations.Command;
import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.tools.migrations.MigrationConfig;
import io.confluent.ksql.tools.migrations.MigrationException;
import io.confluent.ksql.tools.migrations.util.MetadataUtil;
import io.confluent.ksql.tools.migrations.util.MigrationFile;
import io.confluent.ksql.tools.migrations.util.MigrationVersionInfo;
import io.confluent.ksql.tools.migrations.util.MigrationVersionInfoFormatter;
import io.confluent.ksql.tools.migrations.util.MigrationsDirectoryUtil;
import io.confluent.ksql.tools.migrations.util.MigrationsUtil;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Command(
    name = "info",
    description = "Displays information about the current and available migrations."
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
      config = MigrationConfig.load(getConfigFile());
    } catch (KsqlException | MigrationException e) {
      LOGGER.error(e.getMessage());
      return 1;
    }

    return command(
        config,
        MigrationsUtil::getKsqlClient,
        getMigrationsDir(getConfigFile(), config)
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
      printCurrentVersion(config, ksqlClient);
      printVersionInfoTable(config, ksqlClient, migrationsDir);

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

  private void printCurrentVersion(
      final MigrationConfig config,
      final Client ksqlClient
  ) {
    final String currentVersion = MetadataUtil.getCurrentVersion(config, ksqlClient);
    LOGGER.info("Current migration version: {}", currentVersion);
  }

  private void printVersionInfoTable(
      final MigrationConfig config,
      final Client ksqlClient,
      final String migrationsDir
  ) {
    final List<MigrationFile> allMigrations =
        MigrationsDirectoryUtil.getAllMigrations(migrationsDir);
    final List<Integer> allVersions = allMigrations.stream()
        .map(MigrationFile::getVersion)
        .collect(Collectors.toList());

    if (allMigrations.size() != 0) {
      final Map<Integer, Optional<MigrationVersionInfo>> versionInfos =
          getOptionalInfoForVersions(allVersions, config, ksqlClient);

      printAsTable(allMigrations, versionInfos);
    } else {
      LOGGER.info("No migrations files found");
    }
  }

  private static void printAsTable(
      final List<MigrationFile> allMigrations,
      final Map<Integer, Optional<MigrationVersionInfo>> versionInfos
  ) {
    final MigrationVersionInfoFormatter formatter = new MigrationVersionInfoFormatter();

    for (final MigrationFile migration : allMigrations) {
      final MigrationVersionInfo versionInfo = versionInfos.get(migration.getVersion()).orElse(
          MigrationVersionInfo.pendingMigration(migration.getVersion(), migration.getName()));
      formatter.addVersionInfo(versionInfo);
    }

    LOGGER.info(formatter.getFormatted());
  }
}
