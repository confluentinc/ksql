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

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.MutuallyExclusiveWith;
import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.tools.migrations.Migration;
import io.confluent.ksql.tools.migrations.MigrationConfig;
import io.confluent.ksql.tools.migrations.MigrationException;
import io.confluent.ksql.tools.migrations.util.MetadataUtil;
import io.confluent.ksql.tools.migrations.util.MetadataUtil.MigrationState;
import io.confluent.ksql.tools.migrations.util.MetadataUtil.VersionInfo;
import io.confluent.ksql.tools.migrations.util.MigrationsDirectoryUtil;
import io.confluent.ksql.tools.migrations.util.MigrationsUtil;
import io.confluent.ksql.util.KsqlException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Command(
    name = "apply",
    description = "Migrates a schema to new available schema versions"
)
public class ApplyMigrationCommand extends BaseCommand {

  private static final Logger LOGGER = LoggerFactory.getLogger(ApplyMigrationCommand.class);

  private static final int MAX_RETRIES = 10;

  @Option(
      title = "all",
      name = {"-a", "--all"},
      description = "run all available migrations"
  )
  @MutuallyExclusiveWith(tag = "target")
  private boolean all;

  @Option(
      title = "next",
      name = {"-n", "--next"},
      description = "migrate the next available version"
  )
  @MutuallyExclusiveWith(tag = "target")
  private boolean next;

  @Option(
      title = "version",
      name = {"-u", "--until"},
      arity = 1,
      description = "migrate until the specified version"
  )
  @MutuallyExclusiveWith(tag = "target")
  private int version;

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

    if (!ValidateMigrationsCommand.validate(config, migrationsDir, ksqlClient)) {
      return 1;
    }

    final int startVersion = getStartVersion(config, ksqlClient);
    final int endVersion;
    try {
      endVersion = getEndVersion(startVersion, migrationsDir);
    } catch (MigrationException e) {
      LOGGER.error(e.getMessage());
      return 1;
    }

    LOGGER.info("Loading migration files");

    final List<Migration> migrations;
    try {
      migrations = loadMigrations(startVersion, endVersion, migrationsDir);
    } catch (MigrationException e) {
      LOGGER.error(e.getMessage());
      return 1;
    }

    for (Migration migration : migrations) {
      if (!applyMigration(config, ksqlClient, migration)) {
        return 1;
      }
    }
    return 0;
  }

  private List<Migration> loadMigrations(
      final int start,
      final int end,
      final String migrationsDir
  ) {
    final List<Migration> migrations = new ArrayList<>();
    for (int version = start; version <= end; version++) {
      migrations.add(loadMigration(version, migrationsDir));
    }
    return migrations;
  }

  private Migration loadMigration(
      final int version,
      final String migrationsDir
  ) {
    final String versionString = Integer.toString(version);

    final Optional<String> migrationFilePath =
        MigrationsDirectoryUtil.getFilePathForVersion(versionString, migrationsDir);
    if (!migrationFilePath.isPresent()) {
      throw new MigrationException("Failed to find file for version " + versionString);
    }

    final String migrationCommand =
        MigrationsDirectoryUtil.getFileContentsForVersion(versionString, migrationsDir);

    LOGGER.info(migrationFilePath.get() + " loaded");
    return new Migration(
        version,
        MigrationsDirectoryUtil.getNameFromMigrationFilePath(migrationFilePath.get()),
        MigrationsDirectoryUtil.computeHashForFile(migrationFilePath.get()),
        migrationCommand
    );
  }

  private boolean applyMigration(
      final MigrationConfig config,
      final Client ksqlClient,
      final Migration migration
  ) {
    LOGGER.info("Applying " + migration.getName() + " version " + migration.getVersion());
    LOGGER.info(migration.getCommand());

    if (dryRun) {
      return true;
    }

    if (!verifyMigrated(config, ksqlClient, migration.getPrevious(), MAX_RETRIES)) {
      LOGGER.error("Failed to verify status of version" + migration.getPrevious());
      return false;
    }

    final String executionStart = Long.toString(System.nanoTime() / 1000000);

    if (!updateState(config, ksqlClient, MigrationState.RUNNING, executionStart, migration)) {
      return false;
    }

    try {
      final List<String> commands = Arrays.asList(migration.getCommand().split(";"));
      for (final String command : commands) {
        ksqlClient.executeStatement(command + ";").get();
      }
    } catch (InterruptedException | ExecutionException e) {
      LOGGER.error(e.getMessage());
      updateState(config, ksqlClient, MigrationState.ERROR, executionStart, migration);
      return false;
    }
    updateState(config, ksqlClient, MigrationState.MIGRATED, executionStart, migration);
    LOGGER.info("Successfully migrated");
    return true;
  }

  private int getStartVersion(final MigrationConfig config, final Client ksqlClient) {
    final String latestMigratedVersion = MetadataUtil.getLatestMigratedVersion(config, ksqlClient);
    if (latestMigratedVersion.equals(MetadataUtil.NONE_VERSION)) {
      return 1;
    } else {
      return Integer.parseInt(latestMigratedVersion) + 1;
    }
  }

  private int getEndVersion(final int startVersion, final String migrationsDir) {
    if (next) {
      return startVersion;
    } else if (version >= startVersion) {
      return version;
    } else if (version > 0 && version < startVersion) {
      throw new MigrationException("Specified version is lower than latest migrated version");
    } else {
      int endVersion = startVersion;
      while (
          MigrationsDirectoryUtil
              .getFilePathForVersion(Integer.toString(endVersion), migrationsDir)
              .isPresent()
      ) {
        endVersion++;
      }
      return endVersion - 1;
    }
  }

  private boolean verifyMigrated(
      final MigrationConfig config,
      final Client ksqlClient,
      final String version,
      final int retries
  ) {
    if (version.equals(MetadataUtil.NONE_VERSION)) {
      return true;
    }
    for (int i = 1; i <= retries; i++) {
      final VersionInfo versionInfo = MetadataUtil.getInfoForVersion(version, config, ksqlClient);
      if (versionInfo.getState().equals(MigrationState.MIGRATED)) {
        return true;
      } else {
        LOGGER.info(String.format(
            "Could not verify status of version %s. Retrying (%i/%i)", version, i, retries));
      }
    }
    return false;
  }

  private boolean updateState(
      final MigrationConfig config,
      final Client ksqlClient,
      final MigrationState state,
      final String executionStart,
      final Migration migration
  ) {
    final String executionEnd = (state == MigrationState.MIGRATED || state == MigrationState.ERROR)
        ? Long.toString(System.nanoTime() / 1000000)
        : "";
    try {
      MetadataUtil.writeRow(
          config,
          ksqlClient,
          MetadataUtil.CURRENT_VERSION_KEY,
          state.toString(),
          executionStart,
          executionEnd,
          migration
      ).get();
      MetadataUtil.writeRow(
          config,
          ksqlClient,
          migration.getVersion(),
          state.toString(),
          executionStart,
          executionEnd,
          migration
      ).get();
      return true;
    } catch (InterruptedException | ExecutionException e) {
      LOGGER.error(e.getMessage());
      return false;
    }
  }

  @Override
  protected Logger getLogger() {
    return LOGGER;
  }
}
