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

import static io.confluent.ksql.tools.migrations.util.MigrationsDirectoryUtil.getAllMigrations;
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
import io.confluent.ksql.tools.migrations.util.MigrationsDirectoryUtil;
import io.confluent.ksql.tools.migrations.util.MigrationsUtil;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.RetryUtil;
import java.time.Clock;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
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
        getMigrationsDirFromConfigFile(configFile),
        Clock.systemDefaultZone()
    );
  }

  @VisibleForTesting
  int command(
      final MigrationConfig config,
      final Function<MigrationConfig, Client> clientSupplier,
      final String migrationsDir,
      final Clock clock
  ) {
    final Client ksqlClient;
    try {
      ksqlClient = clientSupplier.apply(config);
    } catch (MigrationException e) {
      LOGGER.error(e.getMessage());
      return 1;
    }

    if (ValidateMigrationsCommand.validate(config, migrationsDir, ksqlClient)
        && apply(config, ksqlClient, migrationsDir, clock)) {
      ksqlClient.close();
      return 0;
    } else {
      ksqlClient.close();
      return 1;
    }
  }

  private boolean apply(
      final MigrationConfig config,
      final Client ksqlClient,
      final String migrationsDir,
      final Clock clock
  ) {
    String previous = MetadataUtil.getLatestMigratedVersion(config, ksqlClient);
    final int minimumVersion = previous.equals(MetadataUtil.NONE_VERSION)
        ? 1
        : Integer.parseInt(previous) + 1;
    if (minimumVersion <= 0) {
      LOGGER.error("Invalid migration version found: " + minimumVersion);
    }

    LOGGER.info("Loading migration files");
    final List<Migration> migrations;
    try {
      migrations = getAllMigrations(migrationsDir).stream()
          .filter(migration -> {
            if (version > 0) {
              return migration.getVersion() <= version && migration.getVersion() >= minimumVersion;
            } else {
              return migration.getVersion() >= minimumVersion;
            }
          })
          .collect(Collectors.toList());
    } catch (MigrationException e) {
      LOGGER.error(e.getMessage());
      return false;
    }

    if (migrations.size() == 0) {
      LOGGER.info("No eligible migrations found.");
    } else {
      LOGGER.info(migrations.size() + " migration files loaded.");
    }

    for (Migration migration : migrations) {
      if (!applyMigration(config, ksqlClient, migration, clock, previous)) {
        return false;
      }
      previous = Integer.toString(migration.getVersion());
    }
    return true;
  }

  private boolean applyMigration(
      final MigrationConfig config,
      final Client ksqlClient,
      final Migration migration,
      final Clock clock,
      final String previous
  ) {
    LOGGER.info("Applying " + migration.getName() + " version " + migration.getVersion());
    final String migrationFileContent =
        MigrationsDirectoryUtil.getFileContentsForName(migration.getFilepath());
    LOGGER.info(migrationFileContent);

    if (dryRun) {
      return true;
    }

    if (!verifyMigrated(config, ksqlClient, previous, MAX_RETRIES)) {
      LOGGER.error("Failed to verify status of version " + previous);
      return false;
    }

    final String executionStart = Long.toString(clock.millis());

    if (
        !updateState(config, ksqlClient, MigrationState.RUNNING,
            executionStart, migration, clock, previous)
    ) {
      return false;
    }

    try {
      final List<String> commands = Arrays.stream(migrationFileContent.split(";"))
          .filter(s -> s.length() > 1)
          .collect(Collectors.toList());
      for (final String command : commands) {
        ksqlClient.executeStatement(command + ";").get();
      }
    } catch (InterruptedException | ExecutionException e) {
      LOGGER.error(e.getMessage());
      updateState(config, ksqlClient, MigrationState.ERROR,
          executionStart, migration, clock, previous);
      return false;
    }
    updateState(config, ksqlClient, MigrationState.MIGRATED,
        executionStart, migration, clock, previous);
    LOGGER.info("Successfully migrated");
    return true;
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
    try {
      RetryUtil.retryWithBackoff(
          retries,
          1000,
          1000,
          () -> {
            final MigrationState state = MetadataUtil
                .getInfoForVersion(version, config, ksqlClient)
                .getState();
            if (!state.equals(MigrationState.MIGRATED)) {
              throw new MigrationException(
                  String.format("Expected status MIGRATED for version %s. Got %s", version, state));
            }
          }
      );
    } catch (MigrationException e) {
      LOGGER.error(e.getMessage());
      return false;
    }
    return true;
  }

  private boolean updateState(
      final MigrationConfig config,
      final Client ksqlClient,
      final MigrationState state,
      final String executionStart,
      final Migration migration,
      final Clock clock,
      final String previous
  ) {
    final String executionEnd = (state == MigrationState.MIGRATED || state == MigrationState.ERROR)
        ? Long.toString(clock.millis())
        : "";
    final String checksum = MigrationsDirectoryUtil.computeHashForFile(migration.getFilepath());
    try {
      MetadataUtil.writeRow(
          config,
          ksqlClient,
          MetadataUtil.CURRENT_VERSION_KEY,
          state.toString(),
          executionStart,
          executionEnd,
          migration,
          previous,
          checksum
      ).get();
      MetadataUtil.writeRow(
          config,
          ksqlClient,
          Integer.toString(migration.getVersion()),
          state.toString(),
          executionStart,
          executionEnd,
          migration,
          previous,
          checksum
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
