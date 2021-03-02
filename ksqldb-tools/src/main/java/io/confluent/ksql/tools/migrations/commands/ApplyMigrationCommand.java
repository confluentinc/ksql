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
import static io.confluent.ksql.tools.migrations.util.MigrationsDirectoryUtil.getMigrationForVersion;
import static io.confluent.ksql.tools.migrations.util.MigrationsDirectoryUtil.getMigrationsDirFromConfigFile;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.RequireOnlyOne;
import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.tools.migrations.MigrationConfig;
import io.confluent.ksql.tools.migrations.MigrationException;
import io.confluent.ksql.tools.migrations.util.MetadataUtil;
import io.confluent.ksql.tools.migrations.util.MetadataUtil.MigrationState;
import io.confluent.ksql.tools.migrations.util.MigrationFile;
import io.confluent.ksql.tools.migrations.util.MigrationsDirectoryUtil;
import io.confluent.ksql.tools.migrations.util.MigrationsUtil;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.RetryUtil;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
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
  @RequireOnlyOne(tag = "target")
  private boolean all;

  @Option(
      title = "next",
      name = {"-n", "--next"},
      description = "migrate the next available version"
  )
  @RequireOnlyOne(tag = "target")
  private boolean next;

  @Option(
      title = "untilVersion",
      name = {"-u", "--until"},
      arity = 1,
      description = "migrate until the specified version"
  )
  @RequireOnlyOne(tag = "target")
  private int untilVersion;

  @Option(
      title = "untilVersion",
      name = {"-v", "--version"},
      arity = 1,
      description = "apply the migration with the specified version"
  )
  @RequireOnlyOne(tag = "target")
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
    if (untilVersion < 0) {
      LOGGER.error("'until' migration version must be positive. Got: {}", untilVersion);
      return 1;
    }
    if (version < 0) {
      LOGGER.error("migration version to apply must be positive. Got: {}", version);
      return 1;
    }

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
      success = validateCurrentState(config, ksqlClient, migrationsDir)
          && apply(config, ksqlClient, migrationsDir, clock);
    } catch (MigrationException e) {
      LOGGER.error(e.getMessage());
      success = false;
    } finally {
      ksqlClient.close();
    }

    return success ? 0 : 1;
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

    LOGGER.info("Loading migration files");
    final List<MigrationFile> migrations;
    try {
      migrations = loadMigrationsToApply(migrationsDir, minimumVersion);
    } catch (MigrationException e) {
      LOGGER.error(e.getMessage());
      return false;
    }

    if (migrations.size() == 0) {
      LOGGER.info("No eligible migrations found.");
    } else {
      LOGGER.info(migrations.size() + " migration file(s) loaded.");
    }

    for (MigrationFile migration : migrations) {
      if (!applyMigration(config, ksqlClient, migration, clock, previous)) {
        return false;
      }
      previous = Integer.toString(migration.getVersion());
    }

    return true;
  }

  private List<MigrationFile> loadMigrationsToApply(
      final String migrationsDir,
      final int minimumVersion
  ) {
    if (version > 0) {
      final Optional<MigrationFile> migration =
          getMigrationForVersion(String.valueOf(version), migrationsDir);
      if (!migration.isPresent()) {
        throw new MigrationException("No migration file with version " + version + " exists.");
      }
      return Collections.singletonList(migration.get());
    }

    final List<MigrationFile> migrations = getAllMigrations(migrationsDir).stream()
        .filter(migration -> {
          if (migration.getVersion() < minimumVersion) {
            return false;
          }
          if (untilVersion > 0) {
            return migration.getVersion() <= untilVersion;
          } else {
            return true;
          }
        })
        .collect(Collectors.toList());

    if (next) {
      if (migrations.size() == 0) {
        throw new MigrationException("No eligible migrations found.");
      }
      return Collections.singletonList(migrations.get(0));
    }

    return migrations;
  }

  private boolean applyMigration(
      final MigrationConfig config,
      final Client ksqlClient,
      final MigrationFile migration,
      final Clock clock,
      final String previous
  ) {
    LOGGER.info("Applying migration version {}: {}", migration.getVersion(), migration.getName());
    final String migrationFileContent =
        MigrationsDirectoryUtil.getFileContentsForName(migration.getFilepath());
    LOGGER.info("MigrationFile file contents:\n{}", migrationFileContent);

    if (dryRun) {
      LOGGER.info("Dry run complete. No migrations were actually applied.");
      return true;
    }

    if (!verifyMigrated(config, ksqlClient, previous, MAX_RETRIES)) {
      LOGGER.error("Failed to verify status of version " + previous);
      return false;
    }

    final String executionStart = Long.toString(clock.millis());

    if (
        !updateState(config, ksqlClient, MigrationState.RUNNING,
            executionStart, migration, clock, previous, Optional.empty())
    ) {
      return false;
    }

    final List<String> commands = Arrays.stream(migrationFileContent.split(";"))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .map(s -> s + ";")
        .collect(Collectors.toList());
    for (final String command : commands) {
      try {
        ksqlClient.executeStatement(command).get();
      } catch (InterruptedException | ExecutionException e) {
        final String errorMsg = String.format(
            "Failed to execute sql: %s. Error: %s", command, e.getMessage());
        LOGGER.error(errorMsg);
        updateState(config, ksqlClient, MigrationState.ERROR,
            executionStart, migration, clock, previous, Optional.of(errorMsg));
        return false;
      }
    }

    updateState(config, ksqlClient, MigrationState.MIGRATED,
        executionStart, migration, clock, previous, Optional.empty());
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
      final MigrationFile migration,
      final Clock clock,
      final String previous,
      final Optional<String> errorReason
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
          checksum,
          errorReason
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
          checksum,
          errorReason
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

  private static boolean validateCurrentState(
      final MigrationConfig config,
      final Client ksqlClient,
      final String migrationsDir
  ) {
    LOGGER.info("Validating current migration state before applying new migrations");
    return ValidateMigrationsCommand.validate(config, migrationsDir, ksqlClient);
  }
}
