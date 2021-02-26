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
import io.confluent.ksql.tools.migrations.util.MetadataUtil.VersionInfo;
import io.confluent.ksql.tools.migrations.util.MigrationsUtil;
import io.confluent.ksql.util.KsqlException;
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
    final int start = getStartVersion(config, ksqlClient);
    final List<Migration> migrations;
    LOGGER.info("Loading migration files");

    if (next) {
      migrations = getAllMigrations(start, start, migrationsDir);
    } else if (version >= start) {
      migrations = getAllMigrations(start, version, migrationsDir);;
    } else if (version > 0 && version < start) {
      LOGGER.error("Specified version is lower than latest migrated version");
      return false;
    } else {
      migrations = getAllMigrations(start, migrationsDir);
    }

    for (Migration migration : migrations) {
      if (!applyMigration(config, ksqlClient, migration, clock)) {
        return false;
      }
    }
    return true;
  }

  private boolean applyMigration(
      final MigrationConfig config,
      final Client ksqlClient,
      final Migration migration,
      final Clock clock
  ) {
    LOGGER.info("Applying " + migration.getName() + " version " + migration.getVersion());
    LOGGER.info(migration.getCommand());

    if (dryRun) {
      return true;
    }

    if (!verifyMigrated(config, ksqlClient, migration.getPrevious(), MAX_RETRIES)) {
      LOGGER.error("Failed to verify status of version " + migration.getPrevious());
      return false;
    }

    final String executionStart = Long.toString(clock.millis());

    if (
        !updateState(config, ksqlClient, MigrationState.RUNNING, executionStart, migration, clock)
    ) {
      return false;
    }

    try {
      final List<String> commands = Arrays.stream(migration.getCommand().split(";"))
          .filter(s -> s.length() > 1)
          .collect(Collectors.toList());
      for (final String command : commands) {
        ksqlClient.executeStatement(command + ";").get();
      }
    } catch (InterruptedException | ExecutionException e) {
      LOGGER.error(e.getMessage());
      updateState(config, ksqlClient, MigrationState.ERROR, executionStart, migration, clock);
      return false;
    }
    updateState(config, ksqlClient, MigrationState.MIGRATED, executionStart, migration, clock);
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
            "Could not verify status of version %s. Retrying (%d/%d)", version, i, retries));
      }
    }
    return false;
  }

  private boolean updateState(
      final MigrationConfig config,
      final Client ksqlClient,
      final MigrationState state,
      final String executionStart,
      final Migration migration,
      final Clock clock
  ) {
    final String executionEnd = (state == MigrationState.MIGRATED || state == MigrationState.ERROR)
        ? Long.toString(clock.millis())
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
