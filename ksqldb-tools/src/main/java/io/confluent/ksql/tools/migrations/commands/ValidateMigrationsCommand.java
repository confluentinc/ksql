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

import static io.confluent.ksql.tools.migrations.util.MigrationsDirectoryUtil.computeHashForFile;
import static io.confluent.ksql.tools.migrations.util.MigrationsDirectoryUtil.getFilePathForVersion;
import static io.confluent.ksql.tools.migrations.util.MigrationsDirectoryUtil.getMigrationsDirFromConfigFile;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.help.Discussion;
import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.api.client.BatchedQueryResult;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.tools.migrations.MigrationConfig;
import io.confluent.ksql.tools.migrations.MigrationException;
import io.confluent.ksql.tools.migrations.util.MetadataUtil;
import io.confluent.ksql.tools.migrations.util.MetadataUtil.MigrationState;
import io.confluent.ksql.tools.migrations.util.MigrationsUtil;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Command(
    name = "validate",
    description = "Validate applied migrations against local files."
)
@Discussion(
    paragraphs = {
      "Compares local files checksum against the current metadata checksums to check "
          + "for migrations files that have changed."
          + "This tells the user that their schema might be not valid against their local files."
    }
)
public class ValidateMigrationsCommand extends BaseCommand {

  private static final Logger LOGGER = LoggerFactory.getLogger(ValidateMigrationsCommand.class);

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

    final boolean success;
    try {
      success = validate(config, migrationsDir, ksqlClient);
    } catch (MigrationException e) {
      LOGGER.error(e.getMessage());
      return 1;
    }

    if (success) {
      LOGGER.info("Successfully validated checksums for migrations that have already been applied");
      ksqlClient.close();
    } else {
      ksqlClient.close();
      return 1;
    }

    return 0;
  }

  @Override
  protected Logger getLogger() {
    return null;
  }

  /**
   * @return true if validation passes, else false.
   */
  static boolean validate(
      final MigrationConfig config,
      final String migrationsDir,
      final Client ksqlClient
  ) {
    String version = getLatestMigratedVersion(config, ksqlClient);
    String nextVersion = null;
    while (!version.equals(MetadataUtil.NONE_VERSION)) {
      final VersionInfo versionInfo = getInfoForVersion(version, config, ksqlClient);
      if (nextVersion != null) {
        validateVersionIsMigrated(version, versionInfo, nextVersion);
      }

      final String filename;
      try {
        filename = getFilePathForVersion(version, migrationsDir).get();
      } catch (MigrationException | NoSuchElementException e) {
        LOGGER.error("No migrations file found for version with status {}. Version: {}",
            MigrationState.MIGRATED, version);
        return false;
      }

      final String hash = computeHashForFile(filename);
      final String expectedHash = versionInfo.expectedHash;
      if (!expectedHash.equals(hash)) {
        LOGGER.error("Migrations file found for version {} does not match the checksum saved "
                + "for this version. Expected checksum: {}. Actual checksum: {}. File name: {}",
            version, expectedHash, hash, filename);
        return false;
      }

      nextVersion = version;
      version = versionInfo.prevVersion;
    }

    return true;
  }

  private static String getLatestMigratedVersion(
      final MigrationConfig config,
      final Client ksqlClient
  ) {
    final String currentVersion = MetadataUtil.getCurrentVersion(config, ksqlClient);
    if (currentVersion.equals(MetadataUtil.NONE_VERSION)) {
      return currentVersion;
    }

    final VersionInfo currentVersionInfo = getInfoForVersion(currentVersion, config, ksqlClient);
    if (currentVersionInfo.state == MigrationState.MIGRATED) {
      return currentVersion;
    }

    if (currentVersionInfo.prevVersion.equals(MetadataUtil.NONE_VERSION)) {
      return MetadataUtil.NONE_VERSION;
    }

    final VersionInfo prevVersionInfo = getInfoForVersion(
        currentVersionInfo.prevVersion,
        config,
        ksqlClient
    );
    validateVersionIsMigrated(currentVersionInfo.prevVersion, prevVersionInfo, currentVersion);

    return currentVersionInfo.prevVersion;
  }

  private static void validateVersionIsMigrated(
      final String version,
      final VersionInfo versionInfo,
      final String nextVersion
  ) {
    if (versionInfo.state != MigrationState.MIGRATED) {
      throw new MigrationException(String.format(
          "Discovered version with previous version that does not have status {}. "
              + "Version: {}. Previous version: {}. Previous version status: {}",
          MigrationState.MIGRATED,
          nextVersion,
          version,
          versionInfo.state
      ));
    }
  }

  private static VersionInfo getInfoForVersion(
      final String version,
      final MigrationConfig config,
      final Client ksqlClient
  ) {
    final String migrationTableName = config
        .getString(MigrationConfig.KSQL_MIGRATIONS_TABLE_NAME);
    final BatchedQueryResult result = ksqlClient.executeQuery(
        "SELECT checksum, previous, state FROM " + migrationTableName
            + " WHERE version_key = '" + version + "';");

    final String expectedHash;
    final String prevVersion;
    final String state;
    try {
      final List<Row> resultRows = result.get();
      if (resultRows.size() == 0) {
        throw new MigrationException(
            "Failed to query state for migration with version " + version
                + ": no such migration is present in the migrations metadata table");
      }
      expectedHash = resultRows.get(0).getString(0);
      prevVersion = resultRows.get(0).getString(1);
      state = resultRows.get(0).getString(2);
    } catch (InterruptedException | ExecutionException e) {
      throw new MigrationException(String.format(
          "Failed to query state for migration with version %s: %s", version, e.getMessage()));
    }

    return new VersionInfo(expectedHash, prevVersion, state);
  }

  private static class VersionInfo {
    final String expectedHash;
    final String prevVersion;
    final MigrationState state;

    VersionInfo(final String expectedHash, final String prevVersion, final String state) {
      this.expectedHash = Objects.requireNonNull(expectedHash, "expectedHash");
      this.prevVersion = Objects.requireNonNull(prevVersion, "prevVersion");
      this.state = MigrationState.valueOf(Objects.requireNonNull(state, "state"));
    }
  }
}
