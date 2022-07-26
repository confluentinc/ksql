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

import static io.confluent.ksql.tools.migrations.util.MetadataUtil.getInfoForVersion;
import static io.confluent.ksql.tools.migrations.util.MetadataUtil.getLatestMigratedVersion;
import static io.confluent.ksql.tools.migrations.util.MetadataUtil.validateVersionIsMigrated;
import static io.confluent.ksql.tools.migrations.util.MigrationsDirectoryUtil.computeHashForFile;
import static io.confluent.ksql.tools.migrations.util.MigrationsDirectoryUtil.getMigrationForVersion;
import static io.confluent.ksql.tools.migrations.util.MigrationsDirectoryUtil.getMigrationsDir;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.help.Discussion;
import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.tools.migrations.MigrationConfig;
import io.confluent.ksql.tools.migrations.MigrationException;
import io.confluent.ksql.tools.migrations.util.MetadataUtil;
import io.confluent.ksql.tools.migrations.util.MetadataUtil.MigrationState;
import io.confluent.ksql.tools.migrations.util.MigrationVersionInfo;
import io.confluent.ksql.tools.migrations.util.MigrationsUtil;
import io.confluent.ksql.util.KsqlException;
import java.util.NoSuchElementException;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Command(
    name = "validate",
    description = "Validates applied migrations against local files."
)
@Discussion(
    paragraphs = {
      "This command checks whether the migration files that have been applied are "
          + "the same as the local migration files with the same version, by computing "
          + "hashes for the local files and comparing them to the checksums saved with "
          + "the applied migrations."
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
      success = validate(config, migrationsDir, ksqlClient);
    } catch (MigrationException e) {
      LOGGER.error(e.getMessage());
      success = false;
    } finally {
      ksqlClient.close();
    }

    if (success) {
      LOGGER.info("Successfully validated checksums for migrations that have already been applied");
      return 0;
    } else {
      return 1;
    }
  }

  @Override
  protected Logger getLogger() {
    return LOGGER;
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
      final MigrationVersionInfo versionInfo = getInfoForVersion(version, config, ksqlClient);
      if (nextVersion != null) {
        validateVersionIsMigrated(version, versionInfo, nextVersion);
      }

      final String filename;
      try {
        filename = getMigrationForVersion(version, migrationsDir).get().getFilepath();
      } catch (MigrationException | NoSuchElementException e) {
        LOGGER.error("No migrations file found for version with status {}. Version: {}",
            MigrationState.MIGRATED, version);
        return false;
      }

      final String hash = computeHashForFile(filename);
      final String expectedHash = versionInfo.getExpectedHash();
      if (!expectedHash.equals(hash)) {
        LOGGER.error("Migrations file found for version {} does not match the checksum saved "
                + "for this version. Expected checksum: {}. Actual checksum: {}. File name: {}",
            version, expectedHash, hash, filename);
        return false;
      }

      nextVersion = version;
      version = versionInfo.getPrevVersion();
    }

    return true;
  }

}
