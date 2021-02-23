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
import com.github.rvesse.airline.annotations.help.Discussion;
import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.api.client.BatchedQueryResult;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.tools.migrations.MigrationConfig;
import io.confluent.ksql.tools.migrations.MigrationException;
import io.confluent.ksql.tools.migrations.MigrationsUtil;
import io.confluent.ksql.util.KsqlException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
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

    return command(config, MigrationsUtil::getKsqlClient);
  }

  @VisibleForTesting
  int command(
      final MigrationConfig config,
      final Function<MigrationConfig, Client> clientSupplier
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
      success = validate(config, ksqlClient);
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
  static boolean validate(final MigrationConfig config, final Client ksqlClient) {
    String version = MetadataUtil.getCurrentVersion(config);
    while (!version.equals(NONE_VERSION)) {
      // TODO: combine with util helper to de-dup code?
      final String migrationTableName = config
          .getString(MigrationConfig.KSQL_MIGRATIONS_TABLE_NAME);
      final BatchedQueryResult result = ksqlClient.executeQuery(
          "SELECT checksum, previous FROM " + migrationTableName
              + " WHERE version_key = 'CURRENT';");
      final String expectedHash;
      final String prevVersion;
      try {
        final List<Row> resultRows = result.get();
        if (resultRows.size() == 0) {
          throw new MigrationException(
              "Failed to query state for migration with version " + version
                  + ": no such migration is present in the migrations metadata table");
        }
        expectedHash = resultRows.get(0).getString(0);
        prevVersion = resultRows.get(0).getString(1);
      } catch (InterruptedException | ExecutionException e) {
        throw new MigrationException(String.format(
            "Failed to query state for migration with version %s: %s", version, e.getMessage()));
      }

      final String filename;
      try {
        filename = getFileNameForVersion(version);
      } catch (MigrationException e) {
        LOGGER.error("No migrations file found for version with status {}. Version: {}", MIGRATED,
            version);
        return false;
      }
      final String hash = computeHash(filename);
      if (!expectedHash.equals(hash)) {
        LOGGER.error("Migrations file found for version {} does not match the checksum saved "
                + "for this version. Expected checksum: {}. Actual checksum: {}. File name: {}",
            version, expectedHash, hash, filename);
        return false;
      }

      version = prevVersion;
    }

    return true;
  }

  // TODO: move to util
  public static String computeHash(final String filename) {
    try {
      final byte[] bytes = Files.readAllBytes( // TODO: replace with helper method
          Paths.get(filename));
      return new String(MessageDigest.getInstance("MD5").digest(bytes));
    } catch (NoSuchAlgorithmException e) {
      throw new MigrationException(String.format(
          "Could not compute hash for file '%s': %s", filename, e.getMessage()));
    }
  }
}
