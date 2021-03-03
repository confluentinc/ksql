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

import static io.confluent.ksql.tools.migrations.util.MigrationsDirectoryUtil.getAllVersions;
import static io.confluent.ksql.tools.migrations.util.MigrationsDirectoryUtil.getFilePrefixForVersion;
import static io.confluent.ksql.tools.migrations.util.MigrationsDirectoryUtil.getMigrationForVersion;
import static io.confluent.ksql.tools.migrations.util.MigrationsDirectoryUtil.getMigrationsDirFromConfigFile;

import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.help.Examples;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.tools.migrations.MigrationException;
import io.confluent.ksql.tools.migrations.util.MigrationFile;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Command(
    name = "create",
    description = "Create a blank migration file with <description> as description, which "
        + "can then be edited and applied as the next schema version."
)
@Examples(
    examples = "$ ksql-migrations create Add_users",
    descriptions = "Creates a new migrations file for adding a users table to ksqlDB "
        + "(e.g. V000002__Add_users.sql)"
)
public class CreateMigrationCommand extends BaseCommand {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreateMigrationCommand.class);

  private static final String INVALID_FILENAME_CHARS_PATTERN = "\\s|/|\\\\|:|\\*|\\?|\"|<|>|\\|";

  @Option(
      name = {"-v", "--version"},
      description = "the schema version to initialize, defaults to the next"
          + " schema version based on existing migration files."
  )
  private int version;

  @Required
  @Arguments(
      title = "description",
      description = "The description for the migration."
  )
  private String description;

  @Override
  protected int command() {
    return command(getMigrationsDirFromConfigFile(configFile));
  }

  @VisibleForTesting
  int command(final String migrationsDir) {
    if (!validateVersionDoesNotAlreadyExist(migrationsDir) || !validateDescriptionNotEmpty()) {
      return 1;
    }

    try {
      final int newVersion = version != 0 ? version : getLatestVersion(migrationsDir) + 1;
      createMigrationsFile(newVersion, migrationsDir);
    } catch (MigrationException e) {
      LOGGER.error(e.getMessage());
      return 1;
    }

    return 0;
  }

  @Override
  protected Logger getLogger() {
    return LOGGER;
  }

  /**
   * @return true if validation succeeds, else false
   */
  private boolean validateVersionDoesNotAlreadyExist(final String migrationsDir) {
    // no explicit version was specified, nothing to verify
    // (airline actually can't distinguish between explicit 0 and nothing specified,
    // but we won't worry about this edge case for now)
    if (version == 0) {
      return true;
    }

    final Optional<MigrationFile> existingMigration;
    try {
      existingMigration = getMigrationForVersion(String.valueOf(version), migrationsDir);
    } catch (MigrationException e) {
      LOGGER.error(e.getMessage());
      return false;
    }

    if (existingMigration.isPresent()) {
      LOGGER.error("Found existing migrations file for version {}: {}",
          version, existingMigration.get().getFilepath());
      return false;
    }

    return true;
  }

  /**
   * @return true if validation succeeds, else false
   */
  private boolean validateDescriptionNotEmpty() {
    if (description.isEmpty()) {
      LOGGER.error("Description cannot be empty.");
      return false;
    }
    return true;
  }

  private void createMigrationsFile(final int newVersion, final String migrationsDir) {
    if (newVersion <= 0) {
      throw new MigrationException("Invalid version file version: " + newVersion
          + ". Version must be a positive integer.");
    }
    if (newVersion > 999999) {
      throw new MigrationException("Invalid version file version: " + newVersion
          + ". Version must fit into a six-digit integer.");
    }

    final String filename = getNewFileName(newVersion, description);
    final String filePath = Paths.get(migrationsDir, filename).toString();
    try {
      LOGGER.info("Creating file: " + filePath);
      final boolean result = new File(filePath).createNewFile();
      if (!result) {
        throw new IllegalStateException("File should not exist");
      }
    } catch (IOException | IllegalStateException e) {
      throw new MigrationException(String.format(
          "Failed to create file %s: %s", filePath, e.getMessage()));
    }
  }

  private static int getLatestVersion(final String migrationsDir) {
    final List<Integer> allVersions = getAllVersions(migrationsDir);
    return allVersions.size() != 0 ? allVersions.get(allVersions.size() - 1) : 0;
  }

  private static String getNewFileName(final int newVersion, final String description) {
    final String versionPrefix = getFilePrefixForVersion(String.valueOf(newVersion));
    final String descriptionSuffix = description.replaceAll(INVALID_FILENAME_CHARS_PATTERN, "_");
    return versionPrefix + "__" + descriptionSuffix + ".sql";
  }
}
