/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.tools.migrations.util;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.tools.migrations.MigrationConfig;
import io.confluent.ksql.tools.migrations.MigrationException;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MigrationsDirectoryUtil {

  public static final String MIGRATIONS_DIR = "migrations";
  public static final String MIGRATIONS_CONFIG_FILE = "ksql-migrations.properties";

  private static final Pattern MIGRATION_FILE_MATCHER = Pattern.compile("V([0-9]{6})__(.+)\\.sql");

  private static final Logger LOGGER = LoggerFactory.getLogger(MigrationsDirectoryUtil.class);

  private MigrationsDirectoryUtil() {
  }

  /**
   * Returns the migrations directory path based on the provided config and
   * config file path. This method does not perform validation on the provided
   * directory location; that responsibility is left to downstream methods.
   */
  public static String getMigrationsDir(
      final String configFilePath,
      final MigrationConfig config
  ) {
    final String migrationsDir = config.getString(MigrationConfig.KSQL_MIGRATIONS_DIR_OVERRIDE);
    if (migrationsDir != null && !migrationsDir.isEmpty()) {
      return migrationsDir;
    } else {
      return getMigrationsDirFromConfigFile(configFilePath);
    }
  }

  /**
   * Returns the migrations directory path assuming the default file structure
   * (as created by the 'ksql-migrations new-project' command). This method should
   * NOT be called directly outside of testing; prefer
   * {@link MigrationsDirectoryUtil#getMigrationsDir(String, MigrationConfig)}
   * instead as users can override the default location returned by this method
   * in their migrations config if they wish.
   */
  @VisibleForTesting
  public static String getMigrationsDirFromConfigFile(final String configFilePath) {
    final Path parentDir = Paths.get(configFilePath).getParent();
    if (parentDir == null) {
      throw new MigrationException("Could not find parent directory for config file '"
          + configFilePath + "': no parent dir exists.");
    }
    return parentDir.resolve(MIGRATIONS_DIR).toString();
  }

  public static String getFilePrefixForVersion(final String version) {
    return "V" + StringUtils.leftPad(version, 6, "0");
  }

  public static Optional<MigrationFile> getMigrationForVersion(
      final String version,
      final String migrationsDir
  ) {
    final String prefix = getFilePrefixForVersion(version);

    final File directory = new File(migrationsDir);
    if (!directory.isDirectory()) {
      throw new MigrationException(migrationsDir + " is not a directory.");
    }

    final String[] names = directory.list();
    if (names == null) {
      throw new MigrationException("Failed to retrieve files from " + migrationsDir);
    }

    final List<MigrationFile> matches = Arrays.stream(names)
        .filter(name -> name.startsWith(prefix))
        .map(name -> getMigrationFromFilename(migrationsDir, name))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toList());
    // throw on multiple matches
    validateMigrationVersionsUnique(matches);

    return matches.size() > 0 ? Optional.of(matches.get(0)) : Optional.empty();
  }

  public static String getFileContentsForName(final String filename) {
    try {
      return new String(Files.readAllBytes(Paths.get(filename)), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new MigrationException(
          String.format("Failed to read %s: %s", filename, e.getMessage()));
    }
  }

  public static String computeHashForFile(final String filename) {
    try {
      final byte[] bytes = Files.readAllBytes(Paths.get(filename));

      final StringBuilder builder = new StringBuilder();
      for (final byte b : MessageDigest.getInstance("MD5").digest(bytes)) {
        builder.append(String.format("%02x", b));
      }

      return builder.toString();
    } catch (NoSuchAlgorithmException | IOException e) {
      throw new MigrationException(String.format(
          "Could not compute hash for file '%s': %s", filename, e.getMessage()));
    }
  }

  /**
   * @return all migration file versions in sorted order
   */
  public static List<Integer> getAllVersions(final String migrationsDir) {
    return getAllMigrations(migrationsDir).stream()
        .map(MigrationFile::getVersion)
        .collect(Collectors.toList());
  }

  /**
   * @return all migration files in sorted order
   */
  public static List<MigrationFile> getAllMigrations(final String migrationsDir) {
    final File directory = new File(migrationsDir);
    if (!directory.isDirectory()) {
      throw new MigrationException(migrationsDir + " is not a directory.");
    }

    final String[] allNames;
    try {
      allNames = directory.list();
    } catch (SecurityException e) {
      throw new MigrationException("Failed to retrieve files from " + migrationsDir
          + ": " + e.getMessage());
    }
    if (allNames == null) {
      throw new MigrationException("Failed to retrieve files from " + migrationsDir);
    }
    final List<String> filenames = Arrays.stream(allNames)
        .sorted()
        .filter(name -> !new File(name).isDirectory())
        .collect(Collectors.toList());

    final List<MigrationFile> migrations = filenames.stream()
        .map(name -> getMigrationFromFilename(migrationsDir, name))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toList());

    validateMigrationVersionsUnique(migrations);

    return migrations;
  }

  private static Optional<MigrationFile> getMigrationFromFilename(
      final String migrationsDir,
      final String filename
  ) {
    final Matcher matcher = MIGRATION_FILE_MATCHER.matcher(filename);
    if (!matcher.find()) {
      LOGGER.warn("Skipping file does not match expected migration file pattern "
          + "'V<six digit number>__<name>.sql': {}", filename);
      return Optional.empty();
    }

    final int version = Integer.parseInt(matcher.group(1));
    if (version <= 0) {
      throw new MigrationException(
          "MigrationFile file versions must be positive. Found: " + filename);
    }

    final String description = matcher.group(2).replace('_', ' ');

    return Optional.of(new MigrationFile(
        version,
        description,
        Paths.get(migrationsDir, filename).toString()
    ));
  }

  private static void validateMigrationVersionsUnique(final List<MigrationFile> migrations) {
    if (migrations.size() == 0) {
      return;
    }

    MigrationFile previous = migrations.get(0);
    for (int i = 1; i < migrations.size(); i++) {
      if (migrations.get(i).getVersion() == previous.getVersion()) {
        throw new MigrationException(String.format(
            "Found multiple migration files with the same version. Version: %d. Files: '%s', '%s'",
            migrations.get(i).getVersion(),
            migrations.get(i).getFilepath(),
            previous.getFilepath()
        ));
      }
      previous = migrations.get(i);
    }
  }
}
