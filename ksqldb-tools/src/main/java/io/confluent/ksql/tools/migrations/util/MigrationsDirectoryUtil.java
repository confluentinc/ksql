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

import io.confluent.ksql.tools.migrations.Migration;
import io.confluent.ksql.tools.migrations.MigrationException;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

public final class MigrationsDirectoryUtil {

  public static final String MIGRATIONS_DIR = "migrations";
  public static final String MIGRATIONS_CONFIG_FILE = "ksql-migrations.properties";

  private MigrationsDirectoryUtil() {
  }

  public static String getMigrationsDirFromConfigFile(final String configFilePath) {
    final Path parentDir = Paths.get(configFilePath).getParent();
    if (parentDir == null) {
      throw new MigrationException("Could not find parent directory for config file '"
          + configFilePath + "': no parent dir exists.");
    }
    return parentDir.resolve(MIGRATIONS_DIR).toString();
  }

  public static Optional<String> getFilePathForVersion(
      final String version,
      final String migrationsDir
  ) {
    final String prefix = "V" + StringUtils.leftPad(version, 6, "0");

    final File directory = new File(migrationsDir);
    if (!directory.isDirectory()) {
      throw new MigrationException(migrationsDir + " is not a directory.");
    }

    final String[] names = directory.list();
    if (names == null) {
      throw new MigrationException("Failed to retrieve files from " + migrationsDir);
    }

    final List<String> matches = Arrays.stream(names)
        .filter(name -> name.startsWith(prefix))
        .collect(Collectors.toList());
    if (matches.size() == 1) {
      return Optional.of(Paths.get(migrationsDir, matches.get(0)).toString());
    } else if (matches.size() == 0) {
      return Optional.empty();
    } else {
      throw new MigrationException("Found multiple migration files for version " + version);
    }
  }

  public static String getFileContentsForVersion(final String version, final String migrationsDir) {
    final Optional<String> filename = getFilePathForVersion(version, migrationsDir);
    if (!filename.isPresent()) {
      throw new MigrationException("Cannot find migration file with version "
          + version + " in " + migrationsDir);
    }
    try {
      return new String(Files.readAllBytes(Paths.get(filename.get())), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new MigrationException(
          String.format("Failed to read %s: %s", filename.get(), e.getMessage()));
    }
  }

  public static String computeHashForFile(final String filename) {
    try {
      final byte[] bytes = Files.readAllBytes(Paths.get(filename));
      return new String(MessageDigest.getInstance("MD5").digest(bytes), StandardCharsets.UTF_8);
    } catch (NoSuchAlgorithmException | IOException e) {
      throw new MigrationException(String.format(
          "Could not compute hash for file '%s': %s", filename, e.getMessage()));
    }
  }

  public static String getNameFromMigrationFilePath(final String filename) {
    return filename
        .substring(filename.indexOf("__") + 2, filename.indexOf(".sql"))
        .replace('_', ' ');
  }

  public static List<Migration> getAllMigrations(
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

  public static List<Migration> getAllMigrations(
      final int start,
      final String migrationsDir
  ) {
    int end = start;
    while (getFilePathForVersion(Integer.toString(end), migrationsDir).isPresent()) {
      end++;
    }
    return getAllMigrations(start, end - 1, migrationsDir);
  }

  private static Migration loadMigration(final int version, final String migrationsDir) {
    final String versionString = Integer.toString(version);

    final Optional<String> migrationFilePath =
        MigrationsDirectoryUtil.getFilePathForVersion(versionString, migrationsDir);
    if (!migrationFilePath.isPresent()) {
      throw new MigrationException("Failed to find file for version " + versionString);
    }

    final String migrationCommand =
        MigrationsDirectoryUtil.getFileContentsForVersion(versionString, migrationsDir);

    return new Migration(
        version,
        MigrationsDirectoryUtil.getNameFromMigrationFilePath(migrationFilePath.get()),
        MigrationsDirectoryUtil.computeHashForFile(migrationFilePath.get()),
        migrationCommand
    );
  }
}
