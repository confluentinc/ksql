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

import static io.confluent.ksql.tools.migrations.util.MigrationsDirectoryUtil.MIGRATIONS_CONFIG_FILE;
import static io.confluent.ksql.tools.migrations.util.MigrationsDirectoryUtil.MIGRATIONS_DIR;

import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.restrictions.Required;
import io.confluent.ksql.tools.migrations.MigrationConfig;
import io.confluent.ksql.tools.migrations.util.MigrationsUtil;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Command(
    name = NewMigrationCommand.NEW_COMMAND_NAME,
    description = "Creates a new migrations project directory structure and config file."
)
public class NewMigrationCommand extends BaseCommand {

  static final String NEW_COMMAND_NAME = "new";

  private static final Logger LOGGER = LoggerFactory.getLogger(NewMigrationCommand.class);

  @Required
  @Arguments(
      description = "project-path: the path that will be used as the root directory for "
          + "this migrations project.\n"
          + "ksql-server-url: the address of the ksqlDB server to connect to.",
      title = {"project-path", "ksql-server-url"})
  private List<String> args;

  @Override
  protected int command() {
    if (configFile != null && !configFile.equals("")) {
      LOGGER.error("This command does not expect a config file to be passed. "
          + "Rather, this command will create one as part of preparing the migrations directory.");
      return 1;
    }
    if (args.size() != 2) {
      LOGGER.error(
          "Unexpected number of arguments to `{} {}`. Expected: 2. Got: {}. "
              + "See `{} help {}` for usage.",
          MigrationsUtil.MIGRATIONS_COMMAND, NEW_COMMAND_NAME, args.size(),
          MigrationsUtil.MIGRATIONS_COMMAND, NEW_COMMAND_NAME);
      return 1;
    }

    final String projectPath = args.get(0);
    final String ksqlServerUrl = args.get(1);
    LOGGER.info("Creating new migrations project at {}", projectPath);
    if (tryCreateDirectory(projectPath)
        && tryCreateDirectory(Paths.get(projectPath, MIGRATIONS_DIR).toString())
        && tryCreatePropertiesFile(
            Paths.get(projectPath, MIGRATIONS_CONFIG_FILE).toString(),
            ksqlServerUrl)
    ) {
      LOGGER.info("Migrations project directory created successfully");
      return 0;
    } else {
      return 1;
    }
  }

  @Override
  protected Logger getLogger() {
    return LOGGER;
  }

  private boolean tryCreateDirectory(final String path) {
    final File directory = new File(path);

    if (directory.exists() && directory.isDirectory()) {
      LOGGER.warn(path + " already exists. Skipping directory creation.");
      return true;
    } else if (directory.exists() && !directory.isDirectory()) {
      LOGGER.error(path + " already exists as a file. Cannot create directory.");
      return false;
    }

    try {
      LOGGER.info("Creating directory: " + path);
      Files.createDirectories(Paths.get(path));
    } catch (FileSystemException e) {
      LOGGER.error("Permission denied: create directory " + path);
      return false;
    } catch (IOException e) {
      LOGGER.error(String.format("Failed to create directory %s: %s", path, e.getMessage()));
      return false;
    }
    return true;
  }

  private boolean tryCreatePropertiesFile(final String path, final String ksqlServerUrl) {
    try {
      final File file = new File(path);
      if (!file.exists()) {
        LOGGER.info("Creating file: " + path);
      }
      if (!file.createNewFile()) {
        LOGGER.error("Failed to create file. File already exists: {}", path);
        return false;
      }
    } catch (IOException e) {
      LOGGER.error(String.format("Failed to create file %s: %s", path, e.getMessage()));
      return false;
    }

    final String initialConfig = MigrationConfig.KSQL_SERVER_URL + "=" + ksqlServerUrl;
    LOGGER.info("Writing to config file: " + initialConfig);
    try (PrintWriter out = new PrintWriter(path, Charset.defaultCharset().name())) {
      out.println(initialConfig);
    } catch (FileNotFoundException | UnsupportedEncodingException e) {
      LOGGER.error(String.format("Failed to write to config file %s: %s", path, e.getMessage()));
      return false;
    }

    return true;
  }
}
