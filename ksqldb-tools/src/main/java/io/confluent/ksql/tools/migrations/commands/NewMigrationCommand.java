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
import com.google.common.collect.ImmutableList;
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
import org.apache.kafka.common.config.types.Password;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Command(
    name = NewMigrationCommand.NEW_COMMAND_NAME,
    description = "Creates a new migrations project directory structure and config file."
)
public class NewMigrationCommand extends BaseCommand {

  static final String NEW_COMMAND_NAME = "new-project";

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
    if (getConfigFile() != null && !getConfigFile().isEmpty()) {
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

    final String initialConfig = createInitialConfig(ksqlServerUrl);
    LOGGER.info("Writing to config file: " + initialConfig);
    try (PrintWriter out = new PrintWriter(path, Charset.defaultCharset().name())) {
      out.println(initialConfig);
    } catch (FileNotFoundException | UnsupportedEncodingException e) {
      LOGGER.error(String.format("Failed to write to config file %s: %s", path, e.getMessage()));
      return false;
    }

    return true;
  }

  private static final List<String> METADATA_CONFIGS = ImmutableList.of(
      MigrationConfig.KSQL_MIGRATIONS_STREAM_NAME,
      MigrationConfig.KSQL_MIGRATIONS_TABLE_NAME,
      MigrationConfig.KSQL_MIGRATIONS_STREAM_TOPIC_NAME,
      MigrationConfig.KSQL_MIGRATIONS_TABLE_TOPIC_NAME,
      MigrationConfig.KSQL_MIGRATIONS_TOPIC_REPLICAS
  );

  private static final List<String> TLS_CONFIGS = ImmutableList.of(
      MigrationConfig.SSL_TRUSTSTORE_LOCATION,
      MigrationConfig.SSL_TRUSTSTORE_PASSWORD,
      MigrationConfig.SSL_KEYSTORE_LOCATION,
      MigrationConfig.SSL_KEYSTORE_PASSWORD,
      MigrationConfig.SSL_KEY_PASSWORD,
      MigrationConfig.SSL_KEY_ALIAS,
      MigrationConfig.SSL_ALPN,
      MigrationConfig.SSL_VERIFY_HOST
  );

  private static final List<String> SERVER_AUTH_CONFIGS = ImmutableList.of(
      MigrationConfig.KSQL_BASIC_AUTH_USERNAME,
      MigrationConfig.KSQL_BASIC_AUTH_PASSWORD
  );

  private static final List<String> MIGRATIONS_STRUCTURE_CONFIGS = ImmutableList.of(
      MigrationConfig.KSQL_MIGRATIONS_DIR_OVERRIDE
  );

  private static String createInitialConfig(final String ksqlServerUrl) {
    final StringBuilder builder = new StringBuilder();

    builder.append(MigrationConfig.KSQL_SERVER_URL + "=" + ksqlServerUrl);

    appendConfigs(builder, "Migrations metadata configs", METADATA_CONFIGS);
    appendConfigs(builder, "TLS configs", TLS_CONFIGS);
    appendConfigs(builder, "ksqlDB server authentication configs", SERVER_AUTH_CONFIGS);
    appendConfigs(builder, "Migrations directory configs", MIGRATIONS_STRUCTURE_CONFIGS);

    return builder.toString();
  }

  private static void appendConfigs(
      final StringBuilder builder,
      final String headerText,
      final List<String> configs
  ) {
    builder.append("\n\n# ");
    builder.append(headerText);
    builder.append(":");

    for (final String cfg : configs) {
      final Object value = MigrationConfig.DEFAULT_CONFIG.values().get(cfg);
      builder.append("\n# ");
      builder.append(cfg);
      builder.append("=");
      builder.append((value == null || value instanceof Password) ? "" : value);
    }
  }
}
