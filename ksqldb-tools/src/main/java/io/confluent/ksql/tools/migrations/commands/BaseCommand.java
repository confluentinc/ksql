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

import static io.confluent.ksql.tools.migrations.commands.InitializeMigrationCommand.INITIALIZE_COMMAND_NAME;

import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.tools.migrations.MigrationConfig;
import io.confluent.ksql.tools.migrations.util.MigrationsUtil;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;

/**
 * Defines common options across all of the migration
 * tool commands.
 */
public abstract class BaseCommand implements Runnable {

  private static final String CONFIG_FILE_OPTION = "--config-file";
  private static final String CONFIG_FILE_OPTION_SHORT = "-c";

  @Option(
      name = {CONFIG_FILE_OPTION_SHORT, CONFIG_FILE_OPTION},
      title = "config-file",
      description = "Path to migrations configuration file. Required for all commands "
          + "with the exception of `" + NewMigrationCommand.NEW_COMMAND_NAME + "`.",
      type = OptionType.GLOBAL
  )
  protected String configFile;

  @Override
  public void run() {
    runCommand();
  }

  /**
   * @return exit status of the command
   */
  public int runCommand() {
    final long startTime = System.nanoTime();
    final int status = command();
    getLogger().info("Execution time: " + (System.nanoTime() - startTime) / 1000000000);
    return status;
  }

  /**
   * @return exit status of the command
   */
  protected abstract int command();

  protected abstract Logger getLogger();

  protected boolean validateConfigFilePresent() {
    if (configFile == null || configFile.equals("")) {
      getLogger().error("Migrations config file required but not specified. "
          + "Specify with {} (or, equivalently, {}).",
          CONFIG_FILE_OPTION, CONFIG_FILE_OPTION_SHORT);
      return false;
    }
    return true;
  }

  protected boolean validateMetadataInitialized(
      final Client ksqlClient,
      final MigrationConfig config
  ) {
    final String streamName = config.getString(MigrationConfig.KSQL_MIGRATIONS_STREAM_NAME);
    final String tableName = config.getString(MigrationConfig.KSQL_MIGRATIONS_TABLE_NAME);
    return describeSource(ksqlClient, streamName, "stream")
        && describeSource(ksqlClient, tableName, "table");
  }

  private boolean describeSource(
      final Client ksqlClient,
      final String sourceName,
      final String type) {
    try {
      ksqlClient.describeSource(sourceName).get();
    } catch (InterruptedException | ExecutionException e) {
      getLogger().error(String.format("Failed to verify existence of migrations metadata %s '%s'. "
          + "Did you run `%s %s`? Error message: %s", type, sourceName,
          MigrationsUtil.MIGRATIONS_COMMAND, INITIALIZE_COMMAND_NAME, e.getMessage()));
      return false;
    }
    return true;
  }
}
