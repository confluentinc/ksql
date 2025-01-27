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

import com.github.rvesse.airline.HelpOption;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.github.rvesse.airline.annotations.restrictions.MutuallyExclusiveWith;
import com.github.rvesse.airline.annotations.restrictions.Once;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.tools.migrations.MigrationConfig;
import io.confluent.ksql.tools.migrations.util.MigrationsUtil;
import java.util.concurrent.ExecutionException;
import javax.inject.Inject;
import org.slf4j.Logger;

/**
 * Defines common options across all of the migration
 * tool commands.
 */
public abstract class BaseCommand implements Runnable {

  private static final String CONFIG_FILE_OPTION = "--config-file";
  private static final String CONFIG_FILE_OPTION_SHORT = "-c";

  @Inject
  protected HelpOption<BaseCommand> help;

  @Option(
      name = {CONFIG_FILE_OPTION_SHORT, CONFIG_FILE_OPTION},
      title = "config-file",
      description = "Path to migrations configuration file. Required for all commands "
          + "with the exception of `" + NewMigrationCommand.NEW_COMMAND_NAME + "`.",
      // allows users to specify config file before the name of the command
      type = OptionType.GLOBAL
  )
  @MutuallyExclusiveWith(tag = "config")
  @Once
  protected String configFileGlobal;

  @Option(
      name = {CONFIG_FILE_OPTION_SHORT, CONFIG_FILE_OPTION},
      title = "config-file",
      description = "Path to migrations configuration file. Required for all commands "
          + "with the exception of `" + NewMigrationCommand.NEW_COMMAND_NAME + "`.",
      // allows users to specify config file after the name of the command
      type = OptionType.COMMAND,
      // hide this version of the config file option so only the version above appears
      // in help text, to avoid duplication
      hidden = true
  )
  @MutuallyExclusiveWith(tag = "config")
  @Once
  protected String configFileNonGlobal;

  @Override
  public void run() {
    runCommand();
  }

  /**
   * @return exit status of the command
   */
  public int runCommand() {
    if (help.showHelpIfRequested()) {
      return 0;
    }

    final long startTime = System.currentTimeMillis();
    final int status = command();
    getLogger().info(String.format("Execution time: %.4f seconds",
        (System.currentTimeMillis() - startTime) / 1000.));
    return status;
  }

  /**
   * @return exit status of the command
   */
  protected abstract int command();

  protected abstract Logger getLogger();

  protected String getConfigFile() {
    if (configFileGlobal != null) {
      return configFileGlobal;
    }
    if (configFileNonGlobal != null) {
      return configFileNonGlobal;
    }
    return null;
  }

  protected boolean validateConfigFilePresent() {
    if (getConfigFile() == null || getConfigFile().trim().isEmpty()) {
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
