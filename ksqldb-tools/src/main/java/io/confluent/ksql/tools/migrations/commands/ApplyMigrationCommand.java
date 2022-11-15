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

import static io.confluent.ksql.tools.migrations.util.CommandParser.preserveCase;
import static io.confluent.ksql.tools.migrations.util.MigrationsDirectoryUtil.getAllMigrations;
import static io.confluent.ksql.tools.migrations.util.MigrationsDirectoryUtil.getMigrationForVersion;
import static io.confluent.ksql.tools.migrations.util.MigrationsDirectoryUtil.getMigrationsDir;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Once;
import com.github.rvesse.airline.annotations.restrictions.RequireOnlyOne;
import com.github.rvesse.airline.annotations.restrictions.ranges.IntegerRange;
import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.FieldInfo;
import io.confluent.ksql.api.client.KsqlObject;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.parser.VariableParser;
import io.confluent.ksql.tools.migrations.MigrationConfig;
import io.confluent.ksql.tools.migrations.MigrationException;
import io.confluent.ksql.tools.migrations.util.CommandParser;
import io.confluent.ksql.tools.migrations.util.CommandParser.SqlCommand;
import io.confluent.ksql.tools.migrations.util.CommandParser.SqlCreateConnectorStatement;
import io.confluent.ksql.tools.migrations.util.CommandParser.SqlDefineVariableCommand;
import io.confluent.ksql.tools.migrations.util.CommandParser.SqlDropConnectorStatement;
import io.confluent.ksql.tools.migrations.util.CommandParser.SqlInsertValues;
import io.confluent.ksql.tools.migrations.util.CommandParser.SqlPropertyCommand;
import io.confluent.ksql.tools.migrations.util.CommandParser.SqlStatement;
import io.confluent.ksql.tools.migrations.util.CommandParser.SqlUndefineVariableCommand;
import io.confluent.ksql.tools.migrations.util.MetadataUtil;
import io.confluent.ksql.tools.migrations.util.MetadataUtil.MigrationState;
import io.confluent.ksql.tools.migrations.util.MigrationFile;
import io.confluent.ksql.tools.migrations.util.MigrationsDirectoryUtil;
import io.confluent.ksql.tools.migrations.util.MigrationsUtil;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.RetryUtil;
import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Command(
    name = "apply",
    description = "Migrates the metadata schema to a new schema version."
)
public class ApplyMigrationCommand extends BaseCommand {

  private static final Logger LOGGER = LoggerFactory.getLogger(ApplyMigrationCommand.class);

  private static final int MAX_RETRIES = 10;

  @Option(
      title = "all",
      name = {"-a", "--all"},
      description = "Run all available migrations"
  )
  @RequireOnlyOne(tag = "target")
  @Once
  private boolean all;

  @Option(
      title = "next",
      name = {"-n", "--next"},
      description = "Run the next available migration version"
  )
  @RequireOnlyOne(tag = "target")
  @Once
  private boolean next;

  @Option(
      title = "untilVersion",
      name = {"-u", "--until"},
      arity = 1,
      description = "Run all available migrations up through the specified version"
  )
  @RequireOnlyOne(tag = "target")
  @IntegerRange(min = 1, max = 999999)
  @Once
  private int untilVersion;

  @Option(
      title = "version",
      name = {"-v", "--version"},
      arity = 1,
      description = "Run the migration with the specified version"
  )
  @RequireOnlyOne(tag = "target")
  @IntegerRange(min = 1, max = 999999)
  @Once
  private int version;

  @Option(
      name = {"--dry-run"},
      title = "dry-run",
      description = "Dry run the current command. No ksqlDB statements will be "
          + "sent to the ksqlDB server. Note that this dry run is for purposes of "
          + "displaying which migration files (and what ksqlDB statements) the command "
          + "would run in non-dry-run mode, and does NOT attempt to validate whether "
          + "the ksqlDB statements will be accepted by the ksqlDB server."
  )
  @Once
  private boolean dryRun = false;

  @Option(
      name = {"--define", "-d"},
      description = "Define variables for the session. This is equivalent to including DEFINE "
          + "statements before each migration. The `--define` option should be followed by a "
          + "string of the form `name=value` and may be passed any number of times."
  )
  private List<String> definedVars = null;

  @Option(
      name = {"--headers"},
      description = "Path to custom request headers file. These headers will be sent with all "
          + "requests to the ksqlDB server as part of applying these migrations."
  )
  @Once
  private String headersFile;

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
        getMigrationsDir(getConfigFile(), config),
        Clock.systemDefaultZone()
    );
  }

  // CHECKSTYLE_RULES.OFF: NPathComplexity
  @VisibleForTesting
  int command(
      final MigrationConfig config,
      final BiFunction<MigrationConfig, String, Client> clientSupplier,
      final String migrationsDir,
      final Clock clock
  ) {
    // CHECKSTYLE_RULES.ON: NPathComplexity
    final Client ksqlClient;
    try {
      ksqlClient = clientSupplier.apply(config, headersFile);
    } catch (MigrationException e) {
      LOGGER.error(e.getMessage());
      return 1;
    }

    if (!validateMetadataInitialized(ksqlClient, config)) {
      ksqlClient.close();
      return 1;
    }

    if (dryRun) {
      LOGGER.info("This is a dry run. No ksqlDB statements will be submitted "
          + "to the ksqlDB server.");
    }

    boolean success;
    try {
      success = validateCurrentState(config, ksqlClient, migrationsDir)
          && apply(config, ksqlClient, migrationsDir, clock);
    } catch (MigrationException e) {
      LOGGER.error(e.getMessage());
      success = false;
    } finally {
      ksqlClient.close();
    }

    return success ? 0 : 1;
  }

  private boolean apply(
      final MigrationConfig config,
      final Client ksqlClient,
      final String migrationsDir,
      final Clock clock
  ) {
    String previous = MetadataUtil.getLatestMigratedVersion(config, ksqlClient);

    LOGGER.info("Loading migration files");
    final List<MigrationFile> migrations;
    try {
      migrations = loadMigrationsToApply(migrationsDir, previous);
    } catch (MigrationException e) {
      LOGGER.error(e.getMessage());
      return false;
    }

    if (migrations.size() == 0) {
      LOGGER.info("No eligible migrations found.");
    } else {
      LOGGER.info(migrations.size() + " migration file(s) loaded.");
    }

    for (MigrationFile migration : migrations) {
      if (!applyMigration(config, ksqlClient, migration, clock, previous)) {
        return false;
      }
      previous = Integer.toString(migration.getVersion());
    }

    return true;
  }

  private List<MigrationFile> loadMigrationsToApply(
      final String migrationsDir,
      final String previousVersion
  ) {
    final int minimumVersion = previousVersion.equals(MetadataUtil.NONE_VERSION)
        ? 1
        : Integer.parseInt(previousVersion) + 1;
    if (version > 0) {
      final Optional<MigrationFile> migration =
          getMigrationForVersion(String.valueOf(version), migrationsDir);
      if (!migration.isPresent()) {
        throw new MigrationException("No migration file with version " + version + " exists.");
      }
      if (version < minimumVersion) {
        throw new MigrationException(
            "Version must be newer than the last version migrated. Last version migrated was "
                + previousVersion);
      }
      return Collections.singletonList(migration.get());
    }

    final List<MigrationFile> migrations = getAllMigrations(migrationsDir).stream()
        .filter(migration -> {
          if (migration.getVersion() < minimumVersion) {
            return false;
          }
          if (untilVersion > 0) {
            return migration.getVersion() <= untilVersion;
          } else {
            return true;
          }
        })
        .collect(Collectors.toList());

    if (next) {
      if (migrations.size() == 0) {
        throw new MigrationException("No eligible migrations found.");
      }
      return Collections.singletonList(migrations.get(0));
    }

    return migrations;
  }

  private boolean applyMigration(
      final MigrationConfig config,
      final Client ksqlClient,
      final MigrationFile migration,
      final Clock clock,
      final String previous
  ) {
    LOGGER.info("Applying migration version {}: {}", migration.getVersion(), migration.getName());
    final String migrationFileContent =
        MigrationsDirectoryUtil.getFileContentsForName(migration.getFilepath());
    LOGGER.info("{} contents:\n{}", migration.getFilepath(), migrationFileContent);

    if (dryRun) {
      LOGGER.info("Dry run complete. No migrations were actually applied.");
      return true;
    }

    if (!verifyMigrated(config, ksqlClient, previous, MAX_RETRIES)) {
      LOGGER.error("Failed to verify status of version " + previous);
      return false;
    }

    final String executionStart = Long.toString(clock.millis());

    if (
        !updateState(config, ksqlClient, MigrationState.RUNNING,
            executionStart, migration, clock, previous, Optional.empty())
    ) {
      return false;
    }

    try {
      executeCommands(migrationFileContent, ksqlClient, config,
          executionStart, migration, clock, previous);
    } catch (MigrationException e) {
      LOGGER.error(e.getMessage());
      return false;
    }

    if (!updateState(config, ksqlClient, MigrationState.MIGRATED,
        executionStart, migration, clock, previous, Optional.empty())) {
      return false;
    }
    LOGGER.info("Successfully migrated");
    return true;
  }

  private void executeCommands(
      final String migrationFileContent,
      final Client ksqlClient,
      final MigrationConfig config,
      final String executionStart,
      final MigrationFile migration,
      final Clock clock,
      final String previous
  ) {
    final List<String> commands = CommandParser.splitSql(migrationFileContent);

    executeCommands(
        commands, ksqlClient, config, executionStart,
        migration, clock, previous, true);
    executeCommands(
        commands, ksqlClient, config, executionStart,
        migration, clock, previous, false);
  }

  /**
   * If validateOnly is set to true, then this parses each of the commands but only executes
   * DEFINE/UNDEFINE commands (variables are needed for parsing INSERT INTO... VALUES, SET/UNSET
   * and DEFINE commands). If validateOnly is set to false, then each command will execute after
   * parsing.
   */
  private void executeCommands(
      final List<String> commands,
      final Client ksqlClient,
      final MigrationConfig config,
      final String executionStart,
      final MigrationFile migration,
      final Clock clock,
      final String previous,
      final boolean validateOnly
  ) {
    setUpJavaClientVariables(ksqlClient);
    final Map<String, Object> properties = new HashMap<>();
    for (final String command : commands) {
      try {
        final Map<String, String> variables = ksqlClient.getVariables().entrySet()
            .stream().collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().toString()));
        executeCommand(
            CommandParser.transformToSqlCommand(command, variables),
            ksqlClient,
            properties,
            validateOnly
        );
      } catch (InterruptedException | ExecutionException | MigrationException e) {
        final String action = validateOnly ? "parse" : "execute";
        final String errorMsg = String.format(
            "Failed to %s sql: %s. Error: %s", action, command, e.getMessage());
        updateState(config, ksqlClient, MigrationState.ERROR,
            executionStart, migration, clock, previous, Optional.of(errorMsg));
        throw new MigrationException(errorMsg);
      }
    }
  }

  private void setUpJavaClientVariables(final Client ksqlClient) {
    ksqlClient.getVariables().forEach((k, v) -> ksqlClient.undefine(k));
    try {
      VariableParser.getVariables(definedVars).forEach((k, v) -> ksqlClient.define(k, v));
    } catch (IllegalArgumentException e) {
      throw new MigrationException(e.getMessage());
    }
  }

  private void executeCommand(
      final SqlCommand command,
      final Client ksqlClient,
      final Map<String, Object> properties,
      final boolean defineUndefineOnly
  ) throws ExecutionException, InterruptedException {
    if (command instanceof SqlDefineVariableCommand) {
      ksqlClient.define(
          ((SqlDefineVariableCommand) command).getVariable(),
          ((SqlDefineVariableCommand) command).getValue()
      );
    } else if (command instanceof SqlUndefineVariableCommand) {
      ksqlClient.undefine(((SqlUndefineVariableCommand) command).getVariable());
    } else if (!defineUndefineOnly) {
      executeNonVariableCommands(command, ksqlClient, properties);
    }
  }

  /**
   * Executes everything besides define/undefine commands
   */
  private void executeNonVariableCommands(
      final SqlCommand command,
      final Client ksqlClient,
      final Map<String, Object> properties
  ) throws ExecutionException, InterruptedException {
    if (command instanceof SqlStatement) {
      ksqlClient.executeStatement(command.getCommand(), new HashMap<>(properties)).get();
    } else if (command instanceof SqlInsertValues) {
      final List<FieldInfo> fields =
          ksqlClient.describeSource(((SqlInsertValues) command).getSourceName()).get().fields();
      ksqlClient.insertInto(
          ((SqlInsertValues) command).getSourceName(),
          getRow(
              fields,
              ((SqlInsertValues) command).getColumns(),
              ((SqlInsertValues) command).getValues())).get();
    } else if (command instanceof SqlCreateConnectorStatement) {
      ksqlClient.createConnector(
          ((SqlCreateConnectorStatement) command).getName(),
          ((SqlCreateConnectorStatement) command).isSource(),
          ((SqlCreateConnectorStatement) command).getProperties(),
          ((SqlCreateConnectorStatement) command).getIfNotExists()
      ).get();
    } else if (command instanceof SqlDropConnectorStatement) {
      ksqlClient.dropConnector(
          ((SqlDropConnectorStatement) command).getName(),
          ((SqlDropConnectorStatement) command).getIfExists()
      ).get();
    } else if (command instanceof SqlPropertyCommand) {
      if (((SqlPropertyCommand) command).isSetCommand()
          && ((SqlPropertyCommand) command).getValue().isPresent()) {
        properties.put(
            ((SqlPropertyCommand) command).getProperty(),
            ((SqlPropertyCommand) command).getValue().get()
        );
      } else {
        properties.remove(((SqlPropertyCommand) command).getProperty());
      }
    }
  }

  private static KsqlObject getRow(
      final List<FieldInfo> sourceFields,
      final List<String> insertColumns,
      final List<Expression> insertValues
  ) {
    final Map<String, Object> row = new HashMap<>();
    if (insertColumns.size() > 0) {
      verifyColumnValuesMatch(insertColumns, insertValues);
      for (int i = 0 ; i < insertColumns.size(); i++) {
        row.put(
            preserveCase(insertColumns.get(i)),
            CommandParser.toFieldType(insertValues.get(i)));
      }
    } else {
      final List<String> columnNames = sourceFields.stream()
          .map(FieldInfo::name).collect(Collectors.toList());
      verifyColumnValuesMatch(columnNames, insertValues);
      for (int i = 0 ; i < sourceFields.size(); i++) {
        row.put(
            preserveCase(sourceFields.get(i).name()),
            CommandParser.toFieldType(insertValues.get(i)));
      }
    }

    return new KsqlObject(row);
  }

  private static void verifyColumnValuesMatch(
      final List<String> columns,
      final List<Expression> values
  ) {
    if (columns.size() != values.size()) {
      throw new MigrationException(String.format("Invalid `INSERT VALUES` statement. Number of "
          + "columns and values must match. Got: Columns: %d. Values: %d.",
          columns.size(), values.size()));
    }
  }

  private static boolean verifyMigrated(
      final MigrationConfig config,
      final Client ksqlClient,
      final String version,
      final int retries
  ) {
    if (version.equals(MetadataUtil.NONE_VERSION)) {
      return true;
    }
    try {
      RetryUtil.retryWithBackoff(
          retries,
          1000,
          1000,
          () -> {
            final MigrationState state = MetadataUtil
                .getInfoForVersion(version, config, ksqlClient)
                .getState();
            if (!state.equals(MigrationState.MIGRATED)) {
              throw new MigrationException(
                  String.format("Expected status MIGRATED for version %s. Got %s", version, state));
            }
          }
      );
    } catch (MigrationException e) {
      LOGGER.error(e.getMessage());
      return false;
    }
    return true;
  }

  private static boolean updateState(
      final MigrationConfig config,
      final Client ksqlClient,
      final MigrationState state,
      final String executionStart,
      final MigrationFile migration,
      final Clock clock,
      final String previous,
      final Optional<String> errorReason
  ) {
    final String executionEnd = (state == MigrationState.MIGRATED || state == MigrationState.ERROR)
        ? Long.toString(clock.millis())
        : "";
    final String checksum = MigrationsDirectoryUtil.computeHashForFile(migration.getFilepath());
    try {
      MetadataUtil.writeRow(
          config,
          ksqlClient,
          MetadataUtil.CURRENT_VERSION_KEY,
          state.toString(),
          executionStart,
          executionEnd,
          migration,
          previous,
          checksum,
          errorReason
      ).get();
      MetadataUtil.writeRow(
          config,
          ksqlClient,
          Integer.toString(migration.getVersion()),
          state.toString(),
          executionStart,
          executionEnd,
          migration,
          previous,
          checksum,
          errorReason
      ).get();
      return true;
    } catch (InterruptedException | ExecutionException e) {
      LOGGER.error(e.getMessage());
      return false;
    }
  }

  @Override
  protected Logger getLogger() {
    return LOGGER;
  }

  private static boolean validateCurrentState(
      final MigrationConfig config,
      final Client ksqlClient,
      final String migrationsDir
  ) {
    LOGGER.info("Validating current migration state before applying new migrations");
    return ValidateMigrationsCommand.validate(config, migrationsDir, ksqlClient);
  }
}
