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

package io.confluent.ksql.tools.migrations;

import com.github.rvesse.airline.Cli;
import com.github.rvesse.airline.annotations.Parser;
import com.github.rvesse.airline.help.Help;
import com.github.rvesse.airline.help.cli.CliGlobalUsageGenerator;
import com.github.rvesse.airline.model.OptionMetadata;
import com.github.rvesse.airline.parser.ParseResult;
import com.github.rvesse.airline.parser.ParseState;
import com.github.rvesse.airline.parser.errors.ParseCommandMissingException;
import com.github.rvesse.airline.parser.errors.ParseCommandUnrecognizedException;
import com.github.rvesse.airline.parser.errors.ParseException;
import com.github.rvesse.airline.parser.errors.handlers.CollectAll;
import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.tools.migrations.commands.ApplyMigrationCommand;
import io.confluent.ksql.tools.migrations.commands.BaseCommand;
import io.confluent.ksql.tools.migrations.commands.CreateMigrationCommand;
import io.confluent.ksql.tools.migrations.commands.DestroyMigrationsCommand;
import io.confluent.ksql.tools.migrations.commands.InitializeMigrationCommand;
import io.confluent.ksql.tools.migrations.commands.MigrationInfoCommand;
import io.confluent.ksql.tools.migrations.commands.NewMigrationCommand;
import io.confluent.ksql.tools.migrations.commands.ValidateMigrationsCommand;
import io.confluent.ksql.tools.migrations.util.MigrationsUtil;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is the entrypoint to all migration-related tooling. This
 * tooling is packaged with most ksqlDB distributions. For a full description
 * of the available functionality, simply run the main method below, which defaults
 * to the "help" message.
 */
@com.github.rvesse.airline.annotations.Cli(
    name = MigrationsUtil.MIGRATIONS_COMMAND,
    description = "This tool provides easy and automated schema migrations for "
        + "ksqlDB environments. This allows control over ksqlDB schemas, recreate schemas "
        + "from scratch and migrations for current schemas to newer versions.",
    commands = {
        NewMigrationCommand.class,
        CreateMigrationCommand.class,
        ApplyMigrationCommand.class,
        MigrationInfoCommand.class,
        DestroyMigrationsCommand.class,
        ValidateMigrationsCommand.class,
        InitializeMigrationCommand.class,
        Help.class
    },
    parserConfiguration = @Parser(errorHandler = CollectAll.class)
)
public final class Migrations {

  private static final Logger LOGGER = LoggerFactory.getLogger(Migrations.class);

  private Migrations() {

  }

  public static void main(final String[] args) {
    // even though all migrations commands implement BaseCommand, the Help
    // command does not so we infer the type as Runnable instead
    final Cli<Runnable> cli = new Cli<>(Migrations.class);

    final Optional<Runnable> command;
    try {
      command = parseCommandFromArgs(cli, args);
    } catch (MigrationException e) {
      LOGGER.error(e.getMessage());
      System.exit(1);
      return;
    }

    if (!command.isPresent()) {
      System.exit(0);
      return;
    }

    if (command.get() instanceof Help) {
      command.get().run();
      System.exit(0);
      return;
    }

    System.exit(((BaseCommand) command.get()).runCommand());
  }

  @VisibleForTesting
  static Optional<Runnable> parseCommandFromArgs(
      final Cli<Runnable> cli,
      final String[] args
  ) {
    final ParseResult<Runnable> result = cli.parseWithResult(args);
    if (result.wasSuccessful()) {
      return Optional.of(result.getCommand());
    } else {
      if (isGlobalHelpMessageRequested(result.getErrors())) {
        try {
          new CliGlobalUsageGenerator<Runnable>().usage(cli.getMetadata(), System.out);
          return Optional.empty();
        } catch (IOException e) {
          throw new MigrationException("Failed to print help: " + e.getMessage());
        }
      } else if (isCommandSpecificHelpMessageRequested(result.getState())) {
        final String commandName = result.getState().getCommand().getName();
        try {
          Help.help(cli.getMetadata(), Collections.singletonList(commandName), System.out);
          return Optional.empty();
        } catch (IOException e) {
          throw new MigrationException("Failed to print help for command '" + commandName
              + "': " + e.getMessage());
        }
      } else {
        // throw original parse exception
        final ParseException e = result.getErrors().stream()
            .findFirst()
            .orElseThrow(() -> new IllegalStateException(
                "Failed to parse statement yet no errors were collected"));
        // remove stack trace for improved readability
        throw new MigrationException(e.getMessage());
      }
    }
  }

  private static boolean isGlobalHelpMessageRequested(
      final Collection<ParseException> errors
  ) {
    return errors.stream()
        .anyMatch(e -> e instanceof ParseCommandMissingException)
        || errors.stream()
        .filter(e -> e instanceof ParseCommandUnrecognizedException)
        .map(e -> (ParseCommandUnrecognizedException) e)
        .flatMap(e -> e.getUnparsedInput().stream())
        .anyMatch(inp -> inp.equals("--help") || inp.equals("-h"));
  }

  private static boolean isCommandSpecificHelpMessageRequested(
      final ParseState<Runnable> parseState
  ) {
    for (final Pair<OptionMetadata, Object> option : parseState.getParsedOptions()) {
      if (option.getKey().getTitle().equals("help") && (boolean) option.getValue()) {
        return true;
      }
    }
    return false;
  }
}
