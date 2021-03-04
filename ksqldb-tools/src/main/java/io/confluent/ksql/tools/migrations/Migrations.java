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
import com.github.rvesse.airline.help.Help;
import io.confluent.ksql.tools.migrations.commands.ApplyMigrationCommand;
import io.confluent.ksql.tools.migrations.commands.BaseCommand;
import io.confluent.ksql.tools.migrations.commands.CleanMigrationsCommand;
import io.confluent.ksql.tools.migrations.commands.CreateMigrationCommand;
import io.confluent.ksql.tools.migrations.commands.InitializeMigrationCommand;
import io.confluent.ksql.tools.migrations.commands.MigrationInfoCommand;
import io.confluent.ksql.tools.migrations.commands.NewMigrationCommand;
import io.confluent.ksql.tools.migrations.commands.ValidateMigrationsCommand;
import io.confluent.ksql.tools.migrations.util.MigrationsUtil;

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
        CleanMigrationsCommand.class,
        ValidateMigrationsCommand.class,
        InitializeMigrationCommand.class
    },
    defaultCommand = Help.class
)
public final class Migrations {

  private Migrations() {}

  public static void main(final String[] args) {
    // even though all migrations commands implement BaseCommand, the Help
    // command does not so we infer the type as Runnable instead
    final Cli<Runnable> cli = new Cli<>(Migrations.class);
    final Runnable command = cli.parse(args);
    if (command instanceof Help) {
      command.run();
      System.exit(0);
    }

    System.exit(((BaseCommand) command).runCommand());
  }

}
