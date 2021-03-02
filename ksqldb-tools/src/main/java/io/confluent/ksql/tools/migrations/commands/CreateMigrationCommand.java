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

import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.help.Examples;
import com.github.rvesse.airline.annotations.restrictions.Required;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Command(
    name = "create",
    description = "Create a migration file with <desc> as description, which will be used to "
        + "apply the next schema version."
)
@Examples(
    examples = "$ ksql-migrations create add_users",
    descriptions = "Creates a new migrations file for adding a users table to ksqlDB "
        + "(e.g. V000002__Add_users.sql)"
)
public class CreateMigrationCommand extends BaseCommand {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreateMigrationCommand.class);

  @Option(
      name = {"-v", "--version"},
      description = "the schema version to initialize, defaults to the next"
          + " schema version."
  )
  private int version;

  @Required
  @Arguments(
      title = "desc",
      description = "The description for the migration."
  )
  private String description;

  @Override
  protected int command() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected Logger getLogger() {
    return LOGGER;
  }
}
