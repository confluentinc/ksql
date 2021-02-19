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

import com.github.rvesse.airline.annotations.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Command(
    name = "new",
    description = "Cleans all resources related to migrations. WARNING: this is not reversible!"
)
public class CleanMigrationsCommand extends BaseCommand {

  private static final Logger LOGGER = LoggerFactory.getLogger(CleanMigrationsCommand.class);

  @Override
  protected int command() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected Logger getLogger() {
    return LOGGER;
  }

}
