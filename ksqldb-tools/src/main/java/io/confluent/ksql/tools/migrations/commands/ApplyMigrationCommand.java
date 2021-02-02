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
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.MutuallyExclusiveWith;

@Command(
    name = "apply",
    description = "Migrates a schema to new available schema versions"
)
public class ApplyMigrationCommand extends BaseCommand {

  @Option(
      title = "all",
      name = {"-a", "--all"},
      description = "run all available migrations"
  )
  @MutuallyExclusiveWith(tag = "target")
  private boolean all;

  @Option(
      title = "next",
      name = {"-n", "--next"},
      description = "migrate the next available version"
  )
  @MutuallyExclusiveWith(tag = "target")
  private boolean next;

  @Option(
      title = "version",
      name = {"-u", "--until"},
      arity = 1,
      description = "migrate until the specified version"
  )
  @MutuallyExclusiveWith(tag = "target")
  private int version;

  @Override
  public void run() {
    throw new UnsupportedOperationException();
  }
}
