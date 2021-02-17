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

import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.tools.migrations.Migrations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Defines common options across all of the migration
 * tool commands.
 */
@SuppressFBWarnings(
    value = "URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD",
    justification = "code is skeleton only at the moment, used to generate HELP message"
)
public abstract class BaseCommand implements Runnable {

  @Option(
      name = {"-c", "--config-file"},
      title = "config-file",
      description = "Specifies a configuration file",
      type = OptionType.GLOBAL
  )
  protected String configFile;

  @Option(
      name = {"--dry-run"},
      title = "dry-run",
      description = "dry run the current command, no ksqlDB statements will be executed",
      type = OptionType.GLOBAL
  )
  protected boolean dryRun = false;

  protected static final Logger LOGGER = LoggerFactory.getLogger(Migrations.class);

  @Override
  public void run() {
    final long startTime = System.nanoTime();
    command();
    LOGGER.info("Execution time: " + (System.nanoTime() - startTime) / 1000000000);
  }

  protected abstract void command();
}
