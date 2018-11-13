/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.cli.console.cmd;

import io.confluent.ksql.cli.console.Console;
import io.confluent.ksql.cli.console.OutputFormat;
import java.util.Objects;

class Output implements CliSpecificCommand {

  private final Console console;

  Output(final Console console) {
    this.console = Objects.requireNonNull(console, "console");
  }

  @Override
  public String getName() {
    return "output";
  }

  @Override
  public void printHelp() {
    console.writer().println("output:");
    console.writer().println("\tView the current output format.");
    console.writer().println("");
    console.writer().println("output <format>;");
    console.writer().println("");
    console.writer().printf(
        "\tSet the output format to <format> (valid formats: %s)%n",
        OutputFormat.VALID_FORMATS
    );
    console.writer().println("\tFor example: \"output JSON;\"");
  }

  @Override
  public void execute(final String commandStrippedLine) {
    final String newFormat = commandStrippedLine.trim().toUpperCase();
    if (newFormat.isEmpty()) {
      console.writer().printf("Current output format: %s%n", console.getOutputFormat().name());
    } else {
      console.setOutputFormat(newFormat);
    }
  }
}
