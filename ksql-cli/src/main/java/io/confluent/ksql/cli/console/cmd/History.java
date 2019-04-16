/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.cli.console.cmd;

import io.confluent.ksql.cli.console.Console;
import java.util.Objects;

class History implements CliSpecificCommand {

  private Console console;

  History(final Console console) {
    this.console = Objects.requireNonNull(console, "console");
  }

  @Override
  public String getName() {
    return "history";
  }

  @Override
  public void printHelp() {
    console.writer().println(
        "history:");
    console.writer().println(
        "\tShow previous lines entered during the current CLI session. You can"
            + " use up and down arrow keys to view previous lines."
    );
  }

  @Override
  public void execute(final String commandStrippedLine) {
    console.printHistory();
    console.flush();
  }
}
