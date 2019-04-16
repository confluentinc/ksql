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

class Help implements CliSpecificCommand {

  private final Console console;

  Help(final Console console) {
    this.console = Objects.requireNonNull(console, "console");
  }

  @Override
  public String getName() {
    return "help";
  }

  @Override
  public void printHelp() {
    console.writer().println("help:");
    console.writer().println("\tShow this message.");
  }

  @Override
  public void execute(final String line) {
    console.writer().println();
    console.writer().println("Description:");
    console.writer().println(
        "\tThe KSQL CLI provides a terminal-based interactive shell"
            + " for running queries. Each command must be on a separate line. "
            + "For KSQL command syntax, see the documentation at "
            + "https://github.com/confluentinc/ksql/docs/."
    );
    console.writer().println();
    for (final CliSpecificCommand cliSpecificCommand : console.getCliSpecificCommands().values()) {
      cliSpecificCommand.printHelp();
      console.writer().println();
    }
    console.writer().println();
    console.writer().println("Keyboard shortcuts:");
    console.writer().println();
    console.writer().println("    The KSQL CLI supports these keyboard shorcuts:");
    console.writer().println();
    console.writer().println("CTRL+D:");
    console.writer().println("\tEnd your KSQL CLI session.");
    console.writer().println("CTRL+R:");
    console.writer().println("\tSearch your command history.");
    console.writer().println("Up and Down arrow keys:");
    console.writer().println("\tScroll up or down through your command history.");
    console.writer().println();
    console.writer().println("Default behavior:");
    console.writer().println();
    console.writer().println(
        "    Lines are read one at a time and are sent to the "
            + "server as KSQL unless one of the following is true:"
    );
    console.writer().println();
    console.writer().println(
        "    1. The line is empty or entirely whitespace. In this"
            + " case, no request is made to the server."
    );
    console.writer().println();
    console.writer().println(
        "    2. The line ends with backslash ('\\'). In this case, lines are "
            + "continuously read and stripped of their trailing newline and '\\' "
            + "until one is "
            + "encountered that does not end with '\\'; then, the concatenation of "
            + "all lines read "
            + "during this time is sent to the server as KSQL."
    );
    console.writer().println();
  }
}
