/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.cli.console.cmd;

import java.io.PrintWriter;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

class Help implements CliSpecificCommand {

  private static final String HELP = "help:" + System.lineSeparator()
      + "\tShow this message.";

  private final Supplier<Collection<CliSpecificCommand>> cmds;

  Help(final Supplier<Collection<CliSpecificCommand>> cmds) {
    this.cmds = Objects.requireNonNull(cmds, "cmds");
  }

  @Override
  public String getName() {
    return "help";
  }

  @Override
  public String getHelpMessage() {
    return HELP;
  }

  @Override
  public void execute(final List<String> args, final PrintWriter terminal) {
    CliCmdUtil.ensureArgCountBounds(args, 0, 0, () -> HELP);

    terminal.println();
    terminal.println("Description:");
    terminal.println(
        "\tThe KSQL CLI provides a terminal-based interactive shell for running queries. "
            + "Each command should be on a separate line. "
            + "For KSQL command syntax, see the documentation at "
            + "https://github.com/confluentinc/ksql/docs/."
    );
    terminal.println();
    for (final CliSpecificCommand cliSpecificCommand : cmds.get()) {
      terminal.println(cliSpecificCommand.getHelpMessage());
      terminal.println();
    }
    terminal.println();
    terminal.println("Keyboard shortcuts:");
    terminal.println();
    terminal.println("    The KSQL CLI supports these keyboard shorcuts:");
    terminal.println();
    terminal.println("CTRL+D:");
    terminal.println("\tEnd your KSQL CLI session.");
    terminal.println("CTRL+R:");
    terminal.println("\tSearch your command history.");
    terminal.println("Up and Down arrow keys:");
    terminal.println("\tScroll up or down through your command history.");
    terminal.println();
    terminal.println("Default behavior:");
    terminal.println();
    terminal.println(
        "    Lines are read one at a time and are sent to the "
            + "server as KSQL unless one of the following is true:"
    );
    terminal.println();
    terminal.println(
        "    1. The line is empty or entirely whitespace. "
            + "In this case, no request is made to the server."
    );
    terminal.println();
    terminal.println(
        "    2. The line is not an in-built CLI command and does not end with a semi-colon. "
            + "In this case, the cli enters multi-line mode where lines are continuously read "
            + "until a line is encountered that is terminated with a semi-colon, "
            + "the concatenation of all lines read during this time is sent to the server as KSQL."
    );
    terminal.println();
  }
}
