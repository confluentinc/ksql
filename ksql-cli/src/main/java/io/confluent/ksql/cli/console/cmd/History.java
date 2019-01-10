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
import java.util.List;
import java.util.Objects;

class History implements CliSpecificCommand {

  private static final String HELP = "history:" + System.lineSeparator()
      + "\tShow previous lines entered during the current CLI session. "
      + "You can use up and down arrow keys to view previous lines.";

  private final Runnable historyPrinter;

  History(final Runnable historyPrinter) {
    this.historyPrinter = Objects.requireNonNull(historyPrinter, "historyPrinter");
  }

  @Override
  public String getName() {
    return "history";
  }

  @Override
  public String getHelpMessage() {
    return HELP;
  }

  @Override
  public void execute(final List<String> args, final PrintWriter terminal) {
    CliCmdUtil.ensureArgCountBounds(args, 0, 0, () -> HELP);
    historyPrinter.run();
  }
}
