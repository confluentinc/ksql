/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.cli.console.cmd;

import java.io.File;
import java.io.PrintWriter;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public final class Spool implements CliSpecificCommand {

  private static final String HELP = "spool [<path_to_file>|OFF]: "
      + System.lineSeparator()
      + "\tStores issued commands and their results into a file. Only one spool may "
      + System.lineSeparator()
      + "\tbe active at a time and can be closed by issuing ``SPOOL OFF``. Commands are "
      + System.lineSeparator()
      + "\t prefixed with ``ksql> `` to differentiate from output.";

  private static final String OFF = "OFF";

  private final Runnable unsetSpool;
  private final Consumer<File> setSpool;

  private Spool(final Consumer<File> setSpool, final Runnable unsetSpool) {
    this.setSpool = Objects.requireNonNull(setSpool, "setSpool");
    this.unsetSpool = Objects.requireNonNull(unsetSpool, "unsetSpool");
  }

  public static Spool create(final Consumer<File> setSpool, final Runnable unsetSpool) {
    return new Spool(setSpool, unsetSpool);
  }

  @Override
  public String getName() {
    return "spool";
  }

  @Override
  public String getHelpMessage() {
    return HELP;
  }

  @Override
  public void execute(final List<String> args, final PrintWriter terminal) {
    CliCmdUtil.ensureArgCountBounds(args, 1, 1, HELP);

    final String filePathOrOff = args.get(0);
    if (filePathOrOff.equalsIgnoreCase(OFF)) {
      unsetSpool.run();
    } else {
      setSpool.accept(new File(cleanQuotes(filePathOrOff)));
    }
  }

  private static String cleanQuotes(final String stringWithQuotes) {
    if (!stringWithQuotes.startsWith("'") || !stringWithQuotes.endsWith("'")) {
      return stringWithQuotes;
    }

    return stringWithQuotes
        .substring(1, stringWithQuotes.length() - 1)
        .replaceAll("''", "'");
  }
}
