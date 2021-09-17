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

import io.confluent.ksql.cli.console.OutputFormat;
import java.io.PrintWriter;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

final class Output implements CliSpecificCommand {

  private static final String HELP = "output:" + System.lineSeparator()
      + "\tView the current output format." + System.lineSeparator()
      + System.lineSeparator()
      + "output <format>:" + System.lineSeparator()
      + "\tSet the output format to <format> (valid formats: " + OutputFormat.VALID_FORMATS + ")"
      + System.lineSeparator()
      + System.lineSeparator()
      + "\tFor example: \"output JSON;\""
      + System.lineSeparator();

  private final Supplier<OutputFormat> getter;
  private final Consumer<String> setter;

  private Output(final Supplier<OutputFormat> getter, final Consumer<String> setter) {
    this.getter = Objects.requireNonNull(getter, "getter");
    this.setter = Objects.requireNonNull(setter, "setter");
  }

  static Output create(final Supplier<OutputFormat> getter, final Consumer<String> setter) {
    return new Output(getter, setter);
  }

  @Override
  public String getName() {
    return "output";
  }

  @Override
  public String getHelpMessage() {
    return HELP;
  }

  @Override
  public void execute(final List<String> args, final PrintWriter terminal) {
    CliCmdUtil.ensureArgCountBounds(args, 0, 1, HELP);

    if (args.isEmpty()) {
      terminal.printf("Current output format: %s%n", getter.get().name());
      return;
    }

    final String newFormat = args.get(0).toUpperCase();
    setter.accept(newFormat);
  }
}
