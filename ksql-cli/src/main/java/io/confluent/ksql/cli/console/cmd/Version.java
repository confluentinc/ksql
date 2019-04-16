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
import java.util.function.Supplier;

class Version implements CliSpecificCommand {

  private final Console console;
  private final Supplier<String> versionSupplier;

  Version(final Console console, final Supplier<String> versionSupplier) {
    this.console = Objects.requireNonNull(console, "console");
    this.versionSupplier = Objects.requireNonNull(versionSupplier, "versionSupplier");
  }

  @Override
  public String getName() {
    return "version";
  }

  @Override
  public void printHelp() {
    console.writer().println("version:");
    console.writer().println("\tGet the current KSQL version.");
  }

  @Override
  public void execute(final String commandStrippedLine) {
    final String version = versionSupplier.get();
    console.writer().printf("Version: %s%n", version);
    console.flush();
  }
}
