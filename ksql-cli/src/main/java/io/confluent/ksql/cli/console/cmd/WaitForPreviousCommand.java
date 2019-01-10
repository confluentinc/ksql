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
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class WaitForPreviousCommand implements CliSpecificCommand {
  public static final String NAME = "wait-for-previous-command";

  private final PrintWriter writer;
  private final Supplier<Boolean> waitForPreviousCommandSupplier;
  private final Consumer<Boolean> waitForPreviousCommandConsumer;

  public WaitForPreviousCommand(
      final PrintWriter writer,
      final Supplier<Boolean> waitForPreviousCommandSupplier,
      final Consumer<Boolean> waitForPreviousCommandConsumer) {
    this.writer = Objects.requireNonNull(writer, "writer");
    this.waitForPreviousCommandSupplier =
        Objects.requireNonNull(waitForPreviousCommandSupplier, "waitForPreviousCommandSupplier");
    this.waitForPreviousCommandConsumer =
        Objects.requireNonNull(waitForPreviousCommandConsumer, "waitForPreviousCommandConsumer");
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public void printHelp() {
    writer.println(NAME + ":");
    writer.println("\tView the current setting.");
    writer.println("");
    writer.println(NAME + " <ON/OFF>;");
    writer.println("");
    writer.println("\tUpdate the setting as specified.");
    writer.printf("\tFor example: \"%s OFF;\"%n", NAME);
  }

  @Override
  public void execute(final String commandStrippedLine) {
    final String newSetting = commandStrippedLine.trim();
    if (newSetting.isEmpty()) {
      final String setting = waitForPreviousCommandSupplier.get() ? "ON" : "OFF";
      writer.printf("Current %s configuration: %s%n", NAME, setting);
    } else {
      switch (newSetting.toUpperCase()) {
        case "ON":
          waitForPreviousCommandConsumer.accept(true);
          break;
        case "OFF":
          waitForPreviousCommandConsumer.accept(false);
          break;
        default:
          writer.printf("Invalid %s setting: %s. ", NAME, newSetting);
          writer.println("Valid options are 'ON' and 'OFF'.");
          return;
      }
      writer.println(NAME + " configuration is now " + newSetting.toUpperCase());
    }
  }
}
