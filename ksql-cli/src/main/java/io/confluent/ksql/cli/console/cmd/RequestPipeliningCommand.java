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

public class RequestPipeliningCommand implements CliSpecificCommand {
  public static final String NAME = "request-pipelining";

  private final PrintWriter writer;
  private final Supplier<Boolean> requestPipeliningSupplier;
  private final Consumer<Boolean> requestPipeliningConsumer;

  RequestPipeliningCommand(
      final PrintWriter writer,
      final Supplier<Boolean> requestPipeliningSupplier,
      final Consumer<Boolean> requestPipeliningConsumer) {
    this.writer = Objects.requireNonNull(writer, "writer");
    this.requestPipeliningSupplier =
        Objects.requireNonNull(requestPipeliningSupplier, "requestPipeliningSupplier");
    this.requestPipeliningConsumer =
        Objects.requireNonNull(requestPipeliningConsumer, "requestPipeliningConsumer");
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public void printHelp() {
    writer.println(NAME + ":");
    writer.println("\tView the current setting. "
        + "If 'ON', commands will be executed without waiting for previous commands to finish. "
        + "If 'OFF', newly issued commands will wait until all prior commands have finished. "
        + "Defaults to 'OFF'.");
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
      final String setting = requestPipeliningSupplier.get() ? "ON" : "OFF";
      writer.printf("Current %s configuration: %s%n", NAME, setting);
    } else {
      switch (newSetting.toUpperCase()) {
        case "ON":
          requestPipeliningConsumer.accept(true);
          break;
        case "OFF":
          requestPipeliningConsumer.accept(false);
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
