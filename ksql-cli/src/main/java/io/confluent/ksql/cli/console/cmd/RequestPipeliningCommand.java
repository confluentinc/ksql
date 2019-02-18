/*
 * Copyright 2019 Confluent Inc.
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
import java.util.function.Consumer;
import java.util.function.Supplier;

public final class RequestPipeliningCommand implements CliSpecificCommand {
  public static final String NAME = "request-pipelining";
  private static final String HELP = NAME + ":" + System.lineSeparator()
      + "\tView the current setting. "
      + "If 'ON', commands will be executed without waiting for previous commands to finish. "
      + "If 'OFF', newly issued commands will wait until all prior commands have finished. "
      + "Defaults to 'OFF'." + System.lineSeparator()
      + "\n" + NAME + " <ON/OFF>:" + System.lineSeparator()
      + "\tUpdate the setting as specified." + System.lineSeparator()
      + "\tFor example: \"" + NAME + " OFF;\"";

  private final Supplier<Boolean> requestPipeliningSupplier;
  private final Consumer<Boolean> requestPipeliningConsumer;

  public static RequestPipeliningCommand create(
      final Supplier<Boolean> requestPipeliningSupplier,
      final Consumer<Boolean> requestPipeliningConsumer) {
    return new RequestPipeliningCommand(requestPipeliningSupplier, requestPipeliningConsumer);
  }

  private RequestPipeliningCommand(
      final Supplier<Boolean> requestPipeliningSupplier,
      final Consumer<Boolean> requestPipeliningConsumer) {
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
  public String getHelpMessage() {
    return HELP;
  }

  @Override
  public void execute(final List<String> args, final PrintWriter terminal) {
    CliCmdUtil.ensureArgCountBounds(args, 0, 1, HELP);

    if (args.isEmpty()) {
      final String setting = requestPipeliningSupplier.get() ? "ON" : "OFF";
      terminal.printf("Current %s configuration: %s%n", NAME, setting);
    } else {
      final String newSetting = args.get(0);
      switch (newSetting.toUpperCase()) {
        case "ON":
          requestPipeliningConsumer.accept(true);
          break;
        case "OFF":
          requestPipeliningConsumer.accept(false);
          break;
        default:
          terminal.printf("Invalid %s setting: %s. ", NAME, newSetting);
          terminal.println("Valid options are 'ON' and 'OFF'.");
          return;
      }
      terminal.println(NAME + " configuration is now " + newSetting.toUpperCase());
    }
  }
}
