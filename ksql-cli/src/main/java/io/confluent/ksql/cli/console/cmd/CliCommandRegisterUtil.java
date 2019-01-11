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

import io.confluent.ksql.cli.console.Console;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.util.Event;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * God class for registering Cli commands
 */
// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public final class CliCommandRegisterUtil {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private CliCommandRegisterUtil() {
  }

  public static void registerDefaultConsoleCommands(
      final Console console
  ) {
    console.registerCliSpecificCommand(new Help(console));

    console.registerCliSpecificCommand(new Clear(console));

    console.registerCliSpecificCommand(new Output(console));

    console.registerCliSpecificCommand(new History(console));

    console.registerCliSpecificCommand(new Exit(console));
  }

  public static void registerDefaultCliCommands(
      final Console console,
      final KsqlRestClient restClient,
      final Event resetCliForNewServer,
      final Supplier<Boolean> requestPipeliningSupplier,
      final Consumer<Boolean> requestPipeliningConsumer
  ) {
    final Supplier<String> versionSuppler =
        () -> restClient.getServerInfo().getResponse().getVersion();
    console.registerCliSpecificCommand(new Version(console, versionSuppler));

    console.registerCliSpecificCommand(new RemoteServerSpecificCommand(
        restClient, console.writer(), resetCliForNewServer));

    console.registerCliSpecificCommand(new RequestPipeliningCommand(
        console.writer(), requestPipeliningSupplier, requestPipeliningConsumer));
  }
}
