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

import io.confluent.ksql.cli.KsqlRequestExecutor;
import io.confluent.ksql.cli.console.Console;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.util.Event;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * God class for registering Cli commands
 */
public final class CliCommandRegisterUtil {

  private CliCommandRegisterUtil() {
  }

  public static void registerDefaultCommands(
      final KsqlRequestExecutor requestExecutor,
      final Console console,
      final Supplier<String> versionSuppler,
      final KsqlRestClient restClient,
      final Event resetCliForNewServer,
      final Supplier<Boolean> requestPipeliningSupplier,
      final Consumer<Boolean> requestPipeliningConsumer) {

    console.registerCliSpecificCommand(
        Help.create(() -> console.getCliSpecificCommands().values()));

    console.registerCliSpecificCommand(
        Clear.create(console::clearScreen));

    console.registerCliSpecificCommand(
        Output.create(console::getOutputFormat, console::setOutputFormat));

    console.registerCliSpecificCommand(
        History.create(console::getHistory));

    console.registerCliSpecificCommand(
        Version.create(versionSuppler));

    console.registerCliSpecificCommand(
        Exit.create());

    console.registerCliSpecificCommand(
        RunScript.create(requestExecutor));

    console.registerCliSpecificCommand(
        RemoteServerSpecificCommand.create(restClient, resetCliForNewServer));

    console.registerCliSpecificCommand(
        RequestPipeliningCommand.create(requestPipeliningSupplier, requestPipeliningConsumer));
  }
}
