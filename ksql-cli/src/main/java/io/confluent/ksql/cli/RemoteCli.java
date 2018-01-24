/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.cli;

import java.io.PrintWriter;

import javax.ws.rs.ProcessingException;

import io.confluent.ksql.cli.console.CliSpecificCommand;
import io.confluent.ksql.cli.console.Console;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.client.exception.KsqlRestClientException;
import io.confluent.ksql.util.CommonUtils;

public class RemoteCli extends Cli {

  public RemoteCli(
      Long streamedQueryRowLimit,
      Long streamedQueryTimeoutMs,
      KsqlRestClient restClient,
      Console terminal
  ) {
    super(
        streamedQueryRowLimit,
        streamedQueryTimeoutMs,
        restClient,
        terminal
    );

    validateClient(terminal.writer(), restClient);

    terminal.registerCliSpecificCommand(new RemoteCliSpecificCommand(
        restClient,
        terminal.writer()
    ));
  }

  // Visible for testing
  public boolean hasUserCredentials() {
    return restClient.hasUserCredentials();
  }

  private static void validateClient(
      final PrintWriter writer,
      final KsqlRestClient restClient
  ) {
    try {
      RestResponse restResponse = restClient.makeRootRequest();
      if (restResponse.isErroneous()) {
        writer.format(
            "Couldn't connect to the KSQL server: %s\n\n",
            restResponse.getErrorMessage().getMessage()
        );
      }
    } catch (IllegalArgumentException exception) {
      writer.println("Server URL must begin with protocol (e.g., http:// or https://)");
    } catch (KsqlRestClientException exception) {
      if (exception.getCause() instanceof ProcessingException) {
        writer.println();
        writer.println("**************** WARNING ******************");
        writer.println("Remote server address may not be valid:");
        writer.println(CommonUtils.getErrorMessageWithCause(exception));
        writer.println("*******************************************");
        writer.println();
      } else {
        throw exception;
      }
    }
  }

  static class RemoteCliSpecificCommand implements CliSpecificCommand {

    private final KsqlRestClient restClient;
    private final PrintWriter writer;

    RemoteCliSpecificCommand(
        final KsqlRestClient restClient,
        final PrintWriter writer
    ) {
      this.writer = writer;
      this.restClient = restClient;
    }

    @Override
    public String getName() {
      return "server";
    }

    @Override
    public void printHelp() {
      writer.println("\tserver:          Show the current server");
      writer.println("\tserver <server>: Change the current server to <server>");
      writer.println("\t                 example: "
                     + "\"server http://my.awesome.server.com:9098\""
      );
    }

    @Override
    public void execute(String commandStrippedLine) {
      if (commandStrippedLine.isEmpty()) {
        writer.println(restClient.getServerAddress());
      } else {
        String serverAddress = commandStrippedLine.trim();
        restClient.setServerAddress(serverAddress);
        validateClient(writer, restClient);
      }
    }
  }
}
