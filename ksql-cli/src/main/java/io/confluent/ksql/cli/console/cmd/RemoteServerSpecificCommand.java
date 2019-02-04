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

import static javax.ws.rs.core.Response.Status.NOT_ACCEPTABLE;

import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.client.exception.KsqlRestClientException;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.server.resources.Errors;
import io.confluent.ksql.util.ErrorMessageUtil;
import io.confluent.ksql.util.Event;
import java.io.PrintWriter;
import java.util.List;
import java.util.Objects;
import javax.ws.rs.ProcessingException;

public final class RemoteServerSpecificCommand implements CliSpecificCommand {

  private static final String HELP = "server:" + System.lineSeparator()
      + "\tShow the current server" + System.lineSeparator()
      + "\nserver <server>:" + System.lineSeparator()
      + "\tChange the current server to <server>" + System.lineSeparator()
      + "\t example: \"server http://my.awesome.server.com:9098;\"";

  private final KsqlRestClient restClient;
  private final Event resetCliForNewServer;

  public static RemoteServerSpecificCommand create(
      final KsqlRestClient restClient, final Event resetCliForNewServer) {
    return new RemoteServerSpecificCommand(restClient, resetCliForNewServer);
  }

  private RemoteServerSpecificCommand(
      final KsqlRestClient restClient, final Event resetCliForNewServer) {
    this.restClient = Objects.requireNonNull(restClient, "restClient");
    this.resetCliForNewServer =
        Objects.requireNonNull(resetCliForNewServer, "resetCliForNewServer");
  }

  @Override
  public String getName() {
    return "server";
  }

  @Override
  public String getHelpMessage() {
    return HELP;
  }

  @Override
  public void execute(final List<String> args, final PrintWriter terminal) {
    CliCmdUtil.ensureArgCountBounds(args, 0, 1, HELP);

    if (args.isEmpty()) {
      terminal.println(restClient.getServerAddress());
      return;
    } else {
      final String serverAddress = args.get(0);
      restClient.setServerAddress(serverAddress);
      terminal.println("Server now: " + serverAddress);
      resetCliForNewServer.fire();
    }

    validateClient(terminal, restClient);
  }

  public static void validateClient(
      final PrintWriter writer,
      final KsqlRestClient restClient
  ) {
    try {
      final RestResponse restResponse = restClient.makeRootRequest();
      if (restResponse.isErroneous()) {
        final KsqlErrorMessage ksqlError = restResponse.getErrorMessage();
        if (Errors.toStatusCode(ksqlError.getErrorCode()) == NOT_ACCEPTABLE.getStatusCode()) {
          writer.format("This CLI version no longer supported: %s%n%n", ksqlError);
          return;
        }
        writer.format(
            "Couldn't connect to the KSQL server: %s%n%n", ksqlError.getMessage());
      }
    } catch (final IllegalArgumentException exception) {
      writer.println("Server URL must begin with protocol (e.g., http:// or https://)");
    } catch (final KsqlRestClientException exception) {
      if (exception.getCause() instanceof ProcessingException) {
        writer.println();
        writer.println("**************** ERROR ********************");
        writer.println("Remote server address may not be valid.");
        writer.println("Address: " + restClient.getServerAddress());
        writer.println(ErrorMessageUtil.buildErrorMessage(exception));
        writer.println("*******************************************");
        writer.println();
      } else {
        throw exception;
      }
    } finally {
      writer.flush();
    }
  }
}
