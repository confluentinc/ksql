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

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_ACCEPTABLE;

import io.confluent.ksql.links.DocumentationLinks;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.client.exception.KsqlRestClientException;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.ServerInfo;
import io.confluent.ksql.util.ErrorMessageUtil;
import io.confluent.ksql.util.Event;
import java.io.PrintWriter;
import java.util.List;
import java.util.Objects;
import javax.net.ssl.SSLException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.WordUtils;

public final class RemoteServerSpecificCommand implements CliSpecificCommand {

  private static final int CONSOLE_WIDTH = 80;
  private static final String HELP = "server:" + System.lineSeparator()
      + "\tShow the current server" + System.lineSeparator()
      + "\nserver <server>:" + System.lineSeparator()
      + "\tChange the current server to <server>" + System.lineSeparator()
      + "\t example: \"server http://my.awesome.server.com:9098;\"";

  private static final String CCLOUD_KSQL_SERVICE_ID_PREFIX = "pksqlc-";
  private static final String CCLOUD_KSQL_ADDRESS_SUBSTRING = ".cloud";

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
      final RestResponse<ServerInfo> restResponse = restClient.getServerInfo();
      if (restResponse.isErroneous()) {
        final KsqlErrorMessage ksqlError = restResponse.getErrorMessage();
        if (Errors.toStatusCode(ksqlError.getErrorCode()) == NOT_ACCEPTABLE.code()) {
          writer.format("This CLI version no longer supported: %s%n%n", ksqlError);
          return;
        }
        writer.format(
            "Couldn't connect to the KSQL server: %s%n%n", ksqlError.getMessage());
      } else {
        maybeSetIsCCloudServer(restClient, restResponse.getResponse());
      }
    } catch (final IllegalArgumentException exception) {
      writer.println("Server URL must begin with protocol (e.g., http:// or https://)");
    } catch (final KsqlRestClientException exception) {
      writer.println();
      writer.println(StringUtils.center("ERROR", CONSOLE_WIDTH, "*"));

      final String errorMsg;

      if (exception.getCause().getCause() instanceof SSLException) {
        errorMsg = " looks to be configured to use HTTPS / SSL. "
            + "Please refer to the KSQL documentation on how to configure the CLI for SSL: "
            + DocumentationLinks.SECURITY_CLI_SSL_DOC_URL;
      } else {
        errorMsg = " does not appear to be a valid KSQL server."
            + " Please ensure that the URL provided is for an active KSQL server.";
      }

      writer.println(WordUtils.wrap(
          "Remote server at " + restClient.getServerAddress() + errorMsg, CONSOLE_WIDTH));

      writer.println("");
      writer.println("The server responded with the following error: ");
      writer.println(ErrorMessageUtil.buildErrorMessage(exception));
      writer.println(StringUtils.repeat('*', CONSOLE_WIDTH));
      writer.println();
    } finally {
      writer.flush();
    }
  }

  private static void maybeSetIsCCloudServer(
      final KsqlRestClient restClient,
      final ServerInfo serverInfo) {
    final String ksqlServiceId = serverInfo.getKsqlServiceId();
    final String ksqlServerAddress = restClient.getServerAddress().toString();
    restClient.setIsCCloudServer(isCCloudServer(ksqlServiceId, ksqlServerAddress));
  }

  private static boolean isCCloudServer(
      final String ksqlServiceId,
      final String ksqlServerAddress
  ) {
    return ksqlServiceId.startsWith(CCLOUD_KSQL_SERVICE_ID_PREFIX)
        && ksqlServerAddress.contains(CCLOUD_KSQL_ADDRESS_SUBSTRING)
        && ksqlServerAddress.contains(ksqlServiceId);
  }
}
