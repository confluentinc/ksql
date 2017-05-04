/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.cli;

import io.confluent.kql.rest.client.KQLRestClient;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class RemoteCli extends Cli {

  public RemoteCli(String serverAddress) throws IOException {
    super(new KQLRestClient(serverAddress));
  }

  @Override
  protected void handleMetaCommand(String trimmedLine) throws IOException, InterruptedException, ExecutionException {
    String[] commandArgs = trimmedLine.split("\\s+", 2);
    String command = commandArgs[0];
    switch (command) {
      case ":server":
        if (commandArgs.length == 1) {
          terminal.writer().println(restClient.getServerAddress());
        } else {
          String serverAddress = commandArgs[1];
          restClient.setServerAddress(serverAddress);
        }
        break;
      default:
        super.handleMetaCommand(trimmedLine);
        break;
    }
  }

  @Override
  protected void printExtraMetaCommandsHelp() throws IOException {
    terminal.writer().println("    :server             - Show the current server");
    terminal.writer().println("    :server <server>    - Change the current server to <server>");
    terminal.writer().println("                          Example: :server confluent.io:6969'");
  }
}
