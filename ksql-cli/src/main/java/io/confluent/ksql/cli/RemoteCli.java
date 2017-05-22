/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.cli;

import io.confluent.ksql.rest.client.KSQLRestClient;

import java.io.IOException;

public class RemoteCli extends Cli {

  public RemoteCli(
      String serverAddress,
      Long streamedQueryRowLimit,
      Long streamedQueryTimeoutMs,
      OutputFormat outputFormat
  ) throws IOException {
    super(new KSQLRestClient(serverAddress), streamedQueryRowLimit, streamedQueryTimeoutMs, outputFormat);

    registerCliSpecificCommand(new CliSpecificCommand() {
      @Override
      public String getName() {
        return "server";
      }

      @Override
      public void printHelp() {
        terminal.writer().println("\tserver:          Show the current server");
        terminal.writer().println("\tserver <server>: Change the current server to <server>");
        terminal.writer().println("\t                 example: \"server http://my.awesome.server.com:6969\"");
      }

      @Override
      public void execute(String commandStrippedLine) throws IOException {
        if (commandStrippedLine.isEmpty()) {
          terminal.writer().println(restClient.getServerAddress());
        } else {
          String serverAddress = commandStrippedLine.trim();
          restClient.setServerAddress(serverAddress);
        }
      }
    });
  }
}
