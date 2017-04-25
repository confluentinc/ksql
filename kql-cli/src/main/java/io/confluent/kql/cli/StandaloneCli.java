package io.confluent.kql.cli;

import io.confluent.kql.rest.client.KQLRestClient;
import io.confluent.kql.rest.server.KQLRestApplication;
import io.confluent.kql.rest.server.KQLRestConfig;

import java.io.IOException;
import java.util.Properties;

public class StandaloneCli extends Cli {

  private final KQLRestApplication serverApplication;

  public StandaloneCli(Properties serverProperties, int portNumber) throws Exception {
    super(new KQLRestClient(getServerAddress(portNumber)));

    // Have to override listeners config to make sure it aligns with port number for client
    serverProperties.put(KQLRestConfig.LISTENERS_CONFIG, getServerAddress(portNumber));
    this.serverApplication = KQLRestApplication.buildApplication(serverProperties, false);
  }

  @Override
  public void repl() throws IOException {
    try {
      serverApplication.start();
    } catch (Exception exception) {
      throw new RuntimeException(exception);
    }

    super.repl();

    try {
      serverApplication.stop();
      serverApplication.join();
    } catch (Exception exception) {
      throw new RuntimeException(exception);
    }
  }

  private static String getServerAddress(int portNumber) {
    return String.format("http://localhost:%d", portNumber);
  }
}
