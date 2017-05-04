/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.cli;

import io.confluent.kql.rest.client.KQLRestClient;
import io.confluent.kql.rest.server.KQLRestApplication;
import io.confluent.kql.rest.server.KQLRestConfig;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

public class LocalCli extends Cli {

  private final KQLRestApplication serverApplication;

  public LocalCli(Properties serverProperties, int portNumber) throws Exception {
    super(new KQLRestClient(getServerAddress(portNumber)));

    // Have to override listeners config to make sure it aligns with port number for client
    serverProperties.put(KQLRestConfig.LISTENERS_CONFIG, getServerAddress(portNumber));

    this.serverApplication = KQLRestApplication.buildApplication(serverProperties, false);
    serverApplication.start();
  }

  @Override
  public void close() throws IOException {
    try {
      serverApplication.stop();
      serverApplication.join();
    } catch (TimeoutException exception) {
      /*
          This is only thrown under the following circumstances:

            1. A user makes a request for a streamed query.
            2. The user terminates the request for the streamed query.
            3. Before the thread(s) responsible for streaming the query have terminated, serverApplication.stop() is
               called.

          Even if the threads then manage to terminate within the graceful shutdown window for the server, the
          TimeoutException is still thrown.

          TODO: Prevent the TimeoutException from being thrown when this happens.
       */
    } catch (IOException exception) {
      throw exception;
    } catch (Exception exception) {
      throw new RuntimeException(exception);
    } finally {
      super.close();
    }
  }

  private static String getServerAddress(int portNumber) {
    return String.format("http://localhost:%d", portNumber);
  }
}
