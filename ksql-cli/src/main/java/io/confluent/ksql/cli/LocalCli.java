/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.cli;

import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.server.KsqlRestApplication;
import io.confluent.ksql.rest.server.KsqlRestConfig;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

public class LocalCli extends Cli {

  private final KsqlRestApplication serverApplication;

  public LocalCli(
      Properties serverProperties,
      int portNumber,
      Long streamedQueryRowLimit,
      Long streamedQueryTimeoutMs,
      OutputFormat outputFormat
  ) throws Exception {
    super(
        new KsqlRestClient(getServerAddress(portNumber)),
        streamedQueryRowLimit,
        streamedQueryTimeoutMs,
        outputFormat
    );

    // Have to override listeners config to make sure it aligns with port number for client
    serverProperties.put(KsqlRestConfig.LISTENERS_CONFIG, getServerAddress(portNumber));

    this.serverApplication = KsqlRestApplication.buildApplication(serverProperties, false);
    serverApplication.start();
  }

  @Override
  public void close() throws IOException {
    try {
      serverApplication.getKsqlEngine().terminateAllQueries();
      serverApplication.stop();
      serverApplication.join();
    } catch (TimeoutException exception) {
      /*
          This is only thrown under the following circumstances:

            1. A user makes a request for a streamed query.
            2. The user terminates the request for the streamed query.
            3. Before the thread(s) responsible for streaming the query have terminated,
               serverApplication.stop() is called.

          Even if the threads then manage to terminate within the graceful shutdown window for the
          server, the TimeoutException is still thrown.

          TODO: Prevent the TimeoutException from being thrown when this happens.
       */
    } catch (IOException exception) {
      throw exception;
    } catch (Exception exception) {
      throw new RuntimeException(exception);
    } finally {
      super.close();
      System.exit(0);
    }
  }

  private static String getServerAddress(int portNumber) {
    return String.format("http://localhost:%d", portNumber);
  }
}
