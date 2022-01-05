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

package io.confluent.ksql.tools;

import io.confluent.ksql.api.client.BatchedQueryResult;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.Row;
import java.util.List;
import java.util.concurrent.ExecutionException;

public final class ExampleApp {

  private static final String KSQLDB_SERVER_HOST = "localhost";
  private static final int KSQLDB_SERVER_HOST_PORT = 8088;

  private ExampleApp() {}

  public static void main(final String[] args) throws ExecutionException, InterruptedException {
    final ClientOptions options = ClientOptions.create().setHost(KSQLDB_SERVER_HOST)
            .setPort(KSQLDB_SERVER_HOST_PORT);

    final Client client = Client.create(options);

    // Send requests with the client by following the other examples
    final String pullQuery = "select * from ridersNearMountainView;";
    final BatchedQueryResult batchedQueryResult = client.executeQuery(pullQuery);

    // Wait for query result
    final List<Row> resultRows = batchedQueryResult.get();

    System.out.println("Received results. Num rows: " + resultRows.size());
    for (Row row : resultRows) {
      System.out.println("Row: " + row.values());
    }

    // Terminate any open connections and close the client
    client.close();
  }
}
