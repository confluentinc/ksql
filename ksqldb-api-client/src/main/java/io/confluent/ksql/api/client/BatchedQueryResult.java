/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.api.client;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * The result of a query (push or pull), returned as a single batch once the query has finished
 * executing, or the query has been terminated. For non-terminating push queries,
 * {@link StreamedQueryResult} should be used instead.
 *
 * <p>If a non-200 response is received from the server, this future will complete exceptionally.
 *
 * <p>The maximum number of {@code Row}s that may be returned from a {@code BatchedQueryResult}
 * defaults to {@link ClientOptions#DEFAULT_EXECUTE_QUERY_MAX_RESULT_ROWS} and can be configured
 * via {@link ClientOptions#setExecuteQueryMaxResultRows(int)}.
 */
public abstract class BatchedQueryResult extends CompletableFuture<List<Row>> {

  /**
   * Returns a {@code CompletableFuture} containing the ID of the underlying query if the query is
   * a push query, else null. The future is completed once a response is received from the server.
   *
   * <p>If a non-200 response is received from the server, this future will complete exceptionally.
   *
   * @return a future containing the query ID (or null in the case of pull queries)
   */
  @SuppressFBWarnings(value = "EI_EXPOSE_REP")
  public abstract CompletableFuture<String> queryID();

}
