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

import io.confluent.ksql.api.client.impl.ClientImpl;
import io.vertx.core.Vertx;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * A client that connects to a specific ksqlDB server.
 */
public interface Client {

  /**
   * Executes a query (push or pull) and returns the results one row at a time.
   *
   * <p>If a non-200 response is received from the server, the {@code CompletableFuture} will be
   * failed.
   *
   * <p>By default, push queries issued via this method return results starting from the beginning
   * of the stream or table. To override this behavior, use the method
   * {@link #streamQuery(String, Map)} to pass in the query property {@code auto.offset.reset}
   * with value set to {@code latest}.
   *
   * @param sql statement of query to execute
   * @return a future that completes once the server response is received, and contains the query
   *         result if successful
   */
  CompletableFuture<StreamedQueryResult> streamQuery(String sql);

  /**
   * Executes a query (push or pull) and returns the results one row at a time.
   *
   * <p>If a non-200 response is received from the server, the {@code CompletableFuture} will be
   * failed.
   *
   * <p>By default, push queries issued via this method return results starting from the beginning
   * of the stream or table. To override this behavior, pass in the query property
   * {@code auto.offset.reset} with value set to {@code latest}.
   *
   * @param sql statement of query to execute
   * @param properties query properties
   * @return a future that completes once the server response is received, and contains the query
   *         result if successful
   */
  CompletableFuture<StreamedQueryResult> streamQuery(String sql, Map<String, Object> properties);

  /**
   * Executes a query (push or pull) and returns all result rows in a single batch, once the query
   * has completed.
   *
   * <p>By default, push queries issued via this method return results starting from the beginning
   * of the stream or table. To override this behavior, use the method
   * {@link #executeQuery(String, Map)} to pass in the query property {@code auto.offset.reset}
   * with value set to {@code latest}.
   *
   * @param sql statement of query to execute
   * @return query result
   */
  BatchedQueryResult executeQuery(String sql);

  /**
   * Executes a query (push or pull) and returns all result rows in a single batch, once the query
   * has completed.
   *
   * <p>By default, push queries issued via this method return results starting from the beginning
   * of the stream or table. To override this behavior, pass in the query property
   * {@code auto.offset.reset} with value set to {@code latest}.
   *
   * @param sql statement of query to execute
   * @param properties query properties
   * @return query result
   */
  BatchedQueryResult executeQuery(String sql, Map<String, Object> properties);

  /**
   * Inserts a row into a ksqlDB stream.
   *
   * <p>The {@code CompletableFuture} will be failed if a non-200 response is received from the
   * server, or if the server encounters an error while processing the insertion.
   *
   * @param streamName name of the target stream
   * @param row the row to insert. Keys are column names and values are column values.
   * @return a future that completes once the server response is received
   */
  CompletableFuture<Void> insertInto(String streamName, KsqlObject row);

  /**
   * Terminates a push query with the specified query ID.
   *
   * <p>If a non-200 response is received from the server, the {@code CompletableFuture} will be
   * failed.
   *
   * @param queryId ID of the query to terminate
   * @return a future that completes once the server response is received
   */
  CompletableFuture<Void> terminatePushQuery(String queryId);

  /**
   * Closes the underlying HTTP client.
   */
  void close();

  static Client create(ClientOptions clientOptions) {
    return new ClientImpl(clientOptions);
  }

  static Client create(ClientOptions clientOptions, Vertx vertx) {
    return new ClientImpl(clientOptions, vertx);
  }
}