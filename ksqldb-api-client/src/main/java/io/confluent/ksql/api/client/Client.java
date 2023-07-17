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
import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.reactivestreams.Publisher;

/**
 * A client that connects to a specific ksqlDB server.
 */
public interface Client extends Closeable {

  /**
   * Executes a query (push or pull) and returns the results one row at a time.
   *
   * <p>This method may be used to issue both push and pull queries, but the usage
   * pattern is better for push queries. For pull queries, consider using the
   * {@link Client#executeQuery(String)} method instead.
   *
   * <p>If a non-200 response is received from the server, the {@code CompletableFuture} will be
   * failed.
   *
   * @param sql statement of query to execute
   * @return a future that completes once the server response is received, and contains the query
   *         result if successful
   */
  CompletableFuture<StreamedQueryResult> streamQuery(String sql);

  /**
   * Executes a query (push or pull) and returns the results one row at a time.
   *
   * <p>This method may be used to issue both push and pull queries, but the usage
   * pattern is better for push queries. For pull queries, consider using the
   * {@link Client#executeQuery(String, Map)} method instead.
   *
   * <p>If a non-200 response is received from the server, the {@code CompletableFuture} will be
   * failed.
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
   * <p>This method is suitable for both pull queries and for terminating push queries,
   * for example, queries that have a {@code LIMIT} clause. For non-terminating push queries,
   * use the {@link Client#streamQuery(String)} method instead.
   *
   * @param sql statement of query to execute
   * @return query result
   */
  BatchedQueryResult executeQuery(String sql);

  /**
   * Executes a query (push or pull) and returns all result rows in a single batch, once the query
   * has completed.
   *
   * <p>This method is suitable for both pull queries and for terminating push queries,
   * for example, queries that have a {@code LIMIT} clause. For non-terminating push queries,
   * use the {@link Client#streamQuery(String, Map)} method instead.
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
   * Inserts rows into a ksqlDB stream. Rows to insert are supplied by a
   * {@code org.reactivestreams.Publisher} and server acknowledgments are exposed similarly.
   *
   * <p>The {@code CompletableFuture} will be failed if a non-200 response is received from the
   * server.
   *
   * <p>See {@link InsertsPublisher} for an example publisher that may be passed an argument to
   * this method.
   *
   * @param streamName name of the target stream
   * @param insertsPublisher the publisher to provide rows to insert
   * @return a future that completes once the initial server response is received, and contains a
   *         publisher that publishes server acknowledgments for inserted rows.
   */
  CompletableFuture<AcksPublisher>
      streamInserts(String streamName, Publisher<KsqlObject> insertsPublisher);

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
   * Sends a SQL request to the ksqlDB server. This method supports 'CREATE', 'CREATE ... AS
   * SELECT', 'DROP', 'TERMINATE', and 'INSERT INTO ... AS SELECT' statements.
   *
   * <p>Each request should contain exactly one statement. Requests that contain multiple statements
   * will be rejected by the client, in the form of failing the {@code CompletableFuture}, and the
   * request will not be sent to the server.
   *
   * <p>The {@code CompletableFuture} is completed once a response is received from the server.
   * Note that the actual execution of the submitted statement is asynchronous, so the statement
   * may not have been executed by the time the {@code CompletableFuture} is completed.
   *
   * <p>If a non-200 response is received from the server, the {@code CompletableFuture} will be
   * failed.
   *
   * @param sql the request to be executed
   * @return a future that completes once the server response is received, and contains the query ID
   *         for statements that start new persistent queries
   */
  CompletableFuture<ExecuteStatementResult> executeStatement(String sql);

  /**
   * Sends a SQL request with the specified properties to the ksqlDB server. This method supports
   * 'CREATE', 'CREATE ... AS SELECT', 'DROP', 'TERMINATE', and 'INSERT INTO ... AS SELECT'
   * statements.
   *
   * <p>Each request should contain exactly one statement. Requests that contain multiple statements
   * will be rejected by the client, in the form of failing the {@code CompletableFuture}, and the
   * request will not be sent to the server.
   *
   * <p>The {@code CompletableFuture} is completed once a response is received from the server.
   * Note that the actual execution of the submitted statement is asynchronous, so the statement
   * may not have been executed by the time the {@code CompletableFuture} is completed.
   *
   * <p>If a non-200 response is received from the server, the {@code CompletableFuture} will be
   * failed.
   *
   * @param sql the request to be executed
   * @param properties properties associated with the request
   * @return a future that completes once the server response is received, and contains the query ID
   *         for statements that start new persistent queries
   */
  CompletableFuture<ExecuteStatementResult>
      executeStatement(String sql, Map<String, Object> properties);

  /**
   * Returns the list of ksqlDB streams from the ksqlDB server's metastore.
   *
   * <p>If a non-200 response is received from the server, the {@code CompletableFuture} will be
   * failed.
   *
   * @return list of streams
   */
  CompletableFuture<List<StreamInfo>> listStreams();

  /**
   * Returns the list of ksqlDB tables from the ksqlDB server's metastore
   *
   * <p>If a non-200 response is received from the server, the {@code CompletableFuture} will be
   * failed.
   *
   * @return list of tables
   */
  CompletableFuture<List<TableInfo>> listTables();

  /**
   * Returns the list of Kafka topics available for use with ksqlDB.
   *
   * <p>If a non-200 response is received from the server, the {@code CompletableFuture} will be
   * failed.
   *
   * @return list of topics
   */
  CompletableFuture<List<TopicInfo>> listTopics();

  /**
   * Returns the list of queries currently running on the ksqlDB server.
   *
   * <p>If a non-200 response is received from the server, the {@code CompletableFuture} will be
   * failed.
   *
   * @return list of queries
   */
  CompletableFuture<List<QueryInfo>> listQueries();

  /**
   * Returns metadata about the ksqlDB stream or table of the provided name.
   *
   * <p>If a non-200 response is received from the server, the {@code CompletableFuture} will be
   * failed.
   *
   * @param sourceName stream or table name
   * @return metadata for stream or table
   */
  CompletableFuture<SourceDescription> describeSource(String sourceName);

  /**
   * Returns metadata about the ksqlDB server.
   *
   * <p>If a non-200 response is received from the server, the {@code CompletableFuture} will be
   * failed.
   *
   * @return metadata for server
   */
  CompletableFuture<ServerInfo> serverInfo();

  /**
   * Creates a connector.
   *
   * <p>If a non-200 response is received from the server, the {@code CompletableFuture} will be
   * failed.
   *
   * @param connectorName name of the connector
   * @param isSource true if the connector is a source connector, false if it is a sink connector
   * @param properties connector properties
   * @return result of connector creation
   */
  CompletableFuture<Void> createConnector(
      String connectorName, boolean isSource, Map<String, Object> properties);

  /**
   * Drops a connector.
   *
   * <p>If a non-200 response is received from the server, the {@code CompletableFuture} will be
   * failed.
   *
   * @param connectorName name of the connector to drop
   * @return a future that completes once the server response is received
   */
  CompletableFuture<Void> dropConnector(String connectorName);

  /**
   * Returns a list of connectors.
   *
   * <p>If a non-200 response is received from the server, the {@code CompletableFuture} will be
   * failed.
   *
   * @return a list of connectors
   */
  CompletableFuture<List<ConnectorInfo>> listConnectors();

  /**
   * Returns metadata about a connector.
   *
   * <p>If a non-200 response is received from the server, the {@code CompletableFuture} will be
   * failed.
   *
   * @param connectorName name of the connector to describe
   * @return metadata about connector
   */
  CompletableFuture<ConnectorDescription> describeConnector(String connectorName);

  /**
   * Define a session variable which can be referenced in sql commands by wrapping the variable name
   * with {@code ${}}.
   *
   * <p>If the variable is already defined, the existing value will be overridden.
   *
   * @param variable the name of the variable
   * @param value the value represented by the variable
   */
  void define(String variable, Object value);

  /**
   * Undefine a session variable. {@code '${<variable name>}'} will no longer be replaced in other
   * functions.
   *
   * <p>If the variable is not defined, then this method call is a no-op.
   *
   * @param variable the name of the variable to undefine
   */
  void undefine(String variable);

  /**
   * @return a map of the session variables and values used for variable substitution.
   */
  Map<String, Object> getVariables();

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