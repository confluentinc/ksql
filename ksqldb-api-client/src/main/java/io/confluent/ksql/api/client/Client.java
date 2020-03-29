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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.reactivestreams.Publisher;

public interface Client {

  /**
   * Execute a push query.
   *
   * @param sql statement of query to execute.
   * @return query result.
   */
  CompletableFuture<QueryResult> streamQuery(String sql);

  /**
   * Execute a push query.
   *
   * @param sql statement of query to execute.
   * @param properties query properties.
   * @return query result.
   */
  CompletableFuture<QueryResult> streamQuery(String sql, Map<String, Object> properties);

  /**
   * Execute a pull query.
   *
   * @param sql statement of query to execute.
   * @return query result.
   */
  CompletableFuture<List<Row>> executeQuery(String sql);

  /**
   * Execute a pull query.
   *
   * @param sql statement of query to execute.
   * @param properties query properties.
   * @return query result.
   */
  CompletableFuture<List<Row>> executeQuery(String sql, Map<String, Object> properties);

  CompletableFuture<Void> insertInto(String streamName, Map<String, Object> row);

  Publisher<InsertAck> streamInserts(String streamName, Publisher<List<Object>> insertsPublisher);

  void close();

  static Client create(ClientOptions clientOptions) {
    return new ClientImpl(clientOptions);
  }
}