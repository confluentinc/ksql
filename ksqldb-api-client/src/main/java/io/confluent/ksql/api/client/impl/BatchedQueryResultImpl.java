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

package io.confluent.ksql.api.client.impl;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.api.client.BatchedQueryResult;
import io.confluent.ksql.api.client.exception.KsqlClientException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

public class BatchedQueryResultImpl extends BatchedQueryResult {

  private final CompletableFuture<String> queryId;
  private final Optional<AtomicReference<String>> continuationToken;
  private final String sql;
  private final Map<String, Object> properties;
  private final ClientImpl client;

  BatchedQueryResultImpl(
      final Optional<AtomicReference<String>> continuationToken,
      final String sql,
      final Map<String, Object> properties,
      final ClientImpl client
  ) {
    this.queryId = new CompletableFuture<>();

    this.exceptionally(t -> {
      queryId.completeExceptionally(t);
      return null;
    });
    this.continuationToken = continuationToken;
    this.sql = sql;
    this.properties = properties;
    this.client = client;
  }

  @Override
  @SuppressFBWarnings(value = "EI_EXPOSE_REP")
  public CompletableFuture<String> queryID() {
    return queryId;
  }

  @Override
  public boolean hasContinuationToken() {
    return this.continuationToken.isPresent() && this.continuationToken.get().get() != null;
  }

  @Override
  public Optional<AtomicReference<String>> getContinuationToken() {
    return this.continuationToken;
  }

  @Override
  public BatchedQueryResult retry() {
    if (!this.hasContinuationToken()) {
      throw new KsqlClientException("Can only retry queries that have saved a continuation token.");
    }
    return this.client.executeQuery(sql, properties);
  }
}
