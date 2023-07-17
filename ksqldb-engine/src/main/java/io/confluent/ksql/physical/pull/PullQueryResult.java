/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.physical.pull;

import com.google.common.base.Preconditions;
import io.confluent.ksql.internal.PullQueryExecutorMetrics;
import io.confluent.ksql.query.PullQueryQueue;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class PullQueryResult {

  private final LogicalSchema schema;
  private final PullQueryQueuePopulator populator;
  private final QueryId queryId;
  private final PullQueryQueue pullQueryQueue;
  private final Optional<PullQueryExecutorMetrics> pullQueryMetrics;

  // This future is used to keep track of all of the callbacks since we allow for adding them both
  // before and after the pull query has been started.  When the pull query has completed, it will
  // pass on the outcome to this future.
  private CompletableFuture<Void> future = new CompletableFuture<>();
  private boolean started = false;

  public PullQueryResult(
      final LogicalSchema schema,
      final PullQueryQueuePopulator populator,
      final QueryId queryId,
      final PullQueryQueue pullQueryQueue,
      final Optional<PullQueryExecutorMetrics> pullQueryMetrics
  ) {
    this.schema = schema;
    this.populator = populator;
    this.queryId = queryId;
    this.pullQueryQueue = pullQueryQueue;
    this.pullQueryMetrics = pullQueryMetrics;
  }

  public LogicalSchema getSchema() {
    return schema;
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public PullQueryQueue getPullQueryQueue() {
    return pullQueryQueue;
  }

  public void start() {
    Preconditions.checkState(!started, "Should only start once");
    started = true;
    final CompletableFuture<Void> f = populator.run();
    f.exceptionally(t -> {
      future.completeExceptionally(t);
      return null;
    });
    f.thenAccept(future::complete);
  }

  public void stop() {
    pullQueryQueue.close();
  }

  public void onException(final Consumer<Throwable> consumer) {
    future.exceptionally(t -> {
      pullQueryMetrics.ifPresent(metrics -> metrics.recordErrorRate(1));
      consumer.accept(t);
      return null;
    });
  }

  public void onCompletion(final Consumer<Void> consumer) {
    future.thenAccept(consumer::accept);
  }

  public void onCompletionOrException(final BiConsumer<Void, Throwable> biConsumer) {
    future.handle((v, t) -> {
      biConsumer.accept(v, t);
      return null;
    });
  }
}
