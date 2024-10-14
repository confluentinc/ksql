/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.util;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.execution.scalablepush.PushQueryPreparer;
import io.confluent.ksql.execution.scalablepush.PushQueryQueuePopulator;
import io.confluent.ksql.execution.scalablepush.PushRouting.PushConnectionsHandle;
import io.confluent.ksql.internal.ScalablePushQueryMetrics;
import io.confluent.ksql.query.CompletionHandler;
import io.confluent.ksql.query.LimitHandler;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.query.TransientQueryQueue;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlConstants.QuerySourceType;
import io.confluent.ksql.util.KsqlConstants.RoutingNodeType;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

public class ScalablePushQueryMetadata implements PushQueryMetadata {

  private volatile boolean closed = false;
  private final LogicalSchema logicalSchema;
  private final QueryId queryId;
  private final TransientQueryQueue transientQueryQueue;
  private final Optional<ScalablePushQueryMetrics> scalablePushQueryMetrics;
  private final ResultType resultType;
  private final PushQueryQueuePopulator pushQueryQueuePopulator;
  private final PushQueryPreparer pushQueryPreparer;
  private final QuerySourceType sourceType;
  private final RoutingNodeType routingNodeType;
  private final Supplier<Long> rowsProcessedSupplier;


  // Future for the start of the connections, which creates a handle
  private CompletableFuture<PushConnectionsHandle> startFuture = new CompletableFuture<>();
  // Future for the operation of the SPQ. Since there's no defined ending (since limit is enforced
  // on the forwarding node), this is tracks errors.
  private CompletableFuture<Void> runningFuture = new CompletableFuture<>();

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public ScalablePushQueryMetadata(
      final LogicalSchema logicalSchema,
      final QueryId queryId,
      final TransientQueryQueue transientQueryQueue,
      final Optional<ScalablePushQueryMetrics> scalablePushQueryMetrics,
      final ResultType resultType,
      final PushQueryQueuePopulator pushQueryQueuePopulator,
      final PushQueryPreparer pushQueryPreparer,
      final QuerySourceType sourceType,
      final RoutingNodeType routingNodeType,
      final Supplier<Long> rowsProcessedSupplier
  ) {
    this.logicalSchema = logicalSchema;
    this.queryId = queryId;
    this.transientQueryQueue = transientQueryQueue;
    this.scalablePushQueryMetrics = scalablePushQueryMetrics;
    this.resultType = resultType;
    this.pushQueryQueuePopulator = pushQueryQueuePopulator;
    this.pushQueryPreparer = pushQueryPreparer;
    this.sourceType = sourceType;
    this.routingNodeType = routingNodeType;
    this.rowsProcessedSupplier = rowsProcessedSupplier;

  }

  /**
   * Prepare to start. Any exceptions thrown here will result in an error return code rather than
   * an error written to the stream.
   */
  public void prepare() {
    // Any exceptions aren't meant to trickle up to the caller.  This will result in non ok error
    // codes and is good for fast failing.
    pushQueryPreparer.prepare();
  }

  @Override
  public void start() {
    CompletableFuture.completedFuture(null)
        .thenCompose(v -> pushQueryQueuePopulator.run())
        .thenApply(handle -> {
          startFuture.complete(handle);
          handle.onException(runningFuture::completeExceptionally);
          return null;
        }).exceptionally(t -> {
          startFuture.completeExceptionally(t);
          runningFuture.completeExceptionally(t);
          return null;
        });
  }

  @Override
  public void close() {
    transientQueryQueue.close();
    startFuture.thenApply(handle -> {
      handle.close();
      return null;
    });
    closed = true;
  }

  @Override
  public boolean isRunning() {
    return !closed;
  }

  @Override
  @SuppressFBWarnings(value = "EI_EXPOSE_REP")
  public TransientQueryQueue getRowQueue() {
    return transientQueryQueue;
  }

  @Override
  public void setLimitHandler(final LimitHandler limitHandler) {
    transientQueryQueue.setLimitHandler(limitHandler);
  }

  @Override
  public void setCompletionHandler(final CompletionHandler completionHandler) {
    transientQueryQueue.setCompletionHandler(completionHandler);
  }

  @Override
  public void setUncaughtExceptionHandler(final StreamsUncaughtExceptionHandler handler) {
    onException(handler::handle);
  }

  @Override
  public LogicalSchema getLogicalSchema() {
    return logicalSchema;
  }

  @Override
  public QueryId getQueryId() {
    return queryId;
  }

  @Override
  public ResultType getResultType() {
    return resultType;
  }

  public void onException(final Consumer<Throwable> consumer) {
    runningFuture.exceptionally(t -> {
      scalablePushQueryMetrics.ifPresent(metrics ->
          metrics.recordErrorRate(1, sourceType, routingNodeType));
      consumer.accept(t);
      return null;
    });
  }

  public void onCompletion(final Consumer<Void> consumer) {
    runningFuture.thenAccept(consumer);
  }

  public void onCompletionOrException(final BiConsumer<Void, Throwable> biConsumer) {
    runningFuture.handle((v, t) -> {
      biConsumer.accept(v, t);
      return null;
    });
  }

  public QuerySourceType getSourceType() {
    return sourceType;
  }

  public RoutingNodeType getRoutingNodeType() {
    return routingNodeType;
  }

  public long getTotalRowsReturned() {
    return transientQueryQueue.getTotalRowsQueued();
  }

  public long getTotalRowsProcessed() {
    return rowsProcessedSupplier.get();
  }
}
