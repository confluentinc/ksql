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

import io.confluent.ksql.physical.scalablepush.PushQueryQueuePopulator;
import io.confluent.ksql.physical.scalablepush.PushRouting.PushConnectionsHandle;
import io.confluent.ksql.query.BlockingRowQueue;
import io.confluent.ksql.query.LimitHandler;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

public class ScalablePushQueryMetadata implements PushQueryMetadata {

  private volatile boolean closed = false;
  private final LogicalSchema logicalSchema;
  private final QueryId queryId;
  private final BlockingRowQueue rowQueue;
  private final ResultType resultType;
  private final PushQueryQueuePopulator pushQueryQueuePopulator;

  private CompletableFuture<Void> future = new CompletableFuture<>();
  private final AtomicReference<PushConnectionsHandle> handleRef = new AtomicReference<>();

  public ScalablePushQueryMetadata(
      final LogicalSchema logicalSchema,
      final QueryId queryId,
      final BlockingRowQueue blockingRowQueue,
      final ResultType resultType,
      final PushQueryQueuePopulator pushQueryQueuePopulator
  ) {
    this.logicalSchema = logicalSchema;
    this.queryId = queryId;
    this.rowQueue = blockingRowQueue;
    this.resultType = resultType;
    this.pushQueryQueuePopulator = pushQueryQueuePopulator;
  }

  @Override
  public void start() {
    pushQueryQueuePopulator.run().thenApply(handle -> {
      handleRef.set(handle);
      handle.onException(future::completeExceptionally);
      return null;
    })
    .exceptionally(future::completeExceptionally);
  }

  @Override
  public void close() {
    rowQueue.close();
    final PushConnectionsHandle handle = handleRef.get();
    if (handle != null) {
      handle.close();
    }
    closed = true;
  }

  @Override
  public boolean isRunning() {
    return !closed;
  }

  @Override
  public BlockingRowQueue getRowQueue() {
    return rowQueue;
  }

  @Override
  public void setLimitHandler(final LimitHandler limitHandler) {
    rowQueue.setLimitHandler(limitHandler);
  }

  @Override
  public void setUncaughtExceptionHandler(final StreamsUncaughtExceptionHandler handler) {
    // We don't do anything special here since the persistent query handles its own errors
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
    future.exceptionally(t -> {
      consumer.accept(t);
      return null;
    });
  }
}
