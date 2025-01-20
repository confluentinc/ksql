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

package io.confluent.ksql.api.impl;

import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.api.server.QueryHandle;
import io.confluent.ksql.api.spi.QueryPublisher;
import io.confluent.ksql.query.BlockingRowQueue;
import io.confluent.ksql.query.PullQueryWriteStream;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.reactive.BasePublisher;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KeyValueMetadata;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.WorkerExecutor;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A query publisher that uses an internal blocking queue to store rows for delivery. It's currently
 * necessary to use a blocking queue as Kafka Streams delivers message in a synchronous fashion with
 * no back pressure. If the queue was not blocking then if the subscriber was slow the messages
 * could build up on the queue eventually resulting in out of memory. The only mechanism we have to
 * slow streams down is to block the thread. Kafka Streams uses dedicated streams per topology so
 * this won't prevent the thread from doing useful work elsewhere but it does mean we can't have too
 * many push queries in the server at any one time as we can end up with a lot of threads.
 */
public class BlockingQueryPublisher extends BasePublisher<KeyValueMetadata<List<?>, GenericRow>>
    implements QueryPublisher {

  private static final Logger log = LoggerFactory.getLogger(BlockingQueryPublisher.class);

  public static final int SEND_MAX_BATCH_SIZE = 200;

  private final WorkerExecutor workerExecutor;
  private BlockingRowQueue queue;
  private boolean isPullQuery;
  private boolean isScalablePushQuery;
  private QueryHandle queryHandle;
  private ImmutableList<String> columnNames;
  private ImmutableList<String> columnTypes;
  private LogicalSchema logicalSchema;
  private QueryId queryId;
  private boolean complete;
  private boolean hitLimit;
  private volatile boolean closed;

  public BlockingQueryPublisher(final Context ctx,
      final WorkerExecutor workerExecutor) {
    super(ctx);
    this.workerExecutor = Objects.requireNonNull(workerExecutor);
  }

  public void setQueryHandle(final QueryHandle queryHandle, final boolean isPullQuery,
      final boolean isScalablePushQuery) {
    this.columnNames = ImmutableList.copyOf(queryHandle.getColumnNames());
    this.columnTypes = ImmutableList.copyOf(queryHandle.getColumnTypes());
    this.logicalSchema = queryHandle.getLogicalSchema();
    this.queue = queryHandle.getQueue();
    this.isPullQuery = isPullQuery;
    this.isScalablePushQuery = isScalablePushQuery;
    this.queryId = queryHandle.getQueryId();
    this.queue.setQueuedCallback(this::maybeSend);
    this.queue.setLimitHandler(() -> {
      if (isPullQuery) {
        queryHandle.getConsistencyOffsetVector().ifPresent(
            ((PullQueryWriteStream) queue)::putConsistencyVector);
        maybeSend();
      }
      complete = true;
      hitLimit = true;
      // This allows us to hit the limit without having to queue one last row
      if (queue.isEmpty()) {
        ctx.runOnContext(v -> sendComplete());
      }
    });
    // Justification for duplicated code:
    // The way we handle query completion right now happens to be the same as the way
    // we handle limit above, but this is not necessarily going to stay the same. For example,
    // we should be returning a "Limit Reached" message as we do in the HTTP/1 endpoint when
    // we hit the limit, but for query completion, we should just end the response stream.
    this.queue.setCompletionHandler(() -> {
      if (isPullQuery) {
        queryHandle.getConsistencyOffsetVector().ifPresent(
            ((PullQueryWriteStream) queue)::putConsistencyVector);
        maybeSend();
      }
      complete = true;
      // This allows us to finish the query immediately if the query is already fully streamed.
      if (queue.isEmpty()) {
        ctx.runOnContext(v -> sendComplete());
      }
    });
    this.queryHandle = queryHandle;
    queryHandle.onException(t -> ctx.runOnContext(v -> sendError(t)));
  }

  @Override
  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "columnNames is ImmutableList")
  public List<String> getColumnNames() {
    return columnNames;
  }

  @Override
  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "columnTypes is ImmutableList")
  public List<String> getColumnTypes() {
    return columnTypes;
  }

  @Override
  public LogicalSchema geLogicalSchema() {
    return logicalSchema;
  }

  public Future<Void> close() {
    if (closed) {
      return Future.succeededFuture();
    }
    closed = true;
    // Run async as it can block
    executeOnWorker(queryHandle::stop);
    return super.close();
  }

  @Override
  public boolean isPullQuery() {
    return isPullQuery;
  }

  @Override
  public boolean isScalablePushQuery() {
    return isScalablePushQuery;
  }

  @Override
  public QueryId queryId() {
    return queryId;
  }

  @Override
  public boolean hitLimit() {
    return hitLimit;
  }

  @Override
  protected void maybeSend() {
    ctx.runOnContext(v -> doSend());
  }

  @Override
  protected void afterSubscribe() {
    // Run async as it can block
    executeOnWorker(queryHandle::start);
  }

  private void executeOnWorker(final Runnable runnable) {
    workerExecutor.executeBlocking(p -> runnable.run(), false, ar -> {
      if (ar.failed()) {
        log.error("Failed to close query", ar.cause());
      }
    });
  }

  @SuppressFBWarnings(
      value = "IS2_INCONSISTENT_SYNC",
      justification = "Vert.x ensures this is executed on event loop only")
  private void doSend() {
    checkContext();
    int num = 0;
    while (getDemand() > 0 && !queue.isEmpty()) {
      if (num < SEND_MAX_BATCH_SIZE) {
        doOnNext(queue.poll());
        if (complete && queue.isEmpty()) {
          ctx.runOnContext(v -> sendComplete());
        }
        num++;
      } else {
        // Schedule another batch async
        ctx.runOnContext(v -> doSend());
        break;
      }
    }
  }
}
