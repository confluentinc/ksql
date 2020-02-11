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

package io.confluent.ksql.api.plugin;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.api.server.BasePublisher;
import io.confluent.ksql.api.server.PushQueryHandler;
import io.confluent.ksql.api.spi.QueryPublisher;
import io.vertx.core.Context;
import io.vertx.core.WorkerExecutor;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
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
public class BlockingQueryPublisher extends BasePublisher<GenericRow>
    implements QueryPublisher, Consumer<GenericRow> {

  private static final Logger log = LoggerFactory.getLogger(BlockingQueryPublisher.class);

  public static final int SEND_MAX_BATCH_SIZE = 10;
  public static final int BLOCKING_QUEUE_CAPACITY = 1000;

  private final BlockingQueue<GenericRow> queue = new LinkedBlockingQueue<>(
      BLOCKING_QUEUE_CAPACITY);
  private final WorkerExecutor workerExecutor;
  private PushQueryHandler queryHandle;
  private List<String> columnNames;
  private List<String> columnTypes;
  private OptionalInt limit;
  private int numAccepted;
  private boolean complete;
  private volatile boolean closed;

  public BlockingQueryPublisher(final Context ctx,
      final WorkerExecutor workerExecutor) {
    super(ctx);
    this.workerExecutor = Objects.requireNonNull(workerExecutor);
  }

  public void setQueryHandle(final PushQueryHandler queryHandle) {
    this.queryHandle = Objects.requireNonNull(queryHandle);
    this.limit = queryHandle.getLimit();
    this.columnNames = queryHandle.getColumnNames();
    this.columnTypes = queryHandle.getColumnTypes();
  }

  @Override
  public List<String> getColumnNames() {
    return columnNames;
  }

  @Override
  public List<String> getColumnTypes() {
    return columnTypes;
  }

  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    // Run async as it can block
    executeOnWorker(queryHandle::stop);
    super.close();
  }

  @Override
  public synchronized void accept(final GenericRow row) {
    Objects.requireNonNull(row);

    if (closed || complete || !hasReachedLimit()) {
      return;
    }

    while (!closed) {
      try {
        // Don't block for more than a little while each time to allow close to work
        if (queue.offer(row, 250, TimeUnit.MILLISECONDS)) {
          numAccepted++;
          maybeSend();
          return;
        }
      } catch (InterruptedException ignore) {
        return;
      }
    }
  }

  public int queueSize() {
    return queue.size();
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

  private boolean hasReachedLimit() {
    if (limit.isPresent()) {
      final int lim = limit.getAsInt();
      if (numAccepted == lim) {
        // Reached limit
        return false;
      }
      if (numAccepted == lim - 1) {
        // Set to complete after delivering any buffered rows
        complete = true;
      }
    }
    return true;
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
