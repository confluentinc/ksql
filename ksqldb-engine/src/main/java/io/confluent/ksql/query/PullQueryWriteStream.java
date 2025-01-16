/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.query;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Monitor;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.physical.pull.PullQueryRow;
import io.confluent.ksql.physical.pull.StreamedRowTranslator;
import io.confluent.ksql.rest.entity.ConsistencyToken;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.util.ConsistencyOffsetVector;
import io.confluent.ksql.util.KeyValue;
import io.confluent.ksql.util.KeyValueMetadata;
//import io.confluent.ksql.util.KsqlHostInfo;
import io.confluent.ksql.util.RowMetadata;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.impl.future.SucceededFuture;
import io.vertx.core.streams.WriteStream;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.List;
import java.util.OptionalInt;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.GuardedBy;

/**
 * This {@link WriteStream} allows for results to be streamed back to the client when
 * running pull queries. Streaming behavior is important when dealing with large result
 * sets since we don't want to hold the entire set in memory at once.
 *
 * <p>This class has non-blocking writes so that it can work both with callers that implement
 * backpressure and those that don't. If the caller does not implement backpressure, they
 * should block when {@link #writeQueueFull()} by calling {@link #awaitCapacity(long, TimeUnit)}.
 */
@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class PullQueryWriteStream implements WriteStream<List<StreamedRow>>, BlockingRowQueue {

  private static final int DEFAULT_SOFT_CAPACITY = 50;

  private final OptionalInt queryLimit;
  private final StreamedRowTranslator translator;

  // this monitor allows us to implement blocking reads on the poll()
  // methods and also is used to ensure thread safety if multiple threads
  // are using this WriteStream
  private final Monitor monitor = new Monitor();

  @GuardedBy("monitor")
  private final Queue<HandledRow> queue = new ArrayDeque<>();
  @GuardedBy("monitor")
  private int totalRowsQueued = 0;

  @GuardedBy("monitor")
  private boolean closed = false;
  @GuardedBy("monitor")
  private int queueCapacity = DEFAULT_SOFT_CAPACITY;

  private final Monitor.Guard hasData = monitor.newGuard(() -> !isEmpty());
  private final Monitor.Guard atHalfCapacity = monitor.newGuard(
      () -> isDone() || size() <= queueCapacity / 2);

  // this is a bit of a leaky abstraction that is necessary because
  // Vertx's PipeImpl sets the drain handler every single time the
  // soft limit is hit. because we use the same write stream for
  // multiple read streams, we need to make sure that we maintain
  // all of them but only one copy of them. perhaps a better solution
  // is to just write our own version of PipeImpl that works natively
  // with multiple ReadStreams
  private final ConcurrentHashSet<Handler<Void>> drainHandler = new ConcurrentHashSet<>();

  private CompletionHandler endHandler = () -> { };
  private Handler<AsyncResult<Void>> limitHandler = ar -> { };
  private Runnable queueCallback = () -> { };

  public PullQueryWriteStream(
      final OptionalInt queryLimit,
      final StreamedRowTranslator translator
  ) {
    this.queryLimit = queryLimit;
    this.translator = translator;

    // register a drainHandler that will wake up anyone waiting on hasCapacity
    drainHandler.add(ignored -> {
      monitor.enter();
      monitor.leave();
    });
  }

  private static final class HandledRow {
    private final PullQueryRow row;
    private final Handler<AsyncResult<Void>> handler;

    private HandledRow(
        final PullQueryRow row,
        final Handler<AsyncResult<Void>> handler
    ) {
      this.row = row;
      this.handler = handler;
    }
  }

  // -------------------- READ QUEUE METHODS -------------------------

  @Override
  public void drainTo(final Collection<? super KeyValueMetadata<List<?>, GenericRow>> collection) {
    monitor.enter();
    try {
      while (!queue.isEmpty()) {
        collection.add(poll());
      }
    } finally {
      monitor.leave();
    }
  }

  public void drainRowsTo(final Collection<PullQueryRow> collection) {
    monitor.enter();
    try {
      while (!queue.isEmpty()) {
        collection.add(pollRow());
      }
    } finally {
      monitor.leave();
    }
  }

  @Override
  public KeyValueMetadata<List<?>, GenericRow> poll(
      final long timeout,
      final TimeUnit unit
  ) throws InterruptedException {
    if (monitor.enterWhen(hasData, timeout, unit)) {
      try {
        return poll();
      } finally {
        monitor.leave();
      }
    }

    return null;
  }

  @Override
  public KeyValueMetadata<List<?>, GenericRow> poll() {
    final PullQueryRow row = pollRow();
    if (row == null) {
      return null;
    }

    if (row.getConsistencyOffsetVector().isPresent()) {
      return new KeyValueMetadata<>(RowMetadata.of(row.getConsistencyOffsetVector().get()));
    }

    //return new KeyValueMetadata<>(
    //    KeyValue.keyValue(null, row.getGenericRow()),
    //    row.getSourceNode()
    //        .map(sn -> new KsqlHostInfo(sn.getHost(), sn.getPort()))
    //        .map(RowMetadata::of)
    //);

    return new KeyValueMetadata<>(KeyValue.keyValue(null, row.getGenericRow()));
  }

  public PullQueryRow pollRow(
      final long timeout,
      final TimeUnit unit
  ) throws InterruptedException {
    if (monitor.enterWhen(hasData, timeout, unit)) {
      try {
        return pollRow();
      } finally {
        monitor.leave();
      }
    }

    return null;
  }

  private PullQueryRow pollRow() {
    final HandledRow polled;
    monitor.enter();
    try {
      polled = queue.poll();
    } finally {
      monitor.leave();
    }

    if (polled == null) {
      return null;
    }

    polled.handler.handle(new SucceededFuture<>(null, null));

    if (monitor.enterIf(atHalfCapacity)) {
      try {
        drainHandler.forEach(h -> h.handle(null));
      } finally {
        monitor.leave();
      }
    }

    return polled.row;
  }

  public int getTotalRowsQueued() {
    monitor.enter();
    try {
      // pass by value, this should be safe not to be changed elsewhere
      return totalRowsQueued;
    } finally {
      monitor.leave();
    }
  }

  @Override
  public int size() {
    monitor.enter();
    try {
      return queue.size();
    } finally {
      monitor.leave();
    }
  }

  @Override
  public boolean isEmpty() {
    return size() == 0;
  }

  public boolean isDone() {
    monitor.enter();
    try {
      return closed || hardLimitHit();
    } finally {
      monitor.leave();
    }
  }

  private boolean hardLimitHit() {
    monitor.enter();
    try {
      return queryLimit.isPresent() && totalRowsQueued >= queryLimit.getAsInt();
    } finally {
      monitor.leave();
    }
  }

  // -------------------- HANDLER SETTERS -------------------------


  @Override
  public PullQueryWriteStream exceptionHandler(final Handler<Throwable> handler) {
    // we don't currently use this but may have use for it in the future
    return this;
  }

  @Override
  public PullQueryWriteStream drainHandler(final Handler<Void> handler) {
    drainHandler.add(handler);
    return this;
  }

  @Override
  public void setCompletionHandler(final CompletionHandler handler) {
    this.endHandler = handler;
  }

  @Override
  public void setLimitHandler(final LimitHandler handler) {
    this.limitHandler = ar -> handler.limitReached();
  }

  @Override
  public void setQueuedCallback(final Runnable callback) {
    final Runnable parent = queueCallback;

    queueCallback = () -> {
      parent.run();
      callback.run();
    };
  }


  // -------------------- WRITE STREAM METHODS -------------------------

  public void putConsistencyVector(final ConsistencyOffsetVector offsetVector) {
    write(ImmutableList.of(
        StreamedRow.consistencyToken(new ConsistencyToken(offsetVector.serialize()))
    ));
  }

  @Override
  public Future<Void> write(final List<StreamedRow> data) {
    final Promise<Void> promise = Promise.promise();
    write(data,promise);
    return promise.future();
  }

  @Override
  public void write(
      final List<StreamedRow> data,
      final Handler<AsyncResult<Void>> handler
  ) {
    monitor.enter();
    try {
      if (isDone()) {
        return;
      }
      for (final PullQueryRow row: translator.apply(data)) {
        if (queue.offer(new HandledRow(row, handler))) {
          totalRowsQueued++;
          queueCallback.run();
          if (hardLimitHit()) {
            // check if the last row enqueued caused us to break the limit, in which case
            // we should signal the end of the WriteStream
            end(limitHandler);
            break;
          }
        }
      }
    } finally {
      monitor.leave();
    }
  }

  @Override
  public void close() {
    end();
  }

  @Override
  public void end(final Handler<AsyncResult<Void>> handler) {
    monitor.enter();
    try {
      closed = true;
    } finally {
      monitor.leave();
    }

    endHandler.complete();
    handler.handle(new SucceededFuture<>(null, null));
  }

  @Override
  public PullQueryWriteStream setWriteQueueMaxSize(final int maxSize) {
    monitor.enter();
    try {
      queueCapacity = maxSize;
    } finally {
      monitor.leave();
    }
    return this;
  }

  @Override
  public boolean writeQueueFull() {
    monitor.enter();
    try {
      return isDone() || queue.size() >= queueCapacity;
    } finally {
      monitor.leave();
    }
  }

  /**
   * Waits for the supplied timeout for the queue to have capacity. This is preferred to
   * registering an additional {@link #drainHandler(Handler)} on the queue to wait because
   * it uses the same synchronization mechanism as the rest of this class and therefore does
   * not have the same risk of deadlocking.
   *
   * <p>Note that this is a "best effort" mechanism since multiple threads may concurrently
   * be notified of capacity.
   *
   * @return whether the queue had capacity at the end of the timeout
   */
  public boolean awaitCapacity(
      final long timeout,
      final TimeUnit timeUnit
  ) throws InterruptedException {
    if (!writeQueueFull()) {
      return true;
    }

    if (monitor.enterWhen(atHalfCapacity, timeout, timeUnit)) {
      try {
        return !writeQueueFull();
      } finally {
        monitor.leave();
      }
    } else {
      return false;
    }
  }

}
