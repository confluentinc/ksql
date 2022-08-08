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
import io.confluent.ksql.execution.pull.PullQueryRow;
import io.confluent.ksql.execution.pull.StreamedRowTranslator;
import io.confluent.ksql.rest.entity.ConsistencyToken;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.util.ConsistencyOffsetVector;
import io.confluent.ksql.util.KeyValue;
import io.confluent.ksql.util.KeyValueMetadata;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlHostInfo;
import io.confluent.ksql.util.RowMetadata;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.impl.FutureFactoryImpl;
import io.vertx.core.spi.FutureFactory;
import io.vertx.core.streams.WriteStream;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.List;
import java.util.OptionalInt;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.GuardedBy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This {@link WriteStream} allows for results to be streamed back to the client when
 * running pull queries. Streaming behavior is important when dealing with large result
 * sets since we don't want to hold the entire set in memory at once.
 *
 * <p> This class has non-blocking writes so that it can work both with callers that implement
 * backpressure and those that don't. If the caller does not implement backpressure, they
 * should block when {@link #writeQueueFull()} and will be notified that they can continue
 * writing by registering a {@link #drainHandler(Handler)}.
 */
public class PullQueryWriteStream implements WriteStream<List<StreamedRow>>, BlockingRowQueue {

  private static final Logger LOG = LoggerFactory.getLogger(PullQueryWriteStream.class);

  private static final int DEFAULT_SOFT_CAPACITY = 50;
  private static final FutureFactory FUTURE_FACTORY = new FutureFactoryImpl();

  private final OptionalInt hardLimit;
  private final StreamedRowTranslator translator;

  // this monitor allows us to implement blocking reads on the poll()
  // methods and also is used to ensure thread safety if multiple threads
  // are using this WriteStream
  private final Monitor m = new Monitor();
  private final Monitor.Guard hasData = m.newGuard(() -> !isEmpty());

  @GuardedBy("m")
  private final Queue<HandledRow> queue = new ArrayDeque<>();
  private final QueryId queryId;
  @GuardedBy("m")
  private int totalRowsQueued = 0;

  @GuardedBy("m")
  private boolean closed = false;
  @GuardedBy("m")
  private int softLimit = DEFAULT_SOFT_CAPACITY;

  // this is a bit of a leaky abstraction that is necessary because
  // Vertx's PipeImpl sets the drain handler very single time the
  // soft limit is hit. because we use the same write stream for
  // multiple read streams, we need to make sure that we maintain
  // all of them but only one copy of them. perhaps a better solution
  // is to just write our own version of PipeImpl that works natively
  // with multiple ReadStreams
  private final ConcurrentHashSet<Handler<Void>> drainHandler = new ConcurrentHashSet<>();

  private CompletionHandler endHandler = () -> { };
  private Handler<AsyncResult<Void>> limitHandler = ar -> { };
  private Handler<Throwable> exceptionHandler = e -> { };
  private Runnable queueCallback = () -> { };

  public PullQueryWriteStream(
      final QueryId queryId,
      final OptionalInt hardLimit,
      final StreamedRowTranslator translator
  ) {
    this.queryId = queryId;
    this.hardLimit = hardLimit;
    this.translator = translator;
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
    m.enter();
    try {
      while (!queue.isEmpty()) {
        collection.add(poll());
      }
    } finally {
      m.leave();
    }
  }

  public void drainRowsTo(final Collection<PullQueryRow> collection) {
    m.enter();
    try {
      while (!queue.isEmpty()) {
        collection.add(pollRow());
      }
    } finally {
      m.leave();
    }
  }

  @Override
  public KeyValueMetadata<List<?>, GenericRow> poll(
      final long timeout,
      final TimeUnit unit
  ) throws InterruptedException {
    if (m.enterWhen(hasData, timeout, unit)) {
      try {
        return poll();
      } finally {
        m.leave();
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

    return new KeyValueMetadata<>(
        KeyValue.keyValue(null, row.getGenericRow()),
        row.getSourceNode()
            .map(sn -> new KsqlHostInfo(sn.getHost(), sn.getPort()))
            .map(RowMetadata::of)
    );
  }

  public PullQueryRow pollRow(
      final long timeout,
      final TimeUnit unit
  ) throws InterruptedException {
    if (m.enterWhen(hasData, timeout, unit)) {
      try {
        return pollRow();
      } finally {
        m.leave();
      }
    }

    return null;
  }

  int numPolled = 0;

  private PullQueryRow pollRow() {
    final HandledRow polled;
    m.enter();
    try {
      polled = queue.poll();
    } finally {
      m.leave();
    }

    if (polled == null) {
      return null;
    }

    numPolled++;

    polled.handler.handle(FUTURE_FACTORY.succeededFuture());

    m.enter();
    try {
      if (queue.size() < softLimit) {
        drainHandler.forEach(h -> h.handle(null));
      }
    } finally {
      m.leave();
    }

    if (numPolled % 10 == 0) {
      LOG.info("Polled {} from {}. {} left in queue", numPolled, queryId, queue.size());
    }
    return polled.row;
  }

  public int getTotalRowsQueued() {
    m.enter();
    try {
      // pass by value, this should be safe not to be changed elsewhere
      return totalRowsQueued;
    } finally {
      m.leave();
    }
  }

  @Override
  public int size() {
    m.enter();
    try {
      return queue.size();
    } finally {
      m.leave();
    }
  }

  @Override
  public boolean isEmpty() {
    return size() == 0;
  }

  public boolean isDone() {
    m.enter();
    try {
      return closed || hardLimitHit();
    } finally {
      m.leave();
    }
  }

  private boolean hardLimitHit() {
    m.enter();
    try {
      return hardLimit.isPresent() && totalRowsQueued >= hardLimit.getAsInt();
    } finally {
      m.leave();
    }
  }

  // -------------------- HANDLER SETTERS -------------------------


  @Override
  public PullQueryWriteStream exceptionHandler(final Handler<Throwable> handler) {
    this.exceptionHandler = handler;
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
  public PullQueryWriteStream write(final List<StreamedRow> data) {
    write(data, ar -> {});
    return this;
  }

  @Override
  public PullQueryWriteStream write(
      final List<StreamedRow> data,
      final Handler<AsyncResult<Void>> handler
  ) {
    m.enter();
    try {
      if (closed || hardLimitHit()) {
        final String msg =
            "Failed to write row. Closed: " + closed + " Limit Hit: " + hardLimitHit();

        LOG.warn(msg);
        exceptionHandler.handle(new KsqlException(msg));
        return this;
      }

      translator.add(data);
      while (translator.hasNext()) {
        queue.offer(new HandledRow(translator.next(), handler));
        totalRowsQueued++;
        queueCallback.run();

        if (hardLimitHit()) {
          // check if the last row enqueued caused us to break the limit, in which case
          // we should signal the end of the WriteStream
          end(limitHandler);
          break;
        }
      }
    } finally {
      m.leave();
    }

    return this;
  }

  @Override
  public void close() {
    end();
  }

  @Override
  public void end() {
    end(ar -> {});
  }

  @Override
  public void end(final Handler<AsyncResult<Void>> handler) {
    m.enter();
    try {
      LOG.info("Closing {} after queueing {} rows", queryId, totalRowsQueued, new Throwable());
      closed = true;
    } finally {
      m.leave();
    }

    endHandler.complete();
    handler.handle(FUTURE_FACTORY.succeededFuture());
  }

  @Override
  public PullQueryWriteStream setWriteQueueMaxSize(final int maxSize) {
    m.enter();
    try {
      softLimit = maxSize;
    } finally {
      m.leave();
    }
    return this;
  }

  @Override
  public boolean writeQueueFull() {
    m.enter();
    try {
      return isDone() || queue.size() >= softLimit;
    } finally {
      m.leave();
    }
  }

}
