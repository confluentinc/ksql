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

package io.confluent.ksql.query;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.pull.PullQueryRow;
import io.confluent.ksql.util.ConsistencyOffsetVector;
import io.confluent.ksql.util.KeyValue;
import io.confluent.ksql.util.KeyValueMetadata;
import io.confluent.ksql.util.KsqlHostInfo;
import io.confluent.ksql.util.RowMetadata;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This queue allows for results to be streamed back to the client when running pull queries.
 * Streaming behavior is important when dealing with large results since we don't want to hold it
 * all in memory at once.
 *
 * <p>New rows are produced and enqueued by PullPhysicalPlan if the request is being handled locally
 * or HARouting if the request must be forwarded to another node. This is done with the method
 * acceptRow and may block the caller if the queue is at capacity.
 *
 * <p>Rows are consumed by the request thread of the endpoint. This is done with the various poll
 * methods.
 */
public class PullQueryQueue implements BlockingRowQueue {
  private static final Logger LOG = LoggerFactory.getLogger(PullQueryQueue.class);

  // The capacity to allow before blocking when enqueuing
  private static final int BLOCKING_QUEUE_CAPACITY = 50;
  // The time to wait while enqueuing a row before quitting to retry
  private static final long DEFAULT_OFFER_TIMEOUT_MS = 100;

  private final BlockingQueue<PullQueryRow> rowQueue;
  private final long offerTimeoutMs;
  private AtomicBoolean closed = new AtomicBoolean(false);
  private AtomicLong totalRowsQueued = new AtomicLong(0);
  private final OptionalInt limit;

  /**
   * The callback run when we've hit the limit. Specifically, this happens when
   * {@link #closeInternal(boolean)}} is called with true.
   */
  private LimitHandler limitHandler;
  /**
   * Callback is checked before enqueueing new rows and called when new rows are actually added.
   */
  private Runnable queuedCallback;
  /**
   * The callback run when we've hit the end of the data. Specifically, this happens when
   * {@link #close()} is called.
   */
  private CompletionHandler completionHandler;

  public PullQueryQueue(final OptionalInt limit) {
    this(BLOCKING_QUEUE_CAPACITY, DEFAULT_OFFER_TIMEOUT_MS, limit);
  }

  public PullQueryQueue(
      final int queueSizeLimit,
      final long offerTimeoutMs,
      final OptionalInt limit) {
    this.queuedCallback = () -> { };
    this.limitHandler = () -> { };
    this.completionHandler = () -> { };
    this.rowQueue = new ArrayBlockingQueue<>(queueSizeLimit);
    this.offerTimeoutMs = offerTimeoutMs;
    this.limit = limit;
  }

  @Override
  public void setLimitHandler(final LimitHandler limitHandler) {
    this.limitHandler = limitHandler;
  }

  @Override
  public void setCompletionHandler(final CompletionHandler completionHandler) {
    this.completionHandler = completionHandler;
  }

  @Override
  public void setQueuedCallback(final Runnable queuedCallback) {
    final Runnable parent = this.queuedCallback;

    this.queuedCallback = () -> {
      parent.run();
      queuedCallback.run();
    };
  }

  @Override
  public KeyValueMetadata<List<?>, GenericRow> poll(final long timeout, final TimeUnit unit)
      throws InterruptedException {
    return pullQueryRowToKeyValueMetadata(rowQueue.poll(timeout, unit));
  }

  @Override
  public KeyValueMetadata<List<?>, GenericRow> poll() {
    return  pullQueryRowToKeyValueMetadata(rowQueue.poll());
  }

  @Override
  public void drainTo(final Collection<? super KeyValueMetadata<List<?>, GenericRow>> collection) {
    final List<PullQueryRow> list = new ArrayList<>();
    drainRowsTo(list);
    list.stream()
        .map(PullQueryQueue::pullQueryRowToKeyValueMetadata)
        .forEach(collection::add);
  }

  /**
   * Similar to {@link #poll(long, TimeUnit)} , but returns a {@link PullQueryRow}.
   */
  public PullQueryRow pollRow(final long timeout, final TimeUnit unit) throws InterruptedException {
    return rowQueue.poll(timeout, unit);
  }

  /**
   * Similar to {@link #drainTo(Collection)}, but takes {@link PullQueryRow}s.
   */
  public void drainRowsTo(final Collection<PullQueryRow> collection) {
    rowQueue.drainTo(collection);
  }

  @Override
  public int size() {
    return rowQueue.size();
  }

  @Override
  public boolean isEmpty() {
    return rowQueue.isEmpty();
  }

  /**
   * Unlike push queries that run forever until someone deliberately kills it, pull queries have an
   * ending.  When they've reached their end, this is expected to be called.  Also, if the system
   * wants to end pull queries prematurely, such as when the client connection closes, this should
   * also be called then.
   */
  private void closeInternal(final boolean limitHit) {
    if (!closed.getAndSet(true)) {
      // Unlike limits based on a number of rows which can be checked and possibly triggered after
      // every queuing of a row, pull queries just declare they've reached their limit when close is
      // called.
      if (limitHit) {
        limitHandler.limitReached();
      } else {
        completionHandler.complete();
      }
    }
  }

  @Override
  public void close() {
    closeInternal(false);
  }

  public boolean isClosed() {
    return closed.get();
  }

  /**
   * Similar to {@link #acceptRow(PullQueryRow)} but takes many rows.
   * @param tableRows The rows to enqueue.
   */
  public boolean acceptRows(final List<PullQueryRow> tableRows) {
    if (tableRows == null) {
      return false;
    }
    for (PullQueryRow row : tableRows) {
      if (!acceptRow(row)) {
        return false;
      }
    }
    return true;
  }

  private static KeyValueMetadata<List<?>, GenericRow> pullQueryRowToKeyValueMetadata(
      final PullQueryRow row) {
    if (row == null) {
      return null;
    }

    if (row.getConsistencyOffsetVector().isPresent()) {
      return new KeyValueMetadata<>(RowMetadata.of(row.getConsistencyOffsetVector().get()));
    }
    final Optional<RowMetadata> metadata = row.getSourceNode()
        .map(node -> new KsqlHostInfo(node.location().getHost(), node.location().getPort()))
        .map(RowMetadata::of);
    return new KeyValueMetadata<>(KeyValue.keyValue(null, row.getGenericRow()),
        metadata);
  }

  /**
   * Enqueues a row on the queue.  Blocks until the row can be accepted.
   * @param row The row to enqueue.
   */
  public synchronized boolean acceptRow(final PullQueryRow row) {
    try {
      if (row == null) {
        return false;
      }

      final boolean accepted;
      if (limit.isPresent()) {
        synchronized (this) {
          if (totalRowsQueued.get() >= limit.getAsInt()) {
            closeInternal(true);
            return false;
          }
          accepted = doAcceptRow(row);
        }
      } else {
        accepted = doAcceptRow(row);
      }

      if (accepted) {
        queuedCallback.run();
      }
      return accepted;
    } catch (final InterruptedException e) {
      // Forced shutdown?
      LOG.error("Interrupted while trying to offer row to queue", e);
      Thread.currentThread().interrupt();
    }
    return false;
  }

  private boolean doAcceptRow(final PullQueryRow row) throws InterruptedException {
    while (!closed.get()) {
      if (rowQueue.offer(row, offerTimeoutMs, TimeUnit.MILLISECONDS)) {
        totalRowsQueued.incrementAndGet();
        return true;
      }
    }
    return false;
  }

  /**
   * If you don't want to rely on poll timeouts, a sentinel can be directly used, rather than
   * interrupting the sleeping thread. The main difference between this and acceptRow is that
   * this allows the addition of the sentinel even if the queue is closed.
   * @param row The row to use as the sentinel
   */
  public void putSentinelRow(final PullQueryRow row) {
    try {
      rowQueue.put(row);
    } catch (InterruptedException e) {
      LOG.error("Interrupted while trying to put row into queue", e);
      Thread.currentThread().interrupt();
    }
  }

  public void putConsistencyVector(final ConsistencyOffsetVector consistencyOffsetVector) {
    try {
      LOG.info("Push consistency token to queue " + consistencyOffsetVector);
      rowQueue.put(new PullQueryRow(null, null, null, Optional.of(consistencyOffsetVector)));
    } catch (InterruptedException e) {
      LOG.error("Interrupted while trying to put consistency token into queue", e);
      Thread.currentThread().interrupt();
    }
  }

  public long getTotalRowsQueued() {
    return totalRowsQueued.get();
  }
}
