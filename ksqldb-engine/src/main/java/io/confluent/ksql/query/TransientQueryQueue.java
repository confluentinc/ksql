/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.query;

import static io.confluent.ksql.util.KeyValue.keyValue;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.KeyValue;
import io.confluent.ksql.util.KeyValueMetadata;
import java.util.Collection;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A queue of rows for transient queries.
 */
public class TransientQueryQueue implements BlockingRowQueue {

  public static final int BLOCKING_QUEUE_CAPACITY = 500;

  private final BlockingQueue<KeyValueMetadata<List<?>, GenericRow>> rowQueue;
  private final int offerTimeoutMs;
  private volatile boolean closed = false;
  private final AtomicInteger remaining;
  private LimitHandler limitHandler;
  private CompletionHandler completionHandler;
  private Runnable queuedCallback;
  private AtomicLong totalRowsQueued = new AtomicLong(0);
  private AtomicBoolean invokedHandler = new AtomicBoolean(false);

  public TransientQueryQueue(final OptionalInt limit) {
    this(limit, BLOCKING_QUEUE_CAPACITY, 100);
  }

  @VisibleForTesting
  public TransientQueryQueue(
      final OptionalInt limit,
      final int queueSizeLimit,
      final int offerTimeoutMs
  ) {
    this.remaining = limit.isPresent() ? new AtomicInteger(limit.getAsInt()) : null;
    this.rowQueue = new LinkedBlockingQueue<>(queueSizeLimit);
    this.offerTimeoutMs = offerTimeoutMs;
  }

  @Override
  public void setQueuedCallback(final Runnable queuedCallback) {
    this.queuedCallback = queuedCallback;
  }

  @Override
  public void setLimitHandler(final LimitHandler limitHandler) {
    this.limitHandler = limitHandler;
    if (passedLimit()) {
      invokedHandler.set(true);
      limitHandler.limitReached();
    }
  }

  @Override
  public void setCompletionHandler(final CompletionHandler completionHandler) {
    this.completionHandler = completionHandler;
  }

  @Override
  public KeyValueMetadata<List<?>, GenericRow> poll(final long timeout, final TimeUnit unit)
      throws InterruptedException {
    return rowQueue.poll(timeout, unit);
  }

  @Override
  public KeyValueMetadata<List<?>, GenericRow> poll() {
    return rowQueue.poll();
  }

  @Override
  public void drainTo(final Collection<? super KeyValueMetadata<List<?>, GenericRow>> collection) {
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

  @Override
  public void close() {
    closed = true;
    // This ensures that we call at least some callback, which is required
    // to close connections.
    if (invokedHandler.compareAndSet(false, true)) {
      complete();
    }
  }

  public void acceptRow(final List<?> key, final GenericRow value) {
    try {
      if (passedLimit()) {
        return;
      }

      final KeyValue<List<?>, GenericRow> row = keyValue(key, value);
      final KeyValueMetadata<List<?>, GenericRow> keyValueMetadata = new KeyValueMetadata<>(row);

      while (!closed) {
        if (rowQueue.offer(keyValueMetadata, offerTimeoutMs, TimeUnit.MILLISECONDS)) {
          onQueued();
          totalRowsQueued.incrementAndGet();
          break;
        }
      }
    } catch (final InterruptedException e) {
      // Forced shutdown?
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Accepts the rows without blocking.
   * @param keyValueMetadata The key value metadata to accept
   * @return If the row was accepted or discarded for an acceptable reason, false if it was rejected
   *     because the queue was full.
   */
  public boolean acceptRowNonBlocking(
      final KeyValueMetadata<List<?>, GenericRow> keyValueMetadata) {
    try {
      if (passedLimit()) {
        return true;
      }

      if (!closed) {
        if (!rowQueue.offer(keyValueMetadata, 0, TimeUnit.MILLISECONDS)) {
          return false;
        }
        onQueued();
        return true;
      }
    } catch (final InterruptedException e) {
      // Forced shutdown?
      Thread.currentThread().interrupt();
      return false;
    }
    return true;
  }

  public boolean isClosed() {
    return closed;
  }

  public void complete() {
    if (completionHandler != null) {
      completionHandler.complete();
    }
  }

  private void onQueued() {
    if (remaining != null && remaining.decrementAndGet() <= 0) {
      invokedHandler.set(true);
      limitHandler.limitReached();
    }
    if (queuedCallback != null) {
      queuedCallback.run();
    }
  }

  private boolean passedLimit() {
    return remaining != null && remaining.get() <= 0;
  }

  public long getTotalRowsQueued() {
    return totalRowsQueued.get();
  }
}
