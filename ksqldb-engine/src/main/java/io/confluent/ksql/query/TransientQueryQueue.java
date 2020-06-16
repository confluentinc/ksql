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

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.GenericRow;
import java.util.Collection;
import java.util.OptionalInt;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A queue of rows for transient queries.
 */
public class TransientQueryQueue implements BlockingRowQueue {

  public static final int BLOCKING_QUEUE_CAPACITY = 500;

  private final BlockingQueue<GenericRow> rowQueue;
  private final int offerTimeoutMs;
  private LimitQueueCallback callback;
  private volatile boolean closed = false;

  public TransientQueryQueue(final OptionalInt limit) {
    this(limit, BLOCKING_QUEUE_CAPACITY, 100);
  }

  @VisibleForTesting
  TransientQueryQueue(
      final OptionalInt limit,
      final int queueSizeLimit,
      final int offerTimeoutMs
  ) {
    this.callback = limit.isPresent()
        ? new LimitedQueueCallback(limit.getAsInt())
        : new UnlimitedQueueCallback();
    this.rowQueue = new LinkedBlockingQueue<>(queueSizeLimit);
    this.offerTimeoutMs = offerTimeoutMs;
  }

  @Override
  public void setLimitHandler(final LimitHandler limitHandler) {
    callback.setLimitHandler(limitHandler);
  }

  @Override
  public void setQueuedCallback(final Runnable queuedCallback) {
    final LimitQueueCallback parent = callback;

    callback = new LimitQueueCallback() {
      @Override
      public boolean shouldQueue() {
        return parent.shouldQueue();
      }

      @Override
      public void onQueued() {
        parent.onQueued();
        queuedCallback.run();
      }

      @Override
      public void setLimitHandler(final LimitHandler limitHandler) {
        parent.setLimitHandler(limitHandler);
      }
    };
  }

  @Override
  public GenericRow poll(final long timeout, final TimeUnit unit)
      throws InterruptedException {
    return rowQueue.poll(timeout, unit);
  }

  @Override
  public GenericRow poll() {
    return rowQueue.poll();
  }

  @Override
  public void drainTo(final Collection<? super GenericRow> collection) {
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
  }

  public void acceptRow(final GenericRow row) {
    try {
      if (row == null) {
        return;
      }

      if (!callback.shouldQueue()) {
        return;
      }

      while (!closed) {
        if (rowQueue.offer(row, offerTimeoutMs, TimeUnit.MILLISECONDS)) {
          callback.onQueued();
          break;
        }
      }
    } catch (final InterruptedException e) {
      // Forced shutdown?
      Thread.currentThread().interrupt();
    }
  }
}
