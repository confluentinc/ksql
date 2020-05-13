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

package io.confluent.ksql.api.client.impl;

import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.reactive.BaseSubscriber;
import io.vertx.core.Context;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.reactivestreams.Subscription;

public class PollableSubscriber extends BaseSubscriber<Row> {

  private static final int REQUEST_BATCH_SIZE = 100;
  // 100ms in ns
  private static final long MAX_POLL_NANOS = TimeUnit.MILLISECONDS.toNanos(100);

  private final BlockingQueue<Row> queue = new LinkedBlockingQueue<>();
  private final Consumer<Exception> errorHandler;
  private int tokens;
  private volatile boolean complete;
  private volatile boolean closed;

  public PollableSubscriber(final Context context, final Consumer<Exception> errorHandler) {
    super(context);

    this.errorHandler = Objects.requireNonNull(errorHandler);
  }

  @Override
  protected void afterSubscribe(final Subscription subscription) {
    checkRequestTokens();
  }

  @Override
  protected void handleValue(final Row row) {
    queue.add(row);
  }

  @Override
  protected void handleError(final Throwable t) {
    errorHandler.accept(new Exception(t));
  }

  @Override
  protected void handleComplete() {
    complete = true;
  }

  public synchronized Row poll(final long timeout, final TimeUnit timeUnit) {
    if (closed) {
      return null;
    }
    if (complete && queue.isEmpty()) {
      close();
      return null;
    }
    final long timeoutNs = timeUnit.toNanos(timeout);
    final long end;
    long remainingTime;
    if (timeoutNs > 0) {
      end = System.nanoTime() + timeoutNs;
      remainingTime = timeoutNs;
    } else {
      end = Long.MAX_VALUE;
      remainingTime = Long.MAX_VALUE;
    }
    do {
      // Poll in smaller units so we can exit on close
      final long pollTime = Math.min(remainingTime, MAX_POLL_NANOS);
      try {
        final Row row = queue.poll(pollTime, TimeUnit.NANOSECONDS);
        if (row != null) {
          tokens--;
          checkRequestTokens();
          return row;
        }
      } catch (InterruptedException e) {
        return null;
      }
      remainingTime = end - System.nanoTime();
    } while (!closed && remainingTime > 0);
    return null;
  }

  public void close() {
    closed = true;
  }

  synchronized boolean isClosed() {
    return closed;
  }

  private void checkRequestTokens() {
    if (tokens == 0) {
      tokens += REQUEST_BATCH_SIZE;
      makeRequest(REQUEST_BATCH_SIZE);
    }
  }
}