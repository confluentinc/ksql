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

package io.confluent.ksql.api.server;

import io.vertx.core.Context;
import io.vertx.core.Vertx;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Queue;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferedPublisher<T> implements Publisher<T> {

  private static final Logger log = LoggerFactory.getLogger(BufferedPublisher.class);
  private static final int SEND_MAX_BATCH_SIZE = 100;

  private final Context ctx;
  private final Queue<T> buffer = new ArrayDeque<>();
  private Subscriber<? super T> subscriber;
  private long demand;
  private boolean cancelled;
  private Runnable drainHandler;
  private boolean complete;

  public BufferedPublisher(final Context ctx) {
    this.ctx = ctx;
  }

  public void accept(final T t) {
    checkContext();
    if (demand == 0 || cancelled) {
      buffer.add(t);
    } else {
      doOnNext(t);
    }
  }

  public int numBuffered() {
    checkContext();
    return buffer.size();
  }

  public void drainHandler(final Runnable handler) {
    checkContext();
    this.drainHandler = handler;
  }

  public void complete() {
    checkContext();
    if (subscriber == null || cancelled || complete) {
      return;
    }
    if (buffer.isEmpty()) {
      sendComplete();
    } else {
      complete = true;
    }
  }

  private void sendComplete() {
    try {
      cancelled = true;
      subscriber.onComplete();
    } catch (Throwable t) {
      logError("Subscriber violated the Reactive Streams rule 2.13 by throwing an exception"
          + " from onComplete.", t);
    }
  }

  @Override
  public void subscribe(final Subscriber<? super T> subscriber) {
    this.subscriber = Objects.requireNonNull(subscriber);
    try {
      subscriber.onSubscribe(new Sub());
    } catch (final Throwable t) {
      sendError(new IllegalStateException(subscriber
          + " violated the Reactive Streams rule 2.13 by throwing an exception from onSubscribe.",
          t));
    }
  }

  private void sendError(final Exception e) {
    try {
      subscriber.onError(e);
      cancelled = true;
    } catch (Throwable t) {
      logError("Subscriber violated the Reactive Streams rule 2.13 by throwing an exception from "
          + "onError.", t);
    }
  }

  private void checkContext() {
    if (Vertx.currentContext() != ctx) {
      throw new IllegalStateException("On wrong context");
    }
  }

  private void doSend() {
    int numSent = 0;
    while (!cancelled && demand > 0 && !buffer.isEmpty()) {
      if (numSent < SEND_MAX_BATCH_SIZE) {
        final T val = buffer.poll();
        doOnNext(val);
        numSent++;
      } else {
        // Schedule another batch async
        ctx.runOnContext(v -> doSend());
      }
    }

    if (buffer.isEmpty() && !cancelled) {
      if (complete) {
        sendComplete();
      } else if (demand > 0 && drainHandler != null) {
        final Runnable handler = drainHandler;
        ctx.runOnContext(v -> handler.run());
        drainHandler = null;
      }
    }
  }

  private void doOnNext(final T val) {
    try {
      subscriber.onNext(val);
    } catch (final Throwable t) {
      logError("Subscriber violated the Reactive Streams rule 2.13 by throwing an exception"
          + " from onNext or onComplete.", t);
    }
    // If demand == Long.MAX_VALUE this means "infinite demand"
    if (demand != Long.MAX_VALUE) {
      demand--;
    }
  }

  private void logError(final String message, final Throwable t) {
    log.error(message, t);
    cancelled = true;
  }

  private void doRequest(final long n) {
    if (n <= 0) {
      sendError(new IllegalArgumentException(
          "Subscriber violated the Reactive Streams rule 3.9 by requesting a non-positive "
              + "number of elements."));
    } else if (demand + n < 1) {
      demand = Long.MAX_VALUE;
      doSend();
    } else {
      demand += n;
      doSend();
    }
  }

  private void doCancel() {
    cancelled = true;
  }

  private class Sub implements Subscription {

    @Override
    public void request(final long n) {
      ctx.runOnContext(v -> doRequest(n));
    }

    @Override
    public void cancel() {
      ctx.runOnContext(v -> doCancel());
    }
  }
}
