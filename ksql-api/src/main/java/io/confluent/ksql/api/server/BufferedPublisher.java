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

package io.confluent.ksql.api.server;

import io.vertx.core.Context;
import io.vertx.core.Vertx;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Queue;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A reactive streams publisher which can buffer received elements before sending them to it's
 * subscriber. The state for this publisher will always be accessed on the same Vert.x context so
 * does not require synchronization
 *
 * @param <T> The type of the element
 */
public class BufferedPublisher<T> implements Publisher<T> {

  private static final Logger log = LoggerFactory.getLogger(BufferedPublisher.class);
  public static final int SEND_MAX_BATCH_SIZE = 10;
  public static final int DEFAULT_BUFFER_MAX_SIZE = 100;

  private final Context ctx;
  private final Queue<T> buffer = new ArrayDeque<>();
  private final int bufferMaxSize;
  private Subscriber<? super T> subscriber;
  private long demand;
  private boolean cancelled;
  private Runnable drainHandler;
  private boolean complete;
  private boolean completing;

  /**
   * Construct a BufferedPublisher
   *
   * @param ctx The Vert.x context to use for the publisher - the publisher code must always be
   *            executed on this context. This ensures the code is never executed concurrently by
   *            more than one thread.
   */
  public BufferedPublisher(final Context ctx) {
    this(ctx, Collections.emptySet(), DEFAULT_BUFFER_MAX_SIZE);
  }

  /**
   * Construct a BufferedPublisher
   *
   * @param ctx           The Vert.x context to use for the publisher
   * @param initialBuffer A collection of elements to initialise the buffer with
   */
  public BufferedPublisher(final Context ctx, final Collection<T> initialBuffer) {
    this(ctx, initialBuffer, DEFAULT_BUFFER_MAX_SIZE);
  }

  /**
   * Construct a BufferedPublisher
   *
   * @param ctx           The Vert.x context to use for the publisher
   * @param bufferMaxSize Indicative max number of elements to store in the buffer. Note that this
   *                      is not enforced, but it used to determine what to return from the accept
   *                      method so the caller can stop sending more and set a drainHandler to be
   *                      notified when the buffer is cleared
   */
  public BufferedPublisher(final Context ctx, final int bufferMaxSize) {
    this(ctx, Collections.emptySet(), bufferMaxSize);
  }

  /**
   * Construct a BufferedPublisher
   *
   * @param ctx           The Vert.x context to use for the publisher
   * @param initialBuffer A collection of elements to initialise the buffer with
   * @param bufferMaxSize Indicative max number of elements to buffer
   */
  public BufferedPublisher(final Context ctx, final Collection<T> initialBuffer,
      final int bufferMaxSize) {
    this.ctx = ctx;
    this.buffer.addAll(initialBuffer);
    this.bufferMaxSize = bufferMaxSize;
  }

  /**
   * Provide an element to the publisher. The publisher will attempt to deliver it to it's
   * subscriber (if any). The publisher will buffer it internally if it can't deliver it
   * immediately.
   *
   * @param t The element
   * @return true if the internal buffer is 'full'. I.e. if number of elements is >= bufferMaxSize.
   */
  public boolean accept(final T t) {
    checkContext();
    if (completing) {
      throw new IllegalStateException("Cannot call accept after complete is called");
    }
    if (!cancelled) {
      if (demand == 0) {
        buffer.add(t);
      } else {
        doOnNext(t);
      }
    }
    return buffer.size() >= bufferMaxSize;
  }

  /**
   * If you set a drain handler. It will be called if, after delivery is attempted there are zero
   * elements buffered internally and there is demand from the subscriber for more elements. Drain
   * handlers are one shot handlers, after being called it will never be called more than once.
   *
   * @param handler The handler
   */
  public void drainHandler(final Runnable handler) {
    checkContext();
    if (drainHandler != null) {
      throw new IllegalStateException("drainHandler already set");
    }
    this.drainHandler = Objects.requireNonNull(handler);
  }

  /**
   * Mark the incoming stream of elements as complete. This means onComplete will be called on any
   * subscriber after any buffered messages have been delivered. Once complete has been called no
   * further elements will be accepted
   */
  public void complete() {
    checkContext();
    if (cancelled || completing) {
      return;
    }
    completing = true;
    if (buffer.isEmpty() && subscriber != null) {
      sendComplete();
    } else {
      complete = true;
    }
  }

  /**
   * Subscribe a subscriber to this publisher. The publisher will allow at most one subscriber.
   *
   * @param subscriber The subscriber
   */
  @Override
  public void subscribe(final Subscriber<? super T> subscriber) {
    Objects.requireNonNull(subscriber);
    if (Vertx.currentContext() == ctx) {
      doSubscribe(subscriber);
    } else {
      ctx.runOnContext(v -> doSubscribe(subscriber));
    }
  }

  /**
   * Hook to allow subclasses to inject errors etc.
   * This will be called before onNext is called on the subscriber to deliver an element
   *
   * @return true if processing should continue
   */
  protected boolean beforeOnNext() {
    return true;
  }

  protected final void sendError(final Exception e) {
    checkContext();
    try {
      subscriber.onError(e);
      cancelled = true;
    } catch (Throwable t) {
      logError("Exceptions must not be thrown from onError", t);
    }
  }

  private void sendComplete() {
    try {
      cancelled = true;
      subscriber.onComplete();
    } catch (Throwable t) {
      logError("Exceptions must not be thrown from onComplete", t);
    }
  }

  private void doSubscribe(final Subscriber<? super T> subscriber) {
    this.subscriber = subscriber;
    try {
      subscriber.onSubscribe(new Sub());
    } catch (final Throwable t) {
      sendError(new IllegalStateException("Exceptions must not be thrown from onSubscribe", t));
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
        break;
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
    if (!beforeOnNext()) {
      return;
    }
    try {
      subscriber.onNext(val);
    } catch (final Throwable t) {
      logError("Exceptions must not be thrown from onNext", t);
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
      sendError(new IllegalArgumentException("Amount requested must be > 0"));
    } else if (demand + n < 1) {
      // Catch overflow and set to "infinite"
      demand = Long.MAX_VALUE;
      doSend();
    } else {
      demand += n;
      doSend();
    }
  }

  private void doCancel() {
    cancelled = true;
    subscriber = null;
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
