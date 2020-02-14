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
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Objects;
import java.util.Queue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A reactive streams publisher which can buffer received elements before sending them to it's
 * subscriber. The state for this publisher will always be accessed on the same Vert.x context so
 * does not require synchronization
 *
 * @param <T> The type of the element
 */
public class BufferedPublisher<T> extends BasePublisher<T> {

  private static final Logger log = LoggerFactory.getLogger(BufferedPublisher.class);
  public static final int SEND_MAX_BATCH_SIZE = 10;
  public static final int DEFAULT_BUFFER_MAX_SIZE = 100;

  private final Queue<T> buffer = new ArrayDeque<>();
  private final int bufferMaxSize;
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
    this(ctx, DEFAULT_BUFFER_MAX_SIZE);
  }

  /**
   * Construct a complete BufferedPublisher
   *
   * @param ctx           The Vert.x context to use for the publisher
   * @param initialBuffer A collection of elements to initialise the buffer with
   */
  public BufferedPublisher(final Context ctx, final Collection<T> initialBuffer) {
    this(ctx);
    this.buffer.addAll(initialBuffer);
    complete = true;
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
    super(ctx);
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
    if (!isCancelled()) {
      if (getDemand() == 0) {
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
    if (isCancelled() || completing) {
      return;
    }
    completing = true;
    if (buffer.isEmpty() && getSubscriber() != null) {
      sendComplete();
    } else {
      complete = true;
    }
  }

  protected void maybeSend() {
    int numSent = 0;
    while (!isCancelled() && getDemand() > 0 && !buffer.isEmpty()) {
      if (numSent < SEND_MAX_BATCH_SIZE) {
        final T val = buffer.poll();
        doOnNext(val);
        numSent++;
      } else {
        // Schedule another batch async
        ctx.runOnContext(v -> maybeSend());
        break;
      }
    }

    if (buffer.isEmpty() && !isCancelled()) {
      if (complete) {
        sendComplete();
      } else if (getDemand() > 0 && drainHandler != null) {
        final Runnable handler = drainHandler;
        ctx.runOnContext(v -> handler.run());
        drainHandler = null;
      }
    }
  }

}
