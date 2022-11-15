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

package io.confluent.ksql.api.client;

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Queue;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * A {@code org.reactivestreams.Publisher} suitable for use with the
 * {@link Client#streamInserts(String, Publisher)} method. Rows for insertion are passed to the
 * publisher via the {@link #accept(KsqlObject)} method, and buffered for delivery once the
 * {@link Client#streamInserts} request is made and the server-side subscriber has been subscribed.
 */
public class InsertsPublisher implements Publisher<KsqlObject> {

  /**
   * The buffer max size indicator value used by the default constructor. See
   * {@link #InsertsPublisher(int)} for how this value is used.
   */
  public static final int DEFAULT_BUFFER_MAX_SIZE = 200;

  private Subscriber<? super KsqlObject> subscriber;
  private final Queue<KsqlObject> buffer = new ArrayDeque<>();
  private final int bufferMaxSize;
  private long demand;
  private Runnable drainHandler;
  private volatile boolean cancelled;
  private boolean complete;
  private boolean shouldSendComplete;
  private boolean sentComplete;

  /**
   * Creates an {@code InsertsPublisher}.
   */
  public InsertsPublisher() {
    this(DEFAULT_BUFFER_MAX_SIZE);
  }

  /**
   * Creates an {@code InsertsPublisher}.
   *
   * @param bufferMaxSize Indicative max number of elements to store in the buffer. Note that this
   *                      value is not enforced, but it used to determine what to return from the
   *                      {@link #accept(KsqlObject)} method so the caller can stop sending more
   *                      rows and set a drainHandler to be notified when the buffer is cleared
   */
  public InsertsPublisher(final int bufferMaxSize) {
    this.bufferMaxSize = bufferMaxSize;
  }

  /**
   * Provides a new row for insertion. The publisher will attempt to deliver it to server endpoint,
   * once the {@link Client#streamInserts} request has been made. The publisher will buffer the row
   * internally if it can't deliver it immediately. Note that the row will be buffered even if the
   * buffer is 'full', i.e., if number of elements is at least {@code bufferMaxSize}, as the
   * {@code bufferMaxSize} value is not a hard limit. See {@link #InsertsPublisher(int)} for more.
   *
   * @param row the row to insert
   * @return whether the internal buffer is 'full', i.e., if number of elements is at least
   *         {@code bufferMaxSize}.
   */
  public synchronized boolean accept(final KsqlObject row) {
    if (complete || sentComplete) {
      throw new IllegalStateException("Cannot call accept after complete is called");
    }
    if (!cancelled) {
      if (demand == 0) {
        buffer.add(row);
      } else {
        doOnNext(row);
      }
    }
    return buffer.size() >= bufferMaxSize;
  }

  /**
   * Sets a drain handler on the publisher. The drain handler will be called if after a row is
   * delivered there are zero elements buffered internally and there is demand from the subscriber
   * for more elements. Drain handlers may be used in combination with the return value from
   * {@link #accept(KsqlObject)} to ensure the publisher's buffer does not grow too large.
   *
   * <p>Drain handlers are one shot handlers; after a drain handler is called it
   * will never be called again. Instead, the caller should set a new drain handler for subsequent
   * use.
   *
   * @param handler the drain handler
   */
  public synchronized void drainHandler(final Runnable handler) {
    if (drainHandler != null) {
      throw new IllegalStateException("drainHandler already set");
    }
    this.drainHandler = Objects.requireNonNull(handler);
  }

  /**
   * Marks the incoming stream of elements as complete. This means no further rows will be accepted
   * by the publisher and the {@link Client#streamInserts} connection will be closed once any
   * buffered rows have been delivered for insertion.
   */
  public synchronized void complete() {
    if (complete) {
      return;
    }
    complete = true;
    if (buffer.isEmpty() && subscriber != null) {
      sendComplete();
    } else {
      shouldSendComplete = true;
    }
  }

  @Override
  public synchronized void subscribe(final Subscriber<? super KsqlObject> subscriber) {
    if (this.subscriber != null) {
      throw new IllegalStateException(
          "Cannot subscribe a new subscriber: A subscriber is already present.");
    }

    this.subscriber = subscriber;
    subscriber.onSubscribe(new Subscription() {
      @Override
      public void request(final long l) {
        doRequest(l);
      }

      @Override
      public void cancel() {
        doCancel();
      }
    });
  }

  private synchronized void doRequest(final long n) {
    if (n <= 0) {
      subscriber.onError((new IllegalArgumentException("Amount requested must be > 0")));
    } else if (demand + n < 1) {
      // Catch overflow and set to "infinite"
      demand = Long.MAX_VALUE;
      maybeSend();
    } else {
      demand += n;
      maybeSend();
    }
  }

  private synchronized void doCancel() {
    cancelled = true;
    subscriber = null;
  }

  private void maybeSend() {
    while (demand > 0 && !buffer.isEmpty()) {
      final KsqlObject val = buffer.poll();
      doOnNext(val);
    }

    if (buffer.isEmpty()) {
      if (shouldSendComplete) {
        sendComplete();
        shouldSendComplete = false;
      } else if (demand > 0 && drainHandler != null) {
        drainHandler.run();
        drainHandler = null;
      }
    }
  }

  private void doOnNext(final KsqlObject row) {
    subscriber.onNext(row);

    // If demand == Long.MAX_VALUE this means "infinite demand"
    if (demand != Long.MAX_VALUE) {
      demand--;
    }
  }

  private void sendComplete() {
    sentComplete = true;
    subscriber.onComplete();
  }
}
