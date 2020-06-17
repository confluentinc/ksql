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

public class InsertsPublisher implements Publisher<KsqlObject> {

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
   * Construct an InsertsPublisher
   */
  public InsertsPublisher() {
    this(DEFAULT_BUFFER_MAX_SIZE);
  }

  /**
   * Construct an InsertsPublisher
   *
   * @param bufferMaxSize Indicative max number of elements to store in the buffer. Note that this
   *                      is not enforced, but it used to determine what to return from the accept
   *                      method so the caller can stop sending more and set a drainHandler to be
   *                      notified when the buffer is cleared
   */
  public InsertsPublisher(final int bufferMaxSize) {
    this.bufferMaxSize = bufferMaxSize;
  }

  /**
   * Provide a new row for insertion. The publisher will attempt to deliver it to server endpoint,
   * assuming the streamInserts() request has been made. The publisher will buffer it internally
   * if it can't deliver it immediately.
   *
   * @param row The element
   * @return true if the internal buffer is 'full'. I.e. if number of elements is >= bufferMaxSize.
   */
  public synchronized boolean accept(final KsqlObject row) {
    if (complete || sentComplete) {
      throw new IllegalStateException("Cannot call accept after complete is called");
    }
    if (!cancelled && !isFailed()) {
      if (demand == 0) {
        buffer.add(row);
      } else {
        doOnNext(row);
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
  public synchronized void drainHandler(final Runnable handler) {
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
  public synchronized void complete() {
    if (complete || isFailed()) {
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
      throw new IllegalStateException("Cannot subscribe a new subscriber as one is already present.");
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

    if (buffer.isEmpty() && !isFailed()) {
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

  // TODO: is this necessary? probably not
  private boolean isFailed() {
    return false;
  }
}
