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

import io.confluent.ksql.api.impl.Utils;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import java.util.Objects;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for our reactive streams publishers
 *
 * @param <T> the type of the element
 */
public abstract class BasePublisher<T> implements Publisher<T> {

  private static final Logger log = LoggerFactory.getLogger(BasePublisher.class);

  protected final Context ctx;
  private Subscriber<? super T> subscriber;
  private long demand;
  private boolean cancelled;

  public BasePublisher(final Context ctx) {
    this.ctx = Objects.requireNonNull(ctx);
  }

  /**
   * Subscribe a subscriber to this publisher. The publisher will allow at most one subscriber.
   *
   * @param subscriber The subscriber
   */
  @Override
  public void subscribe(final Subscriber<? super T> subscriber) {
    Objects.requireNonNull(subscriber);
    if (Vertx.currentContext() == ctx && !Utils.isWorkerThread()) {
      doSubscribe(subscriber);
    } else {
      ctx.runOnContext(v -> doSubscribe(subscriber));
    }
  }

  public void close() {
    ctx.runOnContext(v -> doClose());
  }

  protected void checkContext() {
    Utils.checkContext(ctx);
  }

  protected final void sendError(final Exception e) {
    checkContext();
    try {
      if (subscriber != null) {
        subscriber.onError(e);
      } else {
        log.error("Failure in publisher", e);
      }
      cancelled = true;
    } catch (Throwable t) {
      logError("Exceptions must not be thrown from onError", t);
    }
  }

  protected void sendComplete() {
    try {
      cancelled = true;
      subscriber.onComplete();
    } catch (Throwable t) {
      logError("Exceptions must not be thrown from onComplete", t);
    }
  }

  protected void doOnNext(final T val) {
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

  protected long getDemand() {
    return demand;
  }

  protected Subscriber<? super T> getSubscriber() {
    return subscriber;
  }

  protected boolean isCancelled() {
    return cancelled;
  }

  /**
   * Attempt delivery
   */
  protected abstract void maybeSend();

  /**
   * Hook to allow subclasses to inject errors etc. This will be called before onNext is called on
   * the subscriber to deliver an element
   *
   * @return true if processing should continue
   */
  protected boolean beforeOnNext() {
    return true;
  }

  /**
   * Hook. Called after subscribe
   */
  protected void afterSubscribe() {
  }

  private void doSubscribe(final Subscriber<? super T> subscriber) {
    this.subscriber = subscriber;
    try {
      subscriber.onSubscribe(new Sub());
    } catch (final Throwable t) {
      sendError(new IllegalStateException("Exceptions must not be thrown from onSubscribe", t));
    }
    afterSubscribe();
  }

  private void doClose() {
    if (subscriber != null) {
      sendComplete();
    }
  }

  private void doRequest(final long n) {
    if (n <= 0) {
      sendError(new IllegalArgumentException("Amount requested must be > 0"));
    } else if (demand + n < 1) {
      // Catch overflow and set to "infinite"
      demand = Long.MAX_VALUE;
      maybeSend();
    } else {
      demand += n;
      maybeSend();
    }
  }

  private void doCancel() {
    cancelled = true;
    subscriber = null;
  }

  private void logError(final String message, final Throwable t) {
    log.error(message, t);
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
