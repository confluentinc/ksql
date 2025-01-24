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

package io.confluent.ksql.reactive;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.util.VertxUtils;
import io.vertx.core.Context;
import io.vertx.core.Future;
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
  private volatile Subscriber<? super T> subscriber;
  private long demand;
  private boolean cancelled;
  private boolean sentComplete;
  private volatile Throwable failure;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "ctx should be mutable")
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
    if (isFailed()) {
      throw new IllegalStateException(
          "Cannot subscribe to failed publisher. Failure cause: " + failure);
    }
    Objects.requireNonNull(subscriber);
    if (VertxUtils.isEventLoopAndSameContext(ctx)) {
      doSubscribe(subscriber);
    } else {
      ctx.runOnContext(v -> doSubscribe(subscriber));
    }
  }

  public Future<Void> close() {
    ctx.runOnContext(v -> doClose());
    return Future.succeededFuture();
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "ctx should be mutable")
  public Context getContext() {
    return ctx;
  }

  protected void checkContext() {
    VertxUtils.checkContext(ctx);
  }

  protected final void sendError(final Throwable e) {
    checkContext();
    try {
      if (subscriber != null) {
        subscriber.onError(e);
      } else {
        log.error("Failure in publisher", e);
      }
      failure = e;
    } catch (Exception ex) {
      logError("Exception encountered in onError", ex);
    }
  }

  protected void sendComplete() {
    try {
      sentComplete = true;
      subscriber.onComplete();
    } catch (Exception ex) {
      logError("Exception encountered in onComplete", ex);
    }
  }

  protected void doOnNext(final T val) {
    if (!beforeOnNext()) {
      return;
    }
    try {
      subscriber.onNext(val);
    } catch (final Exception ex) {
      logError("Exception encountered in onNext", ex);
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

  protected boolean hasSentComplete() {
    return sentComplete;
  }

  protected boolean isCancelled() {
    return cancelled;
  }

  protected boolean isFailed() {
    return failure != null;
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
      sendError(new IllegalStateException("Exception encountered in onSubscribe", t));
    }
    if (isFailed()) {
      sendError(new IllegalStateException(
          "Cannot subscribe to failed publisher. Failure cause: " + failure));
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

  private void logError(final String message, final Exception e) {
    log.error(message, e);
    failure = e;
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
