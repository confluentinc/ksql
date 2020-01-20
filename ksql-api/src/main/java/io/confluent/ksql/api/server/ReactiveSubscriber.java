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
import java.util.Objects;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A reactive streams subscriber which handles much of the plumbing for you. Override {@link
 * #afterSubscribe}, {@link #handleValue}, {@link #handleComplete} and {@link #handleError} to
 * create your specific implementation.
 *
 * @param <T> The type of the value
 */
public class ReactiveSubscriber<T> implements Subscriber<T> {

  private static final Logger log = LoggerFactory.getLogger(ReactiveSubscriber.class);

  protected final Context context;
  private Subscription subscription;
  private boolean complete;

  public ReactiveSubscriber(final Context context) {
    this.context = context;
  }

  @Override
  public void onSubscribe(final Subscription s) {
    Objects.requireNonNull(s);
    context.runOnContext(v -> doOnSubscribe(s));
  }

  @Override
  public void onNext(final T t) {
    Objects.requireNonNull(t);
    context.runOnContext(v -> doOnNext(t));
  }

  @Override
  public void onError(final Throwable t) {
    Objects.requireNonNull(t);
    context.runOnContext(v -> doOnError(t));
  }

  @Override
  public void onComplete() {
    context.runOnContext(v -> doOnComplete());
  }

  protected void afterSubscribe(final Subscription subscription) {
  }

  protected void handleValue(final T t) {
  }

  protected void handleComplete() {
  }

  protected void handleError(final Throwable t) {
  }

  private void doOnSubscribe(final Subscription subscription) {
    checkContext();
    if (this.subscription != null) {
      try {
        // Cancel the new subscription
        subscription.cancel();
      } catch (final Throwable t) {
        final Exception e =
            new IllegalStateException("Exceptions must not be thrown from cancel");
        logError(e);
      }
    }
    this.subscription = subscription;
    afterSubscribe(subscription);
  }

  private void doOnNext(final T val) {
    checkContext();
    if (complete) {
      return;
    }
    if (subscription == null) {
      final Exception e = new IllegalStateException(
          "onNext must be called without request being called");
      logError(e);
    }
    try {
      handleValue(val);
    } catch (final Throwable t) {
      complete();
      onError(t);
    }
  }

  private void doOnError(final Throwable t) {
    checkContext();
    if (subscription == null) {
      logError(new IllegalStateException("onError must not be called before onSubscribe"));
    } else {
      complete = true;
      handleError(t);
    }
  }

  private void doOnComplete() {
    checkContext();
    if (subscription == null) {
      logError(new IllegalStateException("onComplete must not be called before onSubscribe"));
    } else {
      complete = true;
      handleComplete();
    }
  }

  protected void makeRequest(final long l) {
    checkContext();
    try {
      subscription.request(l);
    } catch (Throwable t) {
      final Exception e =
          new IllegalStateException("Exceptions must not be thrown from request");
      logError(e);
    }
  }

  protected void complete() {
    checkContext();
    complete = true;
    if (subscription != null) {
      try {
        subscription.cancel();
      } catch (final Throwable t) {
        final Exception e =
            new IllegalStateException("Exceptions must not be thrown from cancel");
        logError(e);
      }
    }
  }

  protected void checkContext() {
    if (Vertx.currentContext() != context) {
      throw new IllegalStateException("On wrong context");
    }
  }

  private void logError(final Throwable t) {
    log.error("Failure in subscriber", t);
  }

}
