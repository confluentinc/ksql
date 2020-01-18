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
import java.util.Objects;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ReactiveSubscriber<T> implements Subscriber<T> {

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

  public abstract void handleValue(T t);

  public abstract void handleComplete();

  public abstract void handleError(Throwable t);

  private void doOnSubscribe(final Subscription subscription) {
    checkContext();
    if (this.subscription != null) {
      try {
        // Cancel the new subscription
        subscription.cancel();
      } catch (final Throwable t) {
        final Exception e =
            new IllegalStateException("Subscription violated the Reactive Streams "
                + "rule 3.15 by throwing an exception from cancel.");
        logError(e);
      }
    }
    this.subscription = subscription;
    makeRequest(1);
  }

  private void doOnNext(final T val) {
    checkContext();
    if (complete) {
      return;
    }
    if (subscription == null) {
      final Exception e = new IllegalStateException(
          "Someone violated the Reactive Streams rule 1.09 and 2.1 by signalling OnNext before "
              + "`Subscription.request`. (no Subscription)");
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
      logError(new IllegalStateException("Publisher violated the Reactive Streams rule 1.09 "
          + "signalling onError prior to onSubscribe."));
    } else {
      complete = true;
      handleError(t);
    }
  }

  private void doOnComplete() {
    checkContext();
    if (subscription == null) {
      logError(new IllegalStateException("Publisher violated the Reactive Streams rule 1.09 "
          + "signalling onComplete prior to onSubscribe."));
    } else {
      complete = true;
      handleComplete();
    }
  }

  protected void makeRequest(final long l) {
    checkContext();
    try {
      // TODO - batch size?
      subscription.request(l);
    } catch (Throwable t) {
      final Exception e =
          new IllegalStateException("Subscription violated the Reactive Streams rule 3.16 by "
              + "throwing an exception from request.");
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
            new IllegalStateException("Subscription violated the Reactive Streams rule 3.15 by "
                + "throwing an exception from cancel.");
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
