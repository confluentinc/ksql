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
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * A reactive streams subscriber which handles much of the plumbing for you and executes the onXXX
 * methods asynchronous on the context after being called from the publisher.
 *
 * <p>Override {@link #afterSubscribe}, {@link #handleValue}, {@link #handleComplete} and
 * {@link #handleError} to create your specific implementation.
 *
 * <p>The state for this subscriber will always be accessed on the
 * same Vert.x context so does not require synchronization.
 *
 * @param <T> The type of the value
 */
public class BaseSubscriber<T> implements Subscriber<T> {

  private static final Logger log = LogManager.getLogger(BaseSubscriber.class);

  protected final Context context;
  private Subscription subscription;
  private boolean complete;
  private boolean cancelled;

  /**
   * Construct a ReactiveSubscriber
   *
   * @param context The Vert.x context to use for the subscriber - the subscriber code will always
   *                be executed on this context. This ensures the code is never executed
   *                concurrently by more than one thread.
   */
  @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "context should be mutable")
  public BaseSubscriber(final Context context) {
    this.context = Objects.requireNonNull(context);
  }

  @Override
  public final void onSubscribe(final Subscription s) {
    Objects.requireNonNull(s);
    runOnRightContext(() -> doOnSubscribe(s));
  }

  @Override
  public final void onNext(final T t) {
    Objects.requireNonNull(t);
    runOnRightContext(() -> doOnNext(t));
  }

  @Override
  public final void onError(final Throwable t) {
    Objects.requireNonNull(t);
    runOnRightContext(() -> doOnError(t));
  }

  public void cancel() {
    checkContext();
    cancelled = true;
    if (subscription != null) {
      subscription.cancel();
    }
  }

  @Override
  public final void onComplete() {
    runOnRightContext(this::doOnComplete);
  }

  /**
   * Hook that will be called after onSubscribe has completed
   *
   * @param subscription The subscription
   */
  protected void afterSubscribe(final Subscription subscription) {
  }

  /**
   * Hook that will be called when a value is received by the subscriber Implement this hook to
   * handle the actual value
   *
   * @param t The value
   */
  protected void handleValue(final T t) {
  }

  /**
   * Hook that will be called when the strem is complete. Implement this hook for your custom
   * complete handling
   */
  protected void handleComplete() {
  }

  /**
   * Hook that will be called when an error is signalled by the publisher
   *
   * @param t The exception
   */
  protected void handleError(final Throwable t) {
  }

  protected final void makeRequest(final long l) {
    checkContext();
    try {
      subscription.request(l);
    } catch (Throwable t) {
      final Exception e =
          new IllegalStateException("Exceptions must not be thrown from request", t);
      logError(e);
    }
  }

  protected final void complete() {
    checkContext();
    complete = true;
    if (subscription != null) {
      try {
        subscription.cancel();
      } catch (final Throwable t) {
        final Exception e =
            new IllegalStateException("Exceptions must not be thrown from cancel", t);
        logError(e);
      }
    }
  }

  protected final void checkContext() {
    VertxUtils.checkContext(context);
  }

  private void runOnRightContext(final Runnable runnable) {
    if (VertxUtils.isEventLoopAndSameContext(context)) {
      // Execute directly
      runnable.run();
    } else {
      context.runOnContext(v -> runnable.run());
    }
  }

  private void logError(final Throwable t) {
    log.error(t.getMessage(), t);
  }

  private void doOnSubscribe(final Subscription subscription) {
    checkContext();
    if (this.subscription != null) {
      try {
        // Cancel the new subscription
        subscription.cancel();
      } catch (final Throwable t) {
        final Exception e =
            new IllegalStateException("Exceptions must not be thrown from cancel", t);
        logError(e);
      }
      return;
    }
    this.subscription = subscription;
    afterSubscribe(subscription);
  }

  private void doOnNext(final T val) {
    checkContext();
    if (complete || cancelled) {
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
    if (cancelled) {
      return;
    }
    if (subscription == null) {
      logError(new IllegalStateException("onError must not be called before onSubscribe", t));
    } else {
      complete = true;
      handleError(t);
    }
  }

  private void doOnComplete() {
    checkContext();
    if (cancelled || complete) {
      return;
    }
    if (subscription == null) {
      logError(new IllegalStateException("onComplete must not be called before onSubscribe"));
    } else {
      complete = true;
      handleComplete();
    }
  }

}
