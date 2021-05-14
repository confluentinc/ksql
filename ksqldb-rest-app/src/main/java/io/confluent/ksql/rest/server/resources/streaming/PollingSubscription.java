/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.rest.server.resources.streaming;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public abstract class PollingSubscription<T> implements Flow.Subscription {

  private static final int BACKOFF_DELAY_MS = 100;

  private final Flow.Subscriber<T> subscriber;
  private final ListeningScheduledExecutorService exec;
  private final LogicalSchema schema;

  private boolean needsSchema = true;
  private volatile boolean done = false;
  private Throwable exception = null;
  private boolean draining = false;
  private volatile ListenableFuture<?> future;

  public PollingSubscription(
      final ListeningScheduledExecutorService exec,
      final Flow.Subscriber<T> subscriber,
      final LogicalSchema schema
  ) {
    this.exec = Objects.requireNonNull(exec, "exec");
    this.subscriber = Objects.requireNonNull(subscriber, "subscriber");
    this.schema = schema;
  }

  @Override
  public void cancel() {
    if (future != null) {
      future.cancel(false);
    }
    exec.submit(this::close);
  }

  @Override
  public void request(final long n) {
    Preconditions.checkArgument(n == 1, "number of requested items must be 1");

    if (needsSchema) {
      if (schema != null) {
        subscriber.onSchema(schema);
      }
      needsSchema = false;
    }
    // check status since request() can be reentrant through subscriber.onNext()
    // this is to prevent another thread from calling onNext again
    // while the first one is draining and closing the subscription after having called onNext
    // with the last element polled from the queue after being marked done.
    if (!draining) {
      future = exec.submit(() -> {

        if (done) {
          draining = true;
        }
        final T item = poll();
        if (item == null) {
          if (!draining) {
            future = exec.schedule(() -> request(1), BACKOFF_DELAY_MS, TimeUnit.MILLISECONDS);
          }
        } else {
          subscriber.onNext(item);
        }
        if (draining) {
          close();
          if (exception != null) {
            subscriber.onError(exception);
          } else {
            subscriber.onComplete();
          }
        }
      });
    }
  }

  protected void setError(final Throwable e) {
    exception = e;
    done = true;
  }

  protected void setDone() {
    done = true;
  }

  abstract T poll();

  abstract void close();
}
