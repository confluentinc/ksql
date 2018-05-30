/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.rest.server.resources.streaming;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import org.apache.kafka.connect.data.Schema;

import java.util.concurrent.TimeUnit;

public abstract class PollingSubscription<T> implements Flow.Subscription {

  private static final int BACKOFF_DELAY_MS = 100;

  private final Flow.Subscriber<T> subscriber;
  private final ListeningScheduledExecutorService exec;
  private final Schema schema;

  private boolean needsSchema = true;
  private volatile boolean done = false;
  private Throwable exception = null;
  private boolean draining = false;
  private volatile ListenableFuture future;

  public PollingSubscription(
      ListeningScheduledExecutorService exec,
      Flow.Subscriber<T> subscriber,
      Schema schema
  ) {
    this.exec = exec;
    this.subscriber = subscriber;
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
  public void request(long n) {
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
        T item = poll();
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


  protected void setError(Throwable e) {
    exception = e;
    done = true;
  }

  protected void setDone() {
    done = true;
  }

  abstract T poll();

  abstract void close();
}
