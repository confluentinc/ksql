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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import org.apache.kafka.connect.data.Schema;

import java.util.concurrent.TimeUnit;

public class PollingSubscription<T> implements Flow.Subscription {

  private static final int BACKOFF_DELAY_MS = 100;

  public interface Pollable<T> {

    Schema getSchema();

    T poll();

    boolean hasError();

    Throwable getError();

    void close();
  }

  private final Flow.Subscriber<T> subscriber;
  private final ListeningScheduledExecutorService exec;
  private final Pollable<T> pollable;

  private boolean needsSchema = true;
  private boolean draining = false;
  private volatile ListenableFuture future;

  public PollingSubscription(
      ListeningScheduledExecutorService exec,
      Flow.Subscriber<T> subscriber,
      Pollable<T> pollable
  ) {
    this.exec = exec;
    this.subscriber = subscriber;
    this.pollable = pollable;
  }

  @Override
  public void cancel() {
    if (future != null) {
      future.cancel(false);
    }
    exec.submit(pollable::close);
  }

  @Override
  public void request(long n) {
    if (needsSchema) {
      final Schema schema = pollable.getSchema();
      if (schema != null) {
        subscriber.onSchema(schema);
      }
      needsSchema = false;
    }
    // check draining status since request can be reentrant
    if (!draining) {
      future = exec.submit(() -> {

        if (pollable.hasError()) {
          draining = true;
        }
        T item = pollable.poll();
        if (item == null) {
          future = exec.schedule(() -> request(1), BACKOFF_DELAY_MS, TimeUnit.MILLISECONDS);
        } else {
          subscriber.onNext(item);
        }
        if (draining) {
          pollable.close();
          subscriber.onError(pollable.getError());
        }
      });
    }
  }
}
