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
import io.vertx.core.json.JsonObject;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Queue;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * This class is a reactive streams publisher which publishes the inserts to a subscriber which is
 * provided by the back-end. As it's reactive streams it supports back pressure.
 */
public class InsertsPublisherOld implements Publisher<JsonObject> {

  private final Context ctx;
  private final Queue<JsonObject> buffer = new LinkedList<>();

  private final long maxElements; // Really just to satisfy the TCK
  private long delivered;

  private long demand;
  private Subscriber<? super JsonObject> subscriber;

  public InsertsPublisherOld(final Context ctx) {
    this(ctx, Long.MAX_VALUE);
  }

  public InsertsPublisherOld(final Context ctx, final long maxElements) {
    this.ctx = ctx;
    this.maxElements = maxElements;
  }

  public synchronized void subscribe(final Subscriber<? super JsonObject> subscriber) {
    this.subscriber = Objects.requireNonNull(subscriber);
    subscriber.onSubscribe(new InsertsSubscription());
  }

  public synchronized void receiveRow(final JsonObject row) {
    if (demand > 0 && subscriber != null) {
      deliverRow(row);
    } else {
      buffer.add(row);
      System.out.println("Added row to buffer");
    }
  }

  synchronized void acceptTokens(final long number) {
    System.out.println("Accepting tokens " + number);
    demand += number;
    while (demand > 0 && !buffer.isEmpty()) {
      final JsonObject row = buffer.poll();
      deliverRow(row);
    }
  }

  private void deliverRow(final JsonObject row) {
    System.out.println("Delivering row");
    demand--;
    callOnNext(row, subscriber);
    delivered++;
    if (maxElements != Long.MAX_VALUE && delivered == maxElements) {
      System.out.println("Calling on complete");
      callOnComplete(subscriber);
    }
  }

  private void callOnNext(final JsonObject row, final Subscriber<? super JsonObject> subscriber) {
    ctx.runOnContext(v -> subscriber.onNext(row));
  }

  private void callOnComplete(final Subscriber<? super JsonObject> subscriber) {
    ctx.runOnContext(v -> subscriber.onComplete());
  }

  synchronized void cancel() {
  }

  synchronized void close() {
    callOnComplete(subscriber);
    subscriber = null;
  }

  private synchronized void callOnError(final Throwable t) {
    subscriber.onError(t);
  }

  private class InsertsSubscription implements Subscription {

    @Override
    public void request(final long l) {
      if (l <= 0) {
        callOnError(new IllegalArgumentException(
            "3.9 Subscriber cannot request less then 1 for the number of elements."));
      }
      acceptTokens(l);
    }

    @Override
    public void cancel() {
      InsertsPublisherOld.this.cancel();
    }
  }
}
