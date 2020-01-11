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

import io.vertx.core.json.JsonObject;
import java.util.LinkedList;
import java.util.Queue;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * This class is a reactive streams publisher which publishes the inserts to a subscriber which is
 * provided by the back-end. As it's reactive streams it supports back pressure.
 */
public class InsertsPublisher implements Publisher<JsonObject> {

  private final Queue<JsonObject> buffer = new LinkedList<>();
  private long demand;
  private Subscriber<? super JsonObject> subscriber;

  @Override
  public void subscribe(final Subscriber<? super JsonObject> subscriber) {
    this.subscriber = subscriber;
    subscriber.onSubscribe(new InsertsSubscription());
  }

  public synchronized void receiveRow(final JsonObject row) {
    if (subscriber == null) {
      return;
    }
    if (demand > 0) {
      demand--;
      subscriber.onNext(row);
    } else {
      buffer.add(row);
    }
  }

  synchronized void acceptTokens(final long number) {
    demand += number;
    for (int i = 0; i < buffer.size(); i++) {
      JsonObject row = buffer.poll();
      demand--;
      subscriber.onNext(row);
    }
  }

  synchronized void close() {
    subscriber.onComplete();
    subscriber = null;
  }

  private class InsertsSubscription implements Subscription {

    @Override
    public void request(final long l) {
      if (l <= 0) {
        throw new IllegalArgumentException("Tokens must be > 0 " + l);
      }
      acceptTokens(l);
    }

    @Override
    public void cancel() {
      close();
    }
  }
}
