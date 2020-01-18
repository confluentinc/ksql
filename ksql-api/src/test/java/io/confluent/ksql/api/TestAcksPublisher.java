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

package io.confluent.ksql.api;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class TestAcksPublisher implements Publisher<JsonObject> {

  private final Vertx vertx;
  private final int acksBeforePublisherError;
  private Subscriber<? super JsonObject> subscriber;
  private long acks;
  private long tokens;
  private int acksSent;

  private static final JsonObject ACK = new JsonObject();

  public TestAcksPublisher(final Vertx vertx, final int acksBeforePublisherError) {
    this.vertx = vertx;
    this.acksBeforePublisherError = acksBeforePublisherError;
  }

  void receiveRow(final JsonObject row) {
    // run async to simulate some processing time
    vertx.runOnContext(v -> sendAck());
  }

  synchronized void sendAck() {
    if (subscriber == null) {
      return;
    }
    if (tokens == 0) {
      acks++;
    } else {
      tokens--;
      deliverAck();
    }
  }

  synchronized void acceptTokens(final long num) {
    if (subscriber == null) {
      throw new IllegalStateException("No subscriber");
    }
    tokens += num;
    while (tokens > 0 && acks > 0) {
      deliverAck();
      acks--;
      tokens--;
    }
  }

  private void deliverAck() {
    if (acksBeforePublisherError != -1 && acksSent == acksBeforePublisherError) {
      // Inject an error
      subscriber.onError(new RuntimeException("Failure in processing"));
    } else {
      subscriber.onNext(ACK);
      acksSent++;
    }
  }

  @Override
  public synchronized void subscribe(final Subscriber<? super JsonObject> subscriber) {
    if (this.subscriber != null) {
      throw new IllegalStateException("Already subscribed");
    }
    this.subscriber = subscriber;
    subscriber.onSubscribe(new AcksSubscription());
  }

  public synchronized void cancel() {
    this.subscriber = null;
  }

  public synchronized boolean hasSubscriber() {
    return subscriber != null;
  }

  class AcksSubscription implements Subscription {

    @Override
    public void request(final long num) {
      acceptTokens(num);
    }

    @Override
    public void cancel() {
      TestAcksPublisher.this.cancel();
    }
  }
}
