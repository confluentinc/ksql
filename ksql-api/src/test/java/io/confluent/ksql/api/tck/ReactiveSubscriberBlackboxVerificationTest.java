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

package io.confluent.ksql.api.tck;

import io.confluent.ksql.api.server.ReactiveSubscriber;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.reactivestreams.tck.SubscriberBlackboxVerification;
import org.reactivestreams.tck.TestEnvironment;

public class ReactiveSubscriberBlackboxVerificationTest extends
    SubscriberBlackboxVerification<JsonObject> {

  private final Vertx vertx;

  public ReactiveSubscriberBlackboxVerificationTest() {
    super(new TestEnvironment(500));
    this.vertx = Vertx.vertx();
  }

  @Override
  public JsonObject createElement(int i) {
    return new JsonObject().put("x", i);
  }

  @Override
  public Subscriber<JsonObject> createSubscriber() {
    final Context context = vertx.getOrCreateContext();
    return new ReactiveSubscriber<JsonObject>(context) {

      private Subscription subscription;

      @Override
      public synchronized void afterSubscribe(final Subscription s) {
        s.request(1);
        this.subscription = s;
      }

      @Override
      public synchronized void handleValue(final JsonObject jsonObject) {
        subscription.request(1);
      }
    };
  }

}
