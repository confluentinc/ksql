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

package io.confluent.ksql.api.tck;

import io.confluent.ksql.reactive.BaseSubscriber;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.junit.Ignore;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.reactivestreams.tck.SubscriberWhiteboxVerification;
import org.reactivestreams.tck.TestEnvironment;

@Ignore
public class BaseSubscriberWhiteboxVerificationTest extends
    SubscriberWhiteboxVerification<JsonObject> {

  private final Vertx vertx;

  public BaseSubscriberWhiteboxVerificationTest() {
    super(new TestEnvironment(1000));
    this.vertx = Vertx.vertx();
  }

  @Override
  public JsonObject createElement(final int element) {
    return new JsonObject().put("x", element);
  }

  @Override
  public Subscriber<JsonObject> createSubscriber(final WhiteboxSubscriberProbe<JsonObject> probe) {
    final Context context = vertx.getOrCreateContext();
    return new BaseSubscriber<JsonObject>(context) {

      private Subscription subscription;

      @Override
      protected void afterSubscribe(final Subscription subscription) {
        probe.registerOnSubscribe(new SubscriberPuppet() {

          @Override
          public void triggerRequest(long n) {
            subscription.request(n);
          }

          @Override
          public void signalCancel() {
            subscription.cancel();
          }
        });
        subscription.request(1);
        this.subscription = subscription;
      }

      @Override
      protected void handleValue(final JsonObject value) {
        probe.registerOnNext(value);
        subscription.request(1);
      }

      @Override
      protected void handleComplete() {
        probe.registerOnComplete();
      }

      @Override
      public void handleError(final Throwable t) {
        probe.registerOnError(t);
      }
    };
  }

}
