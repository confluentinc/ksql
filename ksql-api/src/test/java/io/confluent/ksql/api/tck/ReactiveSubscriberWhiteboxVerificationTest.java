/*
 * Copyright 2014 Red Hat, Inc.
 *
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v1.0
 *  and Apache License v2.0 which accompanies this distribution.
 *
 *  The Eclipse Public License is available at
 *  http://www.eclipse.org/legal/epl-v10.html
 *
 *  The Apache License v2.0 is available at
 *  http://www.opensource.org/licenses/apache2.0.php
 *
 *  You may elect to redistribute this code under either of these licenses.
 */

package io.confluent.ksql.api.tck;

import io.confluent.ksql.api.server.ReactiveSubscriber;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.junit.Ignore;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.reactivestreams.tck.SubscriberWhiteboxVerification;
import org.reactivestreams.tck.TestEnvironment;

/*
 *
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
@Ignore
public class ReactiveSubscriberWhiteboxVerificationTest extends
    SubscriberWhiteboxVerification<JsonObject> {

  private final Vertx vertx;

  public ReactiveSubscriberWhiteboxVerificationTest() {
    super(new TestEnvironment(500));
    this.vertx = Vertx.vertx();
  }

  @Override
  public JsonObject createElement(final int element) {
    return new JsonObject().put("x", element);
  }

  @Override
  public Subscriber<JsonObject> createSubscriber(final WhiteboxSubscriberProbe<JsonObject> probe) {
    final Context context = vertx.getOrCreateContext();
    return new ReactiveSubscriber<JsonObject>(context) {

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
