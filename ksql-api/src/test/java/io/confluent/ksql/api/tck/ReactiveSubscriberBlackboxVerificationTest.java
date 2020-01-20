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
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.reactivestreams.tck.SubscriberBlackboxVerification;
import org.reactivestreams.tck.TestEnvironment;

/*
 *
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
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
