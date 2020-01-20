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

package io.confluent.ksql.api;


import static io.confluent.ksql.api.TestUtils.awaitLatch;

import io.confluent.ksql.api.server.ReactiveSubscriber;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import java.util.concurrent.CountDownLatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Subscription;

public class ReactiveSubscriberTest {

  private Vertx vertx;
  private Context context;

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
    context = vertx.getOrCreateContext();
  }

  @After
  public void tearDown() {
    vertx.close();
  }

  @Test
  public void shouldBeCalledOnContext() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    ReactiveSubscriber<String> subscriber = new ReactiveSubscriber<String>(context) {
      @Override
      protected void afterSubscribe(final Subscription subscription) {
        checkContext();
      }

      @Override
      protected void handleValue(final String record) {
        checkContext();
      }

      @Override
      protected void handleComplete() {
        checkContext();
        latch.countDown();
      }

      @Override
      protected void handleError(final Throwable t) {
        checkContext();
      }
    };
    subscriber.onSubscribe(new Subscription() {
      @Override
      public void request(final long n) {
      }

      @Override
      public void cancel() {
      }
    });
    subscriber.onError(new IllegalStateException("foo"));
    subscriber.onNext("record0");
    subscriber.onComplete();
    awaitLatch(latch);
  }

}
