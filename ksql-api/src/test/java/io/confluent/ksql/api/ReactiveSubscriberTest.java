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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.confluent.ksql.api.server.ReactiveSubscriber;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import java.util.concurrent.CountDownLatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Subscription;

/**
 * Much of the testing of the ReactiveSubscriber is done via the TCK tests
 */
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

  @Test
  public void shouldCancel() {
    // Given
    TestReactiveSubscriber subscriber = new TestReactiveSubscriber(context);
    final TestSubscription sub = new TestSubscription();
    subscriber.onSubscribe(sub);

    // When
    context.runOnContext(v -> subscriber.cancel());
    subscriber.onError(new IllegalStateException("foo"));
    subscriber.onNext("record0");
    subscriber.onComplete();

    // Then
    final CountDownLatch latch = new CountDownLatch(1); // Wait for async processing to complete
    vertx.runOnContext(v -> latch.countDown());
    assertThat(sub.isCancelled(), is(true));
    assertThat(subscriber.isHandleValueCalled(), is(false));
    assertThat(subscriber.isHandleCompleteCalled(), is(false));
    assertThat(subscriber.isHandleErrorCalled(), is(false));
  }

  private static class TestSubscription implements Subscription {

    private boolean cancelled;

    @Override
    public void request(final long n) {
    }

    @Override
    public synchronized void cancel() {
      cancelled = true;
    }

    synchronized boolean isCancelled() {
      return cancelled;
    }
  }

  private static class TestReactiveSubscriber extends ReactiveSubscriber<String> {

    private boolean handleValueCalled;
    private boolean handleCompleteCalled;
    private boolean handleErrorCalled;

    public TestReactiveSubscriber(final Context context) {
      super(context);
    }

    @Override
    protected synchronized void handleValue(final String record) {
      handleValueCalled = true;
    }

    @Override
    protected synchronized void handleComplete() {
      handleCompleteCalled = true;
    }

    @Override
    protected synchronized void handleError(final Throwable t) {
      handleErrorCalled = true;
    }

    synchronized boolean isHandleValueCalled() {
      return handleValueCalled;
    }

    synchronized boolean isHandleCompleteCalled() {
      return handleCompleteCalled;
    }

    synchronized boolean isHandleErrorCalled() {
      return handleErrorCalled;
    }
  }

}
