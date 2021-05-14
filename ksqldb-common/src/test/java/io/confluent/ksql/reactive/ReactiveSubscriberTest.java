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

package io.confluent.ksql.reactive;

import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.vertx.core.Context;
import io.vertx.core.Vertx;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
    AtomicBoolean wrongContext = new AtomicBoolean();
    AtomicBoolean afterSubScribeCalled = new AtomicBoolean();
    AtomicBoolean handleValueCalled = new AtomicBoolean();
    BaseSubscriber<String> subscriber = new BaseSubscriber<String>(context) {
      @Override
      protected void afterSubscribe(final Subscription subscription) {
        checkCorrectContext(wrongContext, context);
        afterSubScribeCalled.set(true);
      }

      @Override
      protected void handleValue(final String record) {
        checkCorrectContext(wrongContext, context);
        handleValueCalled.set(true);
      }

      @Override
      protected void handleComplete() {
        checkCorrectContext(wrongContext, context);
        latch.countDown();
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
    assertThatEventually(afterSubScribeCalled::get, is(true));
    subscriber.onNext("record0");
    subscriber.onComplete();
    awaitLatch(latch);
    assertThat(wrongContext.get(), is(false));
    assertThat(handleValueCalled.get(), is(true));
  }

  @Test
  public void shouldBeCalledOnContextOnError() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicBoolean wrongContext = new AtomicBoolean();
    BaseSubscriber<String> subscriber = new BaseSubscriber<String>(context) {

      @Override
      protected void handleError(final Throwable t) {
        checkCorrectContext(wrongContext, context);
        latch.countDown();
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
    awaitLatch(latch);
    assertThat(wrongContext.get(), is(false));
  }

  @Test
  public void shouldCancel() throws Exception {
    // Given
    TestReactiveSubscriber subscriber = new TestReactiveSubscriber(context);
    final TestSubscription sub = new TestSubscription();
    subscriber.onSubscribe(sub);

    // When
    context.runOnContext(v -> subscriber.cancel());
    final CountDownLatch latch = new CountDownLatch(1); // Wait for async processing to complete
    context.runOnContext(v -> latch.countDown());
    awaitLatch(latch);
    subscriber.onError(new IllegalStateException("foo"));
    subscriber.onNext("record0");
    subscriber.onComplete();

    // Then

    assertThat(sub.isCancelled(), is(true));
    assertThat(subscriber.isHandleValueCalled(), is(false));
    assertThat(subscriber.isHandleCompleteCalled(), is(false));
    assertThat(subscriber.isHandleErrorCalled(), is(false));
  }

  private void checkCorrectContext(AtomicBoolean wrongContext, Context context) {
    if (Vertx.currentContext() != context) {
      wrongContext.set(true);
    }
  }

  protected static void awaitLatch(CountDownLatch latch) throws Exception {
    assertThat(latch.await(2000, TimeUnit.MILLISECONDS), is(true));
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

  private static class TestReactiveSubscriber extends BaseSubscriber<String> {

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
