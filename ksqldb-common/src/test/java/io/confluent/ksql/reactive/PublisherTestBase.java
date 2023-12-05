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

package io.confluent.ksql.reactive;

import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * More BufferedPublisher testing occurs in the TCK tests
 */
public abstract class PublisherTestBase<T> {

  protected Vertx vertx;
  protected Context context;
  protected Publisher<T> publisher;

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
    context = vertx.getOrCreateContext();
    publisher = createPublisher();
  }

  @After
  public void tearDown() {
    vertx.close();
  }

  protected abstract Publisher<T> createPublisher();

  protected abstract T expectedValue(int i);

  @Test
  public void shouldCallOnSubscribe() throws Exception {
    TestSubscriber<T> subscriber = new TestSubscriber<>(context);
    subscribeOnContext(subscriber);
    assertThat(subscriber.getSub(), is(notNullValue()));
  }

  @Test
  public void shouldDeliverOneRecordWhenOneIsRequested() throws Exception {
    loadPublisher(10);
    shouldDeliver(1, 1);
  }

  @Test
  public void shouldDeliverSevenRecordsWhenSevenIsRequested() throws Exception {
    loadPublisher(10);
    shouldDeliver(7, 7);
  }

  @Test
  public void shouldDeliverAllRecordsWhenAllAreRequested() throws Exception {
    loadPublisher(10);
    shouldDeliver(10, 10);
  }

  @Test
  public void shouldDeliverAllRecordsWhenMoreAreRequested() throws Exception {
    loadPublisher(10);
    shouldDeliver(15, 10);
  }

  @Test
  public void shouldDeliverAllRequestingOneByOne() throws Exception {
    loadPublisher(10);
    TestSubscriber<T> subscriber = new TestSubscriber<T>(context) {

      @Override
      public synchronized void onSubscribe(final Subscription sub) {
        super.onSubscribe(sub);
        sub.request(1);
      }

      @Override
      public synchronized void onNext(final T value) {
        super.onNext(value);
        getSub().request(1);
      }
    };
    subscribeOnContext(subscriber);
    assertThatEventually(subscriber::getValues, hasSize(10));
    for (int i = 0; i < 10; i++) {
      assertThat(subscriber.getValues().get(i), equalTo(expectedValue(i)));
    }
    assertThat(subscriber.isCompleted(), equalTo(false));
    assertThat(subscriber.getError(), is(nullValue()));
  }

  @Test
  public void shouldDeliverAllRequestingOneByOneLoadAfterSubscribe() throws Exception {
    TestSubscriber<T> subscriber = new TestSubscriber<T>(context) {

      @Override
      public synchronized void onSubscribe(final Subscription sub) {
        super.onSubscribe(sub);
        sub.request(1);
      }

      @Override
      public synchronized void onNext(final T value) {
        super.onNext(value);
        getSub().request(1);
      }
    };
    subscribeOnContext(subscriber);
    loadPublisher(10);
    assertThatEventually(subscriber::getValues, hasSize(10));
    for (int i = 0; i < 10; i++) {
      assertThat(subscriber.getValues().get(i), equalTo(expectedValue(i)));
    }
    assertThat(subscriber.isCompleted(), equalTo(false));
    assertThat(subscriber.getError(), is(nullValue()));
  }

  @Test
  public void shouldFailWhenZeroRequested() throws Exception {
    loadPublisher(10);
    TestSubscriber<T> subscriber = new TestSubscriber<T>(context) {

      @Override
      public synchronized void onSubscribe(final Subscription sub) {
        super.onSubscribe(sub);
        sub.request(0);
      }
    };
    subscribeOnContext(subscriber);
    assertThatEventually(subscriber::getError, is(notNullValue()));
    assertThat(subscriber.getValues(), hasSize(0));
    assertThat(subscriber.getError(), instanceOf(IllegalArgumentException.class));
  }

  @Test
  public void shouldFailWhenNegativeRequested() throws Exception {
    loadPublisher(10);
    TestSubscriber<T> subscriber = new TestSubscriber<T>(context) {

      @Override
      public synchronized void onSubscribe(final Subscription sub) {
        super.onSubscribe(sub);
        sub.request(-1);
      }
    };
    subscribeOnContext(subscriber);
    assertThatEventually(subscriber::getError, is(notNullValue()));
    assertThat(subscriber.getValues(), hasSize(0));
    assertThat(subscriber.getError(), instanceOf(IllegalArgumentException.class));
  }

  protected abstract void loadPublisher(int num) throws Exception;

  protected void subscribeOnContext(Subscriber<T> subScriber) throws Exception {
    execOnContextAndWait(() -> publisher.subscribe(subScriber));
  }

  protected void execOnContextAndWait(Runnable action) throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    context.runOnContext(v -> {
      action.run();
      latch.countDown();
    });
    awaitLatch(latch);
  }

  protected void shouldDeliver(int numRequested, int numDelivered) throws Exception {
    TestSubscriber<T> subscriber = new TestSubscriber<T>(context) {
      @Override
      public synchronized void onSubscribe(final Subscription sub) {
        super.onSubscribe(sub);
        sub.request(numRequested);
      }
    };
    subscribeOnContext(subscriber);
    assertThatEventually(subscriber::getValues, hasSize(numDelivered));
    for (int i = 0; i < numDelivered; i++) {
      assertThat(subscriber.getValues().get(i), equalTo(expectedValue(i)));
    }
    assertThat(subscriber.isCompleted(), equalTo(false));
    assertThat(subscriber.getError(), is(nullValue()));
  }

  protected static void awaitLatch(CountDownLatch latch) throws Exception {
    assertThat(latch.await(2000, TimeUnit.MILLISECONDS), is(true));
  }

  protected static class TestSubscriber<T> implements Subscriber<T> {

    private Subscription sub;
    private boolean completed;
    private Throwable error;
    private final List<T> values = new ArrayList<>();
    private final Context subscriberContext;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
    public TestSubscriber(final Context subscriberContext) {
      this.subscriberContext = subscriberContext;
    }

    @Override
    public synchronized void onSubscribe(final Subscription sub) {
      checkContext();
      this.sub = sub;
    }

    @Override
    public synchronized void onNext(final T value) {
      checkContext();
      values.add(value);
    }

    @Override
    @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
    public synchronized void onError(final Throwable t) {
      checkContext();
      this.error = t;
    }

    @Override
    public synchronized void onComplete() {
      checkContext();
      this.completed = true;
    }

    public boolean isCompleted() {
      return completed;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP")
    public Throwable getError() {
      return error;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP")
    public List<T> getValues() {
      return values;
    }

    public Subscription getSub() {
      return sub;
    }

    private void checkContext() {
      if (Vertx.currentContext() != subscriberContext) {
        throw new IllegalStateException("On wrong context");
      }
    }
  }

}
