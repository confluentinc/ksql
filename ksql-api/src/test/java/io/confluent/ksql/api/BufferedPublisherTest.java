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

import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.fail;

import io.confluent.ksql.api.TestUtils.AsyncAssert;
import io.confluent.ksql.api.server.BufferedPublisher;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * More BufferedPublisher testing occurs in the TCK tests
 */
public class BufferedPublisherTest {

  private Vertx vertx;
  private Context context;
  private BufferedPublisher<String> publisher;

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
    context = vertx.getOrCreateContext();
    publisher = new BufferedPublisher<>(context);
  }

  @After
  public void tearDown() {
    vertx.close();
  }

  @Test
  public void shouldNotAllowSettingDrainHandlerMoreThanOnce() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    context.runOnContext(v -> {
      publisher.drainHandler(() -> {
      });
      try {
        publisher.drainHandler(() -> {
        });
        fail("Should throw exception");
      } catch (IllegalStateException e) {
        // OK
        latch.countDown();
      }
    });
    TestUtils.awaitLatch(latch);
  }

  @Test
  public void shouldCallOnSubscribe() throws Exception {
    TestSubscriber subscriber = new TestSubscriber(context);
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
  public void shouldDeliverMoreThanMaxSendBatchSize() throws Exception {
    int num = 2 * BufferedPublisher.SEND_MAX_BATCH_SIZE;
    loadPublisher(num);
    shouldDeliver(num, num);
  }

  @Test
  public void shouldDeliverAllRequestingOneByOne() throws Exception {
    loadPublisher(10);
    TestSubscriber subscriber = new TestSubscriber(context) {

      @Override
      public synchronized void onSubscribe(final Subscription sub) {
        super.onSubscribe(sub);
        sub.request(1);
      }

      @Override
      public synchronized void onNext(final String value) {
        super.onNext(value);
        getSub().request(1);
      }
    };
    subscribeOnContext(subscriber);
    assertThatEventually(subscriber::getValues, hasSize(10));
    for (int i = 0; i < 10; i++) {
      assertThat(subscriber.getValues().get(i), equalTo("record" + i));
    }
    assertThat(subscriber.isCompleted(), equalTo(false));
    assertThat(subscriber.getError(), is(nullValue()));
  }

  @Test
  public void shouldFailWhenZeroRequested() throws Exception {
    loadPublisher(10);
    TestSubscriber subscriber = new TestSubscriber(context) {

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
    TestSubscriber subscriber = new TestSubscriber(context) {

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

  @Test
  public void shouldCompleteWhenNoRecords() throws Exception {
    TestSubscriber subscriber = new TestSubscriber(context);
    subscribeOnContext(subscriber);
    execOnContextAndWait(publisher::complete);
    assertThatEventually(subscriber::isCompleted, equalTo(true));
  }

  @Test
  public void shouldCompleteAfterDeliveringRecords() throws Exception {
    loadPublisher(10);
    AsyncAssert asyncAssert = new AsyncAssert();
    TestSubscriber subscriber = new TestSubscriber(context) {
      @Override
      public synchronized void onSubscribe(final Subscription sub) {
        super.onSubscribe(sub);
        sub.request(1);
      }

      @Override
      public synchronized void onNext(final String value) {
        super.onNext(value);
        asyncAssert.assertAsync(isCompleted(), equalTo(false));
        getSub().request(1);
      }
    };
    subscribeOnContext(subscriber);
    execOnContextAndWait(publisher::complete);
    assertThatEventually(subscriber::isCompleted, equalTo(true));
    assertThat(subscriber.getValues(), hasSize(10));
    for (int i = 0; i < 10; i++) {
      assertThat(subscriber.getValues().get(i), equalTo("record" + i));
    }
    asyncAssert.throwAssert();
  }

  @Test
  public void shouldCompleteAfterDeliveringRecordsNoBuffering() throws Exception {
    AsyncAssert asyncAssertOnNext = new AsyncAssert();
    TestSubscriber subscriber = new TestSubscriber(context) {
      @Override
      public synchronized void onSubscribe(final Subscription sub) {
        super.onSubscribe(sub);
        sub.request(1);
      }

      @Override
      public synchronized void onNext(final String value) {
        super.onNext(value);
        asyncAssertOnNext.assertAsync(isCompleted(), equalTo(false));
        getSub().request(1);
      }
    };
    subscribeOnContext(subscriber);
    AsyncAssert assertNotBufferFull = new AsyncAssert();
    for (int i = 0; i < 10; i++) {
      String record = "record" + i;
      execOnContextAndWait(() -> {
        boolean bufferFull = publisher.accept(record);
        assertNotBufferFull.assertAsync(bufferFull, equalTo(false));
      });
      assertThatEventually(subscriber::getValues, hasSize(i + 1));
      assertThat(subscriber.getValues().get(i), equalTo(record));
    }
    asyncAssertOnNext.throwAssert();
    assertNotBufferFull.throwAssert();

    execOnContextAndWait(publisher::complete);
    assertThatEventually(subscriber::isCompleted, equalTo(true));
  }

  @Test
  public void shouldNotAllowAcceptingAfterComplete() throws Exception {
    TestSubscriber subscriber = new TestSubscriber(context);
    subscribeOnContext(subscriber);
    execOnContextAndWait(publisher::complete);
    AtomicBoolean failed = new AtomicBoolean();
    execOnContextAndWait(() -> {
      try {
        publisher.accept("foo");
        failed.set(true);
      } catch (IllegalStateException e) {
        // OK
      }
    });
    assertThat(failed.get(), equalTo(false));
  }

  @Test
  public void shouldAcceptBuffered() throws Exception {
    publisher = new BufferedPublisher<>(context, 5);
    AsyncAssert asyncAssert = new AsyncAssert();
    for (int i = 0; i < 10; i++) {
      String record = "record" + i;
      final int index = i;
      execOnContextAndWait(() -> {
        boolean bufferFull = publisher.accept(record);
        asyncAssert.assertAsync(bufferFull, equalTo(index >= 5));
      });
    }
  }

  @Test
  public void shouldAcceptNotBuffered() throws Exception {
    publisher = new BufferedPublisher<>(context, 5);
    TestSubscriber subscriber = new TestSubscriber(context) {

      @Override
      public synchronized void onSubscribe(final Subscription sub) {
        super.onSubscribe(sub);
        sub.request(10);
      }
    };
    subscribeOnContext(subscriber);
    AsyncAssert asyncAssert = new AsyncAssert();
    for (int i = 0; i < 10; i++) {
      String record = "record" + i;
      execOnContextAndWait(() -> {
        boolean bufferFull = publisher.accept(record);
        asyncAssert.assertAsync(bufferFull, equalTo(false));
      });
    }
    asyncAssert.throwAssert();
  }

  @Test
  public void shouldCallDrainHandlerWhenBufferCleared() throws Exception {
    publisher = new BufferedPublisher<>(context, 5);

    AsyncAssert asyncAssert = new AsyncAssert();
    for (int i = 0; i < 10; i++) {
      String record = "record" + i;
      final int index = i;
      execOnContextAndWait(() -> {
        boolean bufferFull = publisher.accept(record);
        asyncAssert.assertAsync(bufferFull, equalTo(index >= 5));
      });

    }
    AtomicBoolean drainHandlerCalled = new AtomicBoolean();
    execOnContextAndWait(() -> publisher.drainHandler(() -> drainHandlerCalled.set(true)));
    AsyncAssert drainLatchCalledAssert = new AsyncAssert();
    TestSubscriber subscriber = new TestSubscriber(context) {

      @Override
      public synchronized void onSubscribe(final Subscription sub) {
        super.onSubscribe(sub);
        sub.request(1);
      }

      @Override
      public synchronized void onNext(final String value) {
        super.onNext(value);
        getSub().request(1);
        drainLatchCalledAssert
            .assertAsync(drainHandlerCalled.get(), equalTo(false));
      }
    };
    subscribeOnContext(subscriber);
    assertThatEventually(drainHandlerCalled::get, equalTo(true));
  }

  private void loadPublisher(int num) throws Exception {
    execOnContextAndWait(() -> {
      for (int i = 0; i < num; i++) {
        publisher.accept("record" + i);
      }
    });
  }

  private void subscribeOnContext(Subscriber<String> subScriber) throws Exception {
    execOnContextAndWait(() -> publisher.subscribe(subScriber));
  }

  private void execOnContextAndWait(Runnable action) throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    context.runOnContext(v -> {
      action.run();
      latch.countDown();
    });
    TestUtils.awaitLatch(latch);
  }

  private void shouldDeliver(int numRequested, int numDelivered) throws Exception {
    TestSubscriber subscriber = new TestSubscriber(context) {
      @Override
      public synchronized void onSubscribe(final Subscription sub) {
        super.onSubscribe(sub);
        sub.request(numRequested);
      }
    };
    subscribeOnContext(subscriber);
    assertThatEventually(subscriber::getValues, hasSize(numDelivered));
    for (int i = 0; i < numDelivered; i++) {
      assertThat(subscriber.getValues().get(i), equalTo("record" + i));
    }
    assertThat(subscriber.isCompleted(), equalTo(false));
    assertThat(subscriber.getError(), is(nullValue()));
  }

  private static class TestSubscriber implements Subscriber<String> {

    private Subscription sub;
    private boolean completed;
    private Throwable error;
    private final List<String> values = new ArrayList<>();
    private final Context subscriberContext;

    public TestSubscriber(final Context subscriberContext) {
      this.subscriberContext = subscriberContext;
    }

    @Override
    public synchronized void onSubscribe(final Subscription sub) {
      checkContext();
      this.sub = sub;
    }

    @Override
    public synchronized void onNext(final String value) {
      checkContext();
      values.add(value);
    }

    @Override
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

    public Throwable getError() {
      return error;
    }

    public List<String> getValues() {
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
