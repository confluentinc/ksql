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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.fail;

import io.confluent.ksql.test.util.AsyncAssert;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

/**
 * More BufferedPublisher testing occurs in the TCK tests
 */
public class BufferedPublisherTest extends PublisherTestBase<String> {

  @Override
  protected Publisher<String> createPublisher() {
    return new BufferedPublisher<>(context);
  }

  @Override
  protected String expectedValue(final int i) {
    return "record" + i;
  }

  private BufferedPublisher<String> getBufferedPublisher() {
    return (BufferedPublisher<String>) publisher;
  }

  @Test
  public void shouldNotAllowSettingDrainHandlerMoreThanOnce() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    context.runOnContext(v -> {
      getBufferedPublisher().drainHandler(() -> {
      });
      try {
        getBufferedPublisher().drainHandler(() -> {
        });
        fail("Should throw exception");
      } catch (IllegalStateException e) {
        // OK
        latch.countDown();
      }
    });
    awaitLatch(latch);
  }

  @Test
  public void shouldCompleteWhenNoRecords() throws Exception {
    TestSubscriber<String> subscriber = new TestSubscriber<>(context);
    subscribeOnContext(subscriber);
    execOnContextAndWait(getBufferedPublisher()::complete);
    assertThatEventually(subscriber::isCompleted, equalTo(true));
  }

  @Test
  public void shouldCompleteAfterDeliveringRecords() throws Exception {
    loadPublisher(10);
    AsyncAssert asyncAssert = new AsyncAssert();
    TestSubscriber<String> subscriber = new TestSubscriber<String>(context) {
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
    execOnContextAndWait(getBufferedPublisher()::complete);
    assertThatEventually(subscriber::isCompleted, equalTo(true));
    assertThat(subscriber.getValues(), hasSize(10));
    for (int i = 0; i < 10; i++) {
      assertThat(subscriber.getValues().get(i), equalTo(expectedValue(i)));
    }
    asyncAssert.throwAssert();
  }

  @Test
  public void shouldCompleteAfterDeliveringRecordsNoBuffering() throws Exception {
    AsyncAssert asyncAssertOnNext = new AsyncAssert();
    TestSubscriber<String> subscriber = new TestSubscriber<String>(context) {
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
      String record = expectedValue(i);
      execOnContextAndWait(() -> {
        boolean bufferFull = getBufferedPublisher().accept(record);
        assertNotBufferFull.assertAsync(bufferFull, equalTo(false));
      });
      assertThatEventually(subscriber::getValues, hasSize(i + 1));
      assertThat(subscriber.getValues().get(i), equalTo(record));
    }
    asyncAssertOnNext.throwAssert();
    assertNotBufferFull.throwAssert();

    execOnContextAndWait(getBufferedPublisher()::complete);
    assertThatEventually(subscriber::isCompleted, equalTo(true));
  }

  @Test
  public void shouldNotAllowAcceptingAfterComplete() throws Exception {
    TestSubscriber<String> subscriber = new TestSubscriber<>(context);
    subscribeOnContext(subscriber);
    execOnContextAndWait(getBufferedPublisher()::complete);
    AtomicBoolean failed = new AtomicBoolean();
    execOnContextAndWait(() -> {
      try {
        getBufferedPublisher().accept("foo");
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
      String record = expectedValue(i);
      final int index = i;
      execOnContextAndWait(() -> {
        boolean bufferFull = getBufferedPublisher().accept(record);
        asyncAssert.assertAsync(bufferFull, equalTo(index >= 5));
      });
    }
  }

  @Test
  public void shouldAcceptNotBuffered() throws Exception {
    publisher = new BufferedPublisher<>(context, 5);
    TestSubscriber<String> subscriber = new TestSubscriber<String>(context) {

      @Override
      public synchronized void onSubscribe(final Subscription sub) {
        super.onSubscribe(sub);
        sub.request(10);
      }
    };
    subscribeOnContext(subscriber);
    AsyncAssert asyncAssert = new AsyncAssert();
    for (int i = 0; i < 10; i++) {
      String record = expectedValue(i);
      execOnContextAndWait(() -> {
        boolean bufferFull = getBufferedPublisher().accept(record);
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
      String record = expectedValue(i);
      final int index = i;
      execOnContextAndWait(() -> {
        boolean bufferFull = getBufferedPublisher().accept(record);
        asyncAssert.assertAsync(bufferFull, equalTo(index >= 5));
      });

    }
    AtomicBoolean drainHandlerCalled = new AtomicBoolean();
    execOnContextAndWait(
        () -> getBufferedPublisher().drainHandler(() -> drainHandlerCalled.set(true)));
    AsyncAssert drainLatchCalledAssert = new AsyncAssert();
    TestSubscriber<String> subscriber = new TestSubscriber<String>(context) {

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

  @Test
  public void shouldDeliverMoreThanMaxSendBatchSize() throws Exception {
    int num = 2 * BufferedPublisher.SEND_MAX_BATCH_SIZE;
    loadPublisher(num);
    shouldDeliver(num, num);
  }

  @Test
  public void shouldNotAllowSubscribeAfterError() throws Exception {
    // Given
    execOnContextAndWait(() -> getBufferedPublisher().sendError(new RuntimeException("boom")));

    AtomicBoolean failed = new AtomicBoolean();
    execOnContextAndWait(() -> {
      try {
        TestSubscriber<String> subscriber = new TestSubscriber<>(context);
        // When
        getBufferedPublisher().subscribe(subscriber);
        failed.set(true);
      } catch (IllegalStateException e) {
        // Then
        if (!e.getMessage().contains("Cannot subscribe to failed publisher")
            || !e.getMessage().contains("boom")) {
          failed.set(true);
        }
      }
    });
    assertThat(failed.get(), equalTo(false));
  }

  @Test
  public void shouldSendBufferedRowsAfterComplete() throws Exception {
    // Given
    TestSubscriber<String> subscriber = new TestSubscriber<String>(context) {
      @Override
      public synchronized void onSubscribe(final Subscription sub) {
        super.onSubscribe(sub);
      }
    };
    subscribeOnContext(subscriber);
    loadPublisher(2);
    execOnContextAndWait(getBufferedPublisher()::complete);

    // When
    subscriber.getSub().request(1);

    // Then
    assertThatEventually(subscriber::getValues, hasSize(1));
    assertThat(subscriber.getValues().get(0), equalTo(expectedValue(0)));
    assertThat(subscriber.getError(), is(nullValue()));
    assertThat(subscriber.isCompleted(), equalTo(false));

    // When
    subscriber.getSub().request(1);

    // Then
    assertThatEventually(subscriber::getValues, hasSize(2));
    assertThat(subscriber.getValues().get(1), equalTo(expectedValue(1)));
    assertThat(subscriber.getError(), is(nullValue()));
    assertThatEventually(subscriber::isCompleted, equalTo(true));
  }

  @Test
  public void shouldAllowSubscribeAfterComplete() throws Exception {
    // Given
    loadPublisher(2);
    execOnContextAndWait(getBufferedPublisher()::complete);

    // When
    TestSubscriber<String> subscriber = new TestSubscriber<String>(context) {
      @Override
      public synchronized void onSubscribe(final Subscription sub) {
        super.onSubscribe(sub);
        sub.request(2);
      }
    };
    subscribeOnContext(subscriber);

    // Then
    assertThatEventually(subscriber::getValues, hasSize(2));
    for (int i = 0; i < 2; i++) {
      assertThat(subscriber.getValues().get(i), equalTo(expectedValue(i)));
    }
    assertThatEventually(subscriber::isCompleted, equalTo(true));
    assertThat(subscriber.getError(), is(nullValue()));
  }

  @Test
  public void shouldSendBufferedRowsAfterError() throws Exception {
    // Given
    TestSubscriber<String> subscriber = new TestSubscriber<String>(context) {
      @Override
      public synchronized void onSubscribe(final Subscription sub) {
        super.onSubscribe(sub);
      }
    };
    subscribeOnContext(subscriber);
    loadPublisher(1);
    execOnContextAndWait(() -> getBufferedPublisher().sendError(new RuntimeException("boom")));

    // When
    subscriber.getSub().request(1);

    // Then
    assertThatEventually(subscriber::getValues, hasSize(1));
    assertThat(subscriber.getValues().get(0), equalTo(expectedValue(0)));
    assertThatEventually(subscriber::getError, instanceOf(RuntimeException.class));
    assertThat(subscriber.getError().getMessage(), containsString("boom"));
    assertThat(subscriber.isCompleted(), equalTo(false));
  }

  @Override
  protected void loadPublisher(int num) throws Exception {
    execOnContextAndWait(() -> {
      for (int i = 0; i < num; i++) {
        getBufferedPublisher().accept(expectedValue(i));
      }
    });
  }

}
