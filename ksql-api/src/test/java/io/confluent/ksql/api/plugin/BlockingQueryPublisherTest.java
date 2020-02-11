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

package io.confluent.ksql.api.plugin;

import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.api.PublisherTestBase;
import io.confluent.ksql.api.server.PushQueryHandler;
import io.confluent.ksql.api.utils.AsyncAssert;
import io.vertx.core.WorkerExecutor;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

/**
 * More BlockingQueryPublisher testing occurs in the TCK tests
 */
public class BlockingQueryPublisherTest extends PublisherTestBase<GenericRow> {

  private WorkerExecutor workerExecutor;
  private TestQueryHandle queryHandle;

  @Override
  protected Publisher<GenericRow> createPublisher() {
    this.workerExecutor = vertx.createSharedWorkerExecutor("test_workers");
    BlockingQueryPublisher publisher = new BlockingQueryPublisher(context, workerExecutor);
    queryHandle = new TestQueryHandle(OptionalInt.empty());
    publisher.setQueryHandle(queryHandle);
    return publisher;
  }

  @Override
  protected GenericRow expectedValue(final int i) {
    return generateRow(i);
  }

  private BlockingQueryPublisher getBlockingQueryPublisher() {
    return (BlockingQueryPublisher) publisher;
  }

  @After
  public void tearDown() {
    super.tearDown();
    if (workerExecutor != null) {
      workerExecutor.close();
    }
  }

  @Override
  protected void loadPublisher(final int num) throws Exception {
    for (int i = 0; i < num; i++) {
      getBlockingQueryPublisher().accept(generateRow(i));
    }
  }

  private GenericRow generateRow(long num) {
    List<Object> l = new ArrayList<>();
    l.add("foo" + num);
    l.add(num);
    l.add(num % 2 == 0);
    return GenericRow.fromList(l);
  }

  @Test
  public void shouldStopQueryHandleOnClose() throws Exception {
    // When
    getBlockingQueryPublisher().close();

    // Then
    assertThatEventually(queryHandle::getStopCalledTimes, is(1));
  }

  @Test
  public void shouldNotStopQueryHandleOnCloseMoreThanOnce() throws Exception {
    // Given:
    getBlockingQueryPublisher().close();
    assertThatEventually(queryHandle::getStopCalledTimes, is(1));

    // When:
    getBlockingQueryPublisher().close();
    Thread.sleep(100);

    // Then:
    assertThat(queryHandle.getStopCalledTimes(), is(1));
  }

  @Test
  public void shouldCompleteWhenLimitReached() throws Exception {
    queryHandle = new TestQueryHandle(OptionalInt.of(10));
    getBlockingQueryPublisher().setQueryHandle(queryHandle);

    loadPublisher(20);
    AsyncAssert asyncAssert = new AsyncAssert();
    TestSubscriber<GenericRow> subscriber = new TestSubscriber<GenericRow>(context) {
      @Override
      public synchronized void onSubscribe(final Subscription sub) {
        super.onSubscribe(sub);
        sub.request(1);
      }

      @Override
      public synchronized void onNext(final GenericRow value) {
        super.onNext(value);
        asyncAssert.assertAsync(isCompleted(), equalTo(false));
        getSub().request(1);
      }
    };
    subscribeOnContext(subscriber);
    assertThatEventually(subscriber::isCompleted, equalTo(true));
    assertThat(subscriber.getValues(), hasSize(10));
    for (int i = 0; i < 10; i++) {
      assertThat(subscriber.getValues().get(i), equalTo(expectedValue(i)));
    }
    asyncAssert.throwAssert();
  }

  @Test
  public void shouldNotAcceptAfterClose() throws Exception {

    // Given:
    getBlockingQueryPublisher().close();

    // When:
    AtomicBoolean onNextCalled = new AtomicBoolean();
    TestSubscriber<GenericRow> subscriber = new TestSubscriber<GenericRow>(context) {
      @Override
      public synchronized void onSubscribe(final Subscription sub) {
        super.onSubscribe(sub);
        sub.request(1);
      }

      @Override
      public synchronized void onNext(final GenericRow value) {
        super.onNext(value);
        onNextCalled.set(true);
      }
    };
    subscribeOnContext(subscriber);
    loadPublisher(1);

    // Then:
    Thread.sleep(100);
    assertThat(onNextCalled.get(), is(false));
  }

  @Test
  public void shouldBlockIfQueueFull() throws Exception {
    // Given:
    AtomicReference<Exception> exception = new AtomicReference<>();
    Thread t = new Thread(() -> {
      try {
        loadPublisher(BlockingQueryPublisher.BLOCKING_QUEUE_CAPACITY + 1);
      } catch (Exception e) {
        exception.set(e);
      }
    });

    // When:
    t.start();
    assertThatEventually(() -> getBlockingQueryPublisher().queueSize(),
        is(BlockingQueryPublisher.BLOCKING_QUEUE_CAPACITY));

    // Then:
    assertThat(t.isAlive(), is(true));
    assertThat(exception.get(), is(nullValue()));

    t.interrupt();
  }

  @Test
  public void shouldReleaseBlockedThreadOnClose() {
    // Given:
    AtomicReference<Exception> exception = new AtomicReference<>();
    Thread t = new Thread(() -> {
      try {
        loadPublisher(BlockingQueryPublisher.BLOCKING_QUEUE_CAPACITY + 1);
      } catch (Exception e) {
        exception.set(e);
      }
    });
    t.start();
    assertThatEventually(() -> getBlockingQueryPublisher().queueSize(),
        is(BlockingQueryPublisher.BLOCKING_QUEUE_CAPACITY));
    assertThat(t.isAlive(), is(true));

    // When:
    getBlockingQueryPublisher().close();

    // Then:
    assertThatEventually(t::isAlive, is(false));
    assertThat(exception.get(), is(nullValue()));
  }

  @Test
  public void shouldDeliverMoreThanMaxSendBatchSize() throws Exception {
    int num = 2 * BlockingQueryPublisher.SEND_MAX_BATCH_SIZE;
    loadPublisher(num);
    shouldDeliver(num, num);
  }

  private static class TestQueryHandle implements PushQueryHandler {

    private final OptionalInt limit;
    private int stopCalledTimes;

    public TestQueryHandle(final OptionalInt limit) {
      this.limit = limit;
    }

    @Override
    public List<String> getColumnNames() {
      return new ArrayList<>();
    }

    @Override
    public List<String> getColumnTypes() {
      return new ArrayList<>();
    }

    @Override
    public OptionalInt getLimit() {
      return limit;
    }

    @Override
    public void start() {
    }

    @Override
    public synchronized void stop() {
      stopCalledTimes++;
    }

    synchronized int getStopCalledTimes() {
      return stopCalledTimes;
    }
  }

}
