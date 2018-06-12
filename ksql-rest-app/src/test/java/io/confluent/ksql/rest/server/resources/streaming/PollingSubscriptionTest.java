/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.rest.server.resources.streaming;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.confluent.ksql.rest.server.resources.streaming.Flow.Subscriber;
import io.confluent.ksql.rest.server.resources.streaming.Flow.Subscription;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class PollingSubscriptionTest {

  private static final ImmutableList<String> ELEMENTS = ImmutableList.of("a", "b", "c", "d", "e", "f");
  private final ScheduledExecutorService multithreadedExec = Executors.newScheduledThreadPool(8);
  final ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();

  private static class TestSubscriber implements Subscriber<String> {

    CountDownLatch done = new CountDownLatch(1);
    Throwable error = null;
    List<String> elements = Lists.newLinkedList();
    Schema schema = null;
    Subscription subscription;

    @Override
    public void onNext(String item) {
      if (done.getCount() == 0) {
        throw new IllegalStateException("already done");
      }
      elements.add(item);
      subscription.request(1);
    }

    @Override
    public void onError(Throwable e) {
      if (done.getCount() == 0) {
        throw new IllegalStateException("already done");
      }
      error = e;
      done.countDown();
    }

    @Override
    public void onComplete() {
      if (done.getCount() == 0) {
        throw new IllegalStateException("already done");
      }
      done.countDown();
    }

    @Override
    public void onSchema(Schema s) {
      if (done.getCount() == 0) {
        throw new IllegalStateException("already done");
      }
      schema = s;
    }

    @Override
    public void onSubscribe(Subscription subscription) {
      this.subscription = subscription;
      subscription.request(1);
    }
  }


  class TestPublisher implements Flow.Publisher<String> {
    TestPollingSubscription subscription;

    @Override
    public void subscribe(Subscriber<String> subscriber) {
      subscription = createSubscription(subscriber);
      subscriber.onSubscribe(subscription);
    }

    TestPollingSubscription createSubscription(Subscriber<String> subscriber) {
      return new TestPollingSubscription(subscriber, exec);
    }
  }

  static class TestPollingSubscription extends PollingSubscription<String> {
    boolean closed;
    Queue<String> queue = Lists.newLinkedList(ELEMENTS);

    public TestPollingSubscription(Subscriber<String> subscriber, ScheduledExecutorService exec) {
      super(
          MoreExecutors.listeningDecorator(exec),
          subscriber,
          SchemaBuilder.OPTIONAL_STRING_SCHEMA
      );
    }

    @Override
    String poll() {
      String value = queue.poll();
      if (value != null) {
        return value;
      } else {
        setDone();
        return null;
      }
    }

    @Override
    synchronized void close() {
      if (closed) {
        fail("closed called more than once");
      }
      closed = true;
    }
  }

  @Test
  public void testBasicFlow() throws Exception {
    TestSubscriber testSubscriber = new TestSubscriber();
    TestPublisher testPublisher = new TestPublisher();
    testPublisher.subscribe(testSubscriber);

    assertTrue(testSubscriber.done.await(1000, TimeUnit.MILLISECONDS));
    assertTrue(exec.shutdownNow().isEmpty());

    assertTrue(testPublisher.subscription.closed);
    assertNull(testSubscriber.error);
    assertNotNull(testSubscriber.schema);
    assertEquals(ELEMENTS, testSubscriber.elements);
  }

  @Test
  public void testErrorDrainsNextElement() throws Exception {
    TestSubscriber testSubscriber = new TestSubscriber();
    TestPublisher testPublisher = new TestPublisher() {
      @Override
      TestPollingSubscription createSubscription(
          Subscriber<String> subscriber
      ) {
        return new TestPollingSubscription(subscriber, exec) {
          @Override
          String poll() {
            // return one element, then set error
            String value = super.poll();
            if (value != null) {
              setError(new RuntimeException("something bad"));
              return value;
            }
            return null;
          }
        };
      }
    };

    testPublisher.subscribe(testSubscriber);

    assertTrue(testSubscriber.done.await(1000, TimeUnit.MILLISECONDS));
    assertTrue(exec.shutdownNow().isEmpty());

    assertTrue(testPublisher.subscription.closed);
    assertNotNull(testSubscriber.error);
    assertEquals(ImmutableList.of("a", "b"), testSubscriber.elements);
  }

  @Test
  public void testMultithreaded() throws Exception {
    TestSubscriber testSubscriber = new TestSubscriber();
    TestPublisher testPublisher = new TestPublisher() {
      @Override
      TestPollingSubscription createSubscription(
          Subscriber<String> subscriber
      ) {
        return new TestPollingSubscription(subscriber, multithreadedExec);
      }
    };
    testPublisher.subscribe(testSubscriber);

    assertTrue(testSubscriber.done.await(1000, TimeUnit.MILLISECONDS));
    assertTrue(multithreadedExec.shutdownNow().isEmpty());

    assertTrue(testPublisher.subscription.closed);
    assertNull(testSubscriber.error);
    assertNotNull(testSubscriber.schema);
    assertEquals(ELEMENTS, testSubscriber.elements);
  }

  @Test
  public void testReentrantNextElement() throws Exception {
    TestSubscriber testSubscriber = new TestSubscriber();
    TestPublisher testPublisher = new TestPublisher() {
      @Override
      TestPollingSubscription createSubscription(
          Subscriber<String> subscriber
      ) {
        return new TestPollingSubscription(subscriber, multithreadedExec) {
          String nextValue;

          @Override
          String poll() {
            // set Error, then return last element
            String value = super.poll();
            if (nextValue == null) {
              nextValue = value;
              setError(new RuntimeException("something bad"));
              return null;
            }
            return nextValue;
          }
        };
      }
    };

    testPublisher.subscribe(testSubscriber);

    assertTrue(testSubscriber.done.await(1000, TimeUnit.MILLISECONDS));
    assertTrue(multithreadedExec.shutdownNow().isEmpty());

    assertTrue(testPublisher.subscription.closed);
    assertNotNull(testSubscriber.error);
    assertEquals(ImmutableList.of("a"), testSubscriber.elements);
  }

  @Test
  public void testEmpty() throws Exception {
    TestSubscriber testSubscriber = new TestSubscriber();
    TestPublisher testPublisher = new TestPublisher() {
      @Override
      TestPollingSubscription createSubscription(
          Subscriber<String> subscriber
      ) {
        return new TestPollingSubscription(subscriber, exec) {
          @Override
          String poll() {
            setDone();
            return null;
          }
        };
      }
    };

    testPublisher.subscribe(testSubscriber);

    assertTrue(testSubscriber.done.await(1000, TimeUnit.MILLISECONDS));
    assertTrue(exec.shutdownNow().isEmpty());

    assertTrue(testPublisher.subscription.closed);
    assertNull(testSubscriber.error);
    assertEquals(ImmutableList.of(), testSubscriber.elements);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testExpectsNEqualsOne() {
    TestSubscriber testSubscriber = new TestSubscriber() {
      @Override
      public void onSubscribe(Subscription subscription) {
        subscription.request(2);
      }
    };
    TestPublisher testPublisher = new TestPublisher();
    testPublisher.subscribe(testSubscriber);
  }
}
