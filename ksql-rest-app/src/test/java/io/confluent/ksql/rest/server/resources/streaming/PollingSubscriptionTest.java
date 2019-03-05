/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.server.resources.streaming;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import io.confluent.ksql.rest.server.resources.streaming.Flow.Subscriber;
import io.confluent.ksql.rest.server.resources.streaming.Flow.Subscription;
import io.confluent.ksql.rest.server.resources.streaming.StreamingTestUtils.TestSubscriber;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;


public class PollingSubscriptionTest {

  private static final ImmutableList<String> ELEMENTS = ImmutableList.of("a", "b", "c", "d", "e", "f");
  private final ScheduledExecutorService multithreadedExec = Executors.newScheduledThreadPool(8);
  final ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();

  class TestPublisher implements Flow.Publisher<String> {
    TestPollingSubscription subscription;

    @Override
    public void subscribe(final Subscriber<String> subscriber) {
      subscription = createSubscription(subscriber);
      subscriber.onSubscribe(subscription);
    }

    TestPollingSubscription createSubscription(final Subscriber<String> subscriber) {
      return new TestPollingSubscription(subscriber, exec);
    }
  }

  static class TestPollingSubscription extends PollingSubscription<String> {
    boolean closed;
    Queue<String> queue = Lists.newLinkedList(ELEMENTS);

    public TestPollingSubscription(
        final Subscriber<String> subscriber, final ScheduledExecutorService exec) {
      super(
          MoreExecutors.listeningDecorator(exec),
          subscriber,
          SchemaBuilder.OPTIONAL_STRING_SCHEMA
      );
    }

    @Override
    String poll() {
      final String value = queue.poll();
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
    final TestSubscriber<String> testSubscriber = new TestSubscriber<>();
    final TestPublisher testPublisher = new TestPublisher();
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
    final TestSubscriber<String> testSubscriber = new TestSubscriber<>();
    final TestPublisher testPublisher = new TestPublisher() {
      @Override
      TestPollingSubscription createSubscription(
          final Subscriber<String> subscriber
      ) {
        return new TestPollingSubscription(subscriber, exec) {
          @Override
          String poll() {
            // return one element, then set error
            final String value = super.poll();
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
    final TestSubscriber<String> testSubscriber = new TestSubscriber<>();
    final TestPublisher testPublisher = new TestPublisher() {
      @Override
      TestPollingSubscription createSubscription(
          final Subscriber<String> subscriber
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
    final TestSubscriber<String> testSubscriber = new TestSubscriber<>();
    final TestPublisher testPublisher = new TestPublisher() {
      @Override
      TestPollingSubscription createSubscription(
          final Subscriber<String> subscriber
      ) {
        return new TestPollingSubscription(subscriber, multithreadedExec) {
          String nextValue;

          @Override
          String poll() {
            // set Error, then return last element
            final String value = super.poll();
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
    final TestSubscriber<String> testSubscriber = new TestSubscriber<>();
    final TestPublisher testPublisher = new TestPublisher() {
      @Override
      TestPollingSubscription createSubscription(
          final Subscriber<String> subscriber
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
    final TestSubscriber<String> testSubscriber = new TestSubscriber<String>() {
      @Override
      public void onSubscribe(final Subscription subscription) {
        subscription.request(2);
      }
    };
    final TestPublisher testPublisher = new TestPublisher();
    testPublisher.subscribe(testSubscriber);
  }
}
