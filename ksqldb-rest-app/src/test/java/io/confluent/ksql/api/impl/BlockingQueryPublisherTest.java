/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.ksql.api.impl;

import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.api.server.QueryHandle;
import io.confluent.ksql.query.BlockingRowQueue;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KeyValueMetadata;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

@RunWith(MockitoJUnitRunner.Silent.class)
public class BlockingQueryPublisherTest {

  private Vertx vertx;
  private Context context;
  @Mock
  private WorkerExecutor workerExecutor;
  @Mock
  private QueryHandle queryHandle;
  @Mock
  private BlockingRowQueue queue;
  @Mock
  private LogicalSchema schema;

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
    context = vertx.getOrCreateContext();
    when(queryHandle.getColumnNames()).thenReturn(ImmutableList.of());
    when(queryHandle.getColumnTypes()).thenReturn(ImmutableList.of());
    when(queryHandle.getLogicalSchema()).thenReturn(schema);
    when(queryHandle.getQueue()).thenReturn(queue);
    when(queryHandle.getQueryId()).thenReturn(new QueryId("q"));
    when(queryHandle.getConsistencyOffsetVector()).thenReturn(Optional.empty());
    // Empty so doSend has nothing to stream (a default mock would report not-empty and spin).
    when(queue.isEmpty()).thenReturn(true);
  }

  @After
  public void tearDown() {
    vertx.close();
  }

  // A pull query can fail asynchronously (e.g. a router-queue rejection) before the subscriber
  // attaches. The failure must still reach the subscriber's onError rather than being dropped.
  @Test
  public void shouldReplayPreSubscriptionErrorToPullQuerySubscriber() throws Exception {
    final BlockingQueryPublisher publisher = new BlockingQueryPublisher(context, workerExecutor);
    final Consumer<Throwable> onException = captureOnException(publisher, true);

    final RuntimeException boom = new RuntimeException("boom");
    // Failure arrives before any subscriber has attached.
    onContext(() -> onException.accept(boom));

    final AtomicReference<Throwable> received = new AtomicReference<>();
    onContext(() -> publisher.subscribe(recordingSubscriber(received)));

    assertThatEventually(received::get, is(sameInstance(boom)));
  }

  // The normal path (subscriber attaches first, then the query fails) must still deliver.
  @Test
  public void shouldDeliverErrorAfterSubscribeForPullQuery() throws Exception {
    final BlockingQueryPublisher publisher = new BlockingQueryPublisher(context, workerExecutor);
    final Consumer<Throwable> onException = captureOnException(publisher, true);

    final AtomicReference<Throwable> received = new AtomicReference<>();
    onContext(() -> publisher.subscribe(recordingSubscriber(received)));

    final RuntimeException boom = new RuntimeException("boom");
    onContext(() -> onException.accept(boom));

    assertThatEventually(received::get, is(sameInstance(boom)));
  }

  private Consumer<Throwable> captureOnException(
      final BlockingQueryPublisher publisher, final boolean isPullQuery) throws Exception {
    @SuppressWarnings("unchecked") final ArgumentCaptor<Consumer<Throwable>> captor =
        ArgumentCaptor.forClass(Consumer.class);
    onContext(() -> publisher.setQueryHandle(queryHandle, isPullQuery, !isPullQuery));
    verify(queryHandle).onException(captor.capture());
    return captor.getValue();
  }

  private Subscriber<KeyValueMetadata<List<?>, GenericRow>> recordingSubscriber(
      final AtomicReference<Throwable> received) {
    return new Subscriber<KeyValueMetadata<List<?>, GenericRow>>() {
      @Override
      public void onSubscribe(final Subscription s) {
        s.request(Long.MAX_VALUE);
      }

      @Override
      public void onNext(final KeyValueMetadata<List<?>, GenericRow> item) {
      }

      @Override
      public void onError(final Throwable t) {
        received.set(t);
      }

      @Override
      public void onComplete() {
      }
    };
  }

  private void onContext(final Runnable runnable) throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);
    context.runOnContext(v -> {
      try {
        runnable.run();
      } finally {
        latch.countDown();
      }
    });
    assertThat(latch.await(10, TimeUnit.SECONDS), is(true));
  }
}
