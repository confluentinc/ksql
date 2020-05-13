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

package io.confluent.ksql.api.client.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;

import io.confluent.ksql.api.client.Row;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

@RunWith(MockitoJUnitRunner.class)
public class StreamedQueryResultImplTest {


  @Mock
  private Subscriber<Row> subscriber;
  @Mock
  private Row row;

  private Vertx vertx;
  private Context context;
  private Subscription subscription;

  private StreamedQueryResultImpl queryResult;

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
    context = vertx.getOrCreateContext();

    doAnswer(invocation -> {
      final Object[] args = invocation.getArguments();
      subscription = (Subscription) args[0];
      return null;
    }).when(subscriber).onSubscribe(any());

    queryResult = new StreamedQueryResultImpl(context, "queryId", Collections.emptyList(), Collections.emptyList());
  }

  @Test
  public void shouldNotSubscribeIfPolling() {
    // Given
    queryResult.poll(1, TimeUnit.NANOSECONDS);

    // When
    final Exception e = assertThrows(IllegalStateException.class, () -> queryResult.subscribe(subscriber));

    // Then
    assertThat(e.getMessage(), containsString("Cannot set subscriber if polling"));
  }

  @Test
  public void shouldNotPollIfSubscribed() throws Exception {
    // Given
    subscribe();

    // When
    final Exception e = assertThrows(IllegalStateException.class, () -> queryResult.poll());

    // Then
    assertThat(e.getMessage(), containsString("Cannot poll if subscriber has been set"));
  }

  @Test
  public void shouldNotPollIfFailed() throws Exception {
    // Given
    handleQueryResultError();

    // When
    final Exception e = assertThrows(IllegalStateException.class, () -> queryResult.poll());

    // Then
    assertThat(e.getMessage(), containsString("Cannot poll on StreamedQueryResult that has failed"));
  }

  @Test
  public void shouldReturnFromPollOnError() throws Exception {
    // Given
    // Poll for a minimal amount of time to ensure PollableSubscriber is subscribed
    queryResult.poll(1, TimeUnit.NANOSECONDS);

    CountDownLatch latch = new CountDownLatch(1);
    new Thread(() -> {
      queryResult.poll();
      latch.countDown();
    }).start();

    // When
    handleQueryResultError();

    // Then
    awaitLatch(latch);
  }

  @Test
  public void shouldPropagateErrorToSubscriber() throws Exception {
    // Given
    subscribe();

    // When
    handleQueryResultError();

    // Then
    verify(subscriber).onError(any());
  }

  @Test
  public void shouldDeliverBufferedRowsIfComplete() throws Exception {
    // Given
    givenPublisherAcceptsOneRow();
    completeQueryResult();

    // When
    final Row receivedRow = queryResult.poll();

    // Then
    assertThat(receivedRow, is(row));
  }

  @Test
  public void shouldDeliverBufferedRowsOnError() throws Exception {
    // Given
    givenPublisherAcceptsOneRow();
    subscribe();
    handleQueryResultError();

    // When
    subscription.request(1);

    // Then
    verify(subscriber).onNext(row);
    verify(subscriber).onError(any());
  }

  @Test
  public void shouldNotSubscribeIfFailed() throws Exception {
    // Given
    handleQueryResultError();

    CountDownLatch latch = new CountDownLatch(1);
    context.runOnContext(v -> {
      // When / Then
      final Exception e = assertThrows(IllegalStateException.class, () -> queryResult.subscribe(subscriber));
      assertThat(e.getMessage(), containsString("Cannot subscribe to failed publisher"));
      latch.countDown();
    });
    awaitLatch(latch);
  }

  @Test
  public void shouldAllowSubscribeIfComplete() throws Exception {
    // Given
    givenPublisherAcceptsOneRow();
    completeQueryResult();

    // When
    subscribe();
    subscription.request(1);

    // Then
    final InOrder inOrder = inOrder(subscriber);
    inOrder.verify(subscriber).onNext(row);
    inOrder.verify(subscriber).onComplete();
  }

  private void subscribe() throws Exception {
    execOnContextAndWait(() -> queryResult.subscribe(subscriber));
  }

  private void handleQueryResultError() throws Exception {
    execOnContextAndWait(() -> queryResult.handleError(new RuntimeException("boom")));
  }

  private void completeQueryResult() throws Exception {
    execOnContextAndWait(() -> queryResult.complete());
  }

  private void givenPublisherAcceptsOneRow() throws Exception {
    execOnContextAndWait(() -> queryResult.accept(row));
  }

  private void execOnContextAndWait(Runnable action) throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    context.runOnContext(v -> {
      action.run();
      latch.countDown();
    });
    awaitLatch(latch);
  }

  private static void awaitLatch(CountDownLatch latch) throws Exception {
    assertThat(latch.await(2000, TimeUnit.MILLISECONDS), is(true));
  }
}