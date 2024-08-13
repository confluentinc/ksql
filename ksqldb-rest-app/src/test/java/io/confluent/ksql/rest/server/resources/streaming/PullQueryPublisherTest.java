/*
 * Copyright 2019 Confluent Inc.
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.api.server.MetricsCallbackHolder;
import io.confluent.ksql.execution.pull.PullQueryResult;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.query.PullQueryWriteStream;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.server.resources.streaming.Flow.Subscription;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KeyValue;
import io.confluent.ksql.util.KeyValueMetadata;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PullQueryPublisherTest {
  private static final long TIME_NANOS = 12345;

  private static final LogicalSchema PULL_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("id"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("bob"), SqlTypes.STRING)
      .build();

  private static final List<?> ROW1 = ImmutableList.of("a", "b");
  private static final List<?> ROW2 = ImmutableList.of("c", "d");

  private static final KeyValueMetadata<List<?>, GenericRow> KV1
      = new KeyValueMetadata<>(new KeyValue<>(null, GenericRow.fromList(ROW1)));
  private static final KeyValueMetadata<List<?>, GenericRow> KV2
      = new KeyValueMetadata<>(new KeyValue<>(null, GenericRow.fromList(ROW2)));

  @Mock
  private WebSocketSubscriber<StreamedRow> subscriber;
  @Mock
  private ListeningScheduledExecutorService exec;
  @Mock
  private PullQueryWriteStream pullQueryQueue;
  @Mock
  private PullQueryResult pullQueryResult;
  @Captor
  private ArgumentCaptor<Subscription> subscriptionCaptor;
  @Captor
  private ArgumentCaptor<Consumer<Void>> completeCaptor;
  @Captor
  private ArgumentCaptor<Consumer<Throwable>> onErrorCaptor;

  private Subscription subscription;
  private PullQueryPublisher publisher;

  @Before
  public void setUp() {
    publisher = new PullQueryPublisher(
        exec,
        pullQueryResult,
        new MetricsCallbackHolder(),
        TIME_NANOS);

    when(pullQueryResult.getSchema()).thenReturn(PULL_SCHEMA);
    when(pullQueryResult.getPullQueryQueue()).thenReturn(pullQueryQueue);
    doNothing().when(pullQueryResult).onException(onErrorCaptor.capture());
    doNothing().when(pullQueryResult).onCompletion(completeCaptor.capture());
    int[] times = new int[1];
    doAnswer(inv -> {
      Collection<? super KeyValueMetadata<List<?>, GenericRow>> c = inv.getArgument(0);
      if (times[0] == 0) {
        c.add(KV1);
      } else if (times[0] == 1) {
        c.add(KV2);
        completeCaptor.getValue().accept(null);
      }
      times[0]++;
      return null;
    }).when(pullQueryQueue).drainTo(any());
    when(exec.submit(any(Runnable.class))).thenAnswer(inv -> {
      Runnable runnable = inv.getArgument(0);
      runnable.run();
      return null;
    });
  }

  @Test
  public void shouldSubscribe() {
    // When:
    publisher.subscribe(subscriber);

    // Then:
    verify(subscriber).onSubscribe(any(), any(), anyLong());
  }

  @Test
  public void shouldOnlyExecuteOnce() {
    // Given:
    givenSubscribed();

    // When:
    subscription.request(1);

    // Then:
    verify(subscriber).onNext(any());
  }

  @Test
  public void shouldCallOnSchemaThenOnNextThenOnCompleteOnSuccess() {
    // Given:
    givenSubscribed();

    // When:
    subscription.request(1);
    subscription.request(1);
    subscription.request(1);

    // Then:
    final InOrder inOrder = inOrder(subscriber);
    inOrder.verify(subscriber).onSchema(any());
    inOrder.verify(subscriber, times(2)).onNext(any());
    inOrder.verify(subscriber).onComplete();
  }

  @Test
  public void shouldPassSchema() {
    // Given:
    givenSubscribed();

    // When:
    subscription.request(1);

    // Then:
    verify(subscriber).onSchema(PULL_SCHEMA);
  }

  @Test
  public void shouldCallOnErrorOnFailure_duringStream() {
    // Given:
    givenSubscribed();
    RuntimeException e = new RuntimeException("Boom!");

    // When:
    onErrorCaptor.getValue().accept(e);
    subscription.request(1);

    // Then:
    verify(subscriber).onError(e);
  }

  @Test
  public void shouldBuildStreamingRows() {
    // Given:
    givenSubscribed();

    // When:
    subscription.request(1);
    subscription.request(1);
    subscription.request(1);

    // Then:
    verify(subscriber, times(2)).onNext(any());
    verify(subscriber).onNext(ImmutableList.of(
        StreamedRow.pushRow(GenericRow.fromList(ROW1))
    ));
    verify(subscriber).onNext(ImmutableList.of(
        StreamedRow.pushRow(GenericRow.fromList(ROW2))
    ));
  }

  private void givenSubscribed() {
    publisher.subscribe(subscriber);
    verify(subscriber).onSubscribe(subscriptionCaptor.capture(), any(), anyLong());
    subscription = subscriptionCaptor.getValue();
  }
}