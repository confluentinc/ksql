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

import static com.google.common.util.concurrent.RateLimiter.create;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.execution.streams.RoutingFilter.RoutingFilterFactory;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.physical.pull.HARouting;
import io.confluent.ksql.physical.pull.PullQueryResult;
import io.confluent.ksql.query.PullQueryQueue;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.util.ConcurrencyLimiter;
import io.confluent.ksql.rest.util.ConcurrencyLimiter.Decrementer;
import io.confluent.ksql.rest.server.resources.streaming.Flow.Subscriber;
import io.confluent.ksql.rest.server.resources.streaming.Flow.Subscription;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KeyValue;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
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

  private static final KeyValue<List<?>, GenericRow> KV1
      = new KeyValue<>(null, GenericRow.fromList(ROW1));
  private static final KeyValue<List<?>, GenericRow> KV2
      = new KeyValue<>(null, GenericRow.fromList(ROW2));

  @Mock
  private KsqlEngine engine;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private ConfiguredStatement<Query> statement;
  @Mock
  private Subscriber<Collection<StreamedRow>> subscriber;
  @Mock
  private ListeningScheduledExecutorService exec;
  @Mock
  private PullQueryQueue pullQueryQueue;
  @Mock
  private PullQueryResult pullQueryResult;
  @Mock
  private RoutingFilterFactory routingFilterFactory;
  @Mock
  private SessionConfig sessionConfig;
  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private HARouting haRouting;
  @Mock
  private ConcurrencyLimiter concurrencyLimiter;
  @Mock
  private Decrementer decrementer;

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
        engine,
        serviceContext,
        exec,
        statement,
        Optional.empty(),
        TIME_NANOS,
        routingFilterFactory,
        create(1),
        concurrencyLimiter,
        haRouting);

    when(statement.getSessionConfig()).thenReturn(sessionConfig);
    when(sessionConfig.getConfig(false)).thenReturn(ksqlConfig);
    when(sessionConfig.getOverrides()).thenReturn(ImmutableMap.of());
    when(pullQueryResult.getSchema()).thenReturn(PULL_SCHEMA);
    when(pullQueryResult.getPullQueryQueue()).thenReturn(pullQueryQueue);
    doNothing().when(pullQueryResult).onException(onErrorCaptor.capture());
    doNothing().when(pullQueryResult).onCompletion(completeCaptor.capture());
    int[] times = new int[1];
    doAnswer(inv -> {
      Collection<? super KeyValue<List<?>, GenericRow>> c = inv.getArgument(0);
      if (times[0] == 0) {
        c.add(KV1);
      } else if (times[0] == 1) {
        c.add(KV2);
        completeCaptor.getValue().accept(null);
      }
      times[0]++;
      return null;
    }).when(pullQueryQueue).drainTo(any());
    when(engine.executePullQuery(any(), any(), any(), any(), any(), any(), anyBoolean()))
        .thenReturn(pullQueryResult);
    when(exec.submit(any(Runnable.class))).thenAnswer(inv -> {
      Runnable runnable = inv.getArgument(0);
      runnable.run();
      return null;
    });
    when(concurrencyLimiter.increment()).thenReturn(decrementer);
  }

  @Test
  public void shouldSubscribe() {
    // When:
    publisher.subscribe(subscriber);

    // Then:
    verify(subscriber).onSubscribe(any());
  }

  @Test
  public void shouldRunQueryWithCorrectParams() {
    // Given:
    givenSubscribed();

    // When:
    subscription.request(1);

    // Then:
    verify(engine).executePullQuery(
        eq(serviceContext), eq(statement), eq(haRouting), any(), any(), eq(Optional.empty()),
        anyBoolean());
  }

  @Test
  public void shouldOnlyExecuteOnce() {
    // Given:
    givenSubscribed();

    // When:
    subscription.request(1);

    // Then:
    verify(subscriber).onNext(any());
    verify(engine).executePullQuery(
        eq(serviceContext), eq(statement), eq(haRouting), any(), any(),
        eq(Optional.empty()), anyBoolean());
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
  public void shouldCallOnErrorOnFailure_initial() {
    // Given:
    when(engine.executePullQuery(any(), any(), any(), any(), any(), any(), anyBoolean()))
        .thenThrow(new RuntimeException("Boom!"));

    // When:
    final Exception e = assertThrows(
        RuntimeException.class,
        () ->  givenSubscribed()
    );

    // Then:
    assertThat(e.getMessage(), containsString("Boom!"));
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
    verify(subscriber).onSubscribe(subscriptionCaptor.capture());
    subscription = subscriptionCaptor.getValue();
  }
}