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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.entity.TableRows;
import io.confluent.ksql.rest.server.execution.PullQueryExecutor;
import io.confluent.ksql.rest.server.execution.PullQueryResult;
import io.confluent.ksql.rest.server.resources.streaming.Flow.Subscriber;
import io.confluent.ksql.rest.server.resources.streaming.Flow.Subscription;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import java.util.Collection;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@RunWith(MockitoJUnitRunner.class)
public class PullQueryPublisherTest {
  private static final long TIME_NANOS = 12345;

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("id"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("bob"), SqlTypes.BIGINT)
      .build();

  @Mock
  private KsqlEngine engine;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private ConfiguredStatement<Query> statement;
  @Mock
  private Subscriber<Collection<StreamedRow>> subscriber;
  @Mock
  private PullQueryExecutor pullQueryExecutor;
  @Mock
  private TableRows entity;
  @Captor
  private ArgumentCaptor<Subscription> subscriptionCaptor;

  private Subscription subscription;
  private PullQueryPublisher publisher;

  @Before
  public void setUp() {
    publisher = new PullQueryPublisher(
        serviceContext,
        statement,
        pullQueryExecutor,
        TIME_NANOS);

    PullQueryResult result = new PullQueryResult(entity, Optional.empty());
    when(pullQueryExecutor.execute(any(), any(), any(), eq(TIME_NANOS))).thenReturn(result);
    when(entity.getSchema()).thenReturn(SCHEMA);

    doAnswer(callRequestAgain()).when(subscriber).onNext(any());
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
    verify(pullQueryExecutor).execute(statement, serviceContext, Optional.of(false), TIME_NANOS);
  }

  @Test
  public void shouldOnlyExecuteOnce() {
    // Given:
    givenSubscribed();

    // When:
    subscription.request(1);

    // Then:
    verify(subscriber).onNext(any());
    verify(pullQueryExecutor).execute(statement, serviceContext, Optional.of(false), TIME_NANOS);
  }

  @Test
  public void shouldCallOnSchemaThenOnNextThenOnCompleteOnSuccess() {
    // Given:
    givenSubscribed();

    // When:
    subscription.request(1);

    // Then:
    final InOrder inOrder = inOrder(subscriber);
    inOrder.verify(subscriber).onSchema(any());
    inOrder.verify(subscriber).onNext(any());
    inOrder.verify(subscriber).onComplete();
  }

  @Test
  public void shouldPassSchema() {
    // Given:
    givenSubscribed();

    // When:
    subscription.request(1);

    // Then:
    verify(subscriber).onSchema(SCHEMA);
  }

  @Test
  public void shouldCallOnErrorOnFailure() {
    // Given:
    givenSubscribed();
    final Throwable e = new RuntimeException("Boom!");
    when(pullQueryExecutor.execute(any(), any(), any(), eq(TIME_NANOS))).thenThrow(e);

    // When:
    subscription.request(1);

    // Then:
    verify(subscriber).onError(e);
  }

  @Test
  public void shouldBuildStreamingRows() {
    // Given:
    givenSubscribed();

    when(entity.getRows()).thenReturn(ImmutableList.of(
        ImmutableList.of("a", 1, 2L, 3.0f),
        ImmutableList.of("b", 1, 2L, 3.0f)
    ));

    // When:
    subscription.request(1);

    // Then:
    verify(subscriber).onNext(ImmutableList.of(
        StreamedRow.row(GenericRow.genericRow("a", 1, 2L, 3.0f)),
        StreamedRow.row(GenericRow.genericRow("b", 1, 2L, 3.0f))
    ));
  }

  private Answer<Void> callRequestAgain() {
    return inv -> {
      subscription.request(1);
      return null;
    };
  }

  private void givenSubscribed() {
    publisher.subscribe(subscriber);
    verify(subscriber).onSubscribe(subscriptionCaptor.capture());
    subscription = subscriptionCaptor.getValue();
  }
}