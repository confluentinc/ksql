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

package io.confluent.ksql.rest.server.computation;

import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.CommandId;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.function.Function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TransactionManagerTest {
  private static final String TRANSACTION_ID = "transactionId";

  @Mock
  private KafkaProducer<CommandId, Command> transactionalProducer;
  @Mock
  private Serializer<CommandId> keySerializer;
  @Mock
  private Serializer<Command> valueSerializer;
  @Mock
  private RecordMetadata recordMetadata;
  @Mock
  private Function<Producer<CommandId, Command>, RecordMetadata> transactionBlock;

  private TransactionManager<CommandId, Command> transactionManager;

  @Before
  public void setup() {
    when(transactionBlock.apply(any())).thenReturn(recordMetadata);

    transactionManager = new TransactionManager<>(
        TRANSACTION_ID,
        Collections.emptyMap(),
        keySerializer,
        valueSerializer,
        mock(Errors.class),
        (a, b, c) -> transactionalProducer
    );
  }

  @Test
  public void shouldExecuteTransaction() {
    // When:
    final Object result = transactionManager.executeTransaction(transactionBlock);

    // Then:
    assertThat(result, is(recordMetadata));

    final InOrder inOrder = Mockito.inOrder(transactionalProducer, transactionBlock);
    inOrder.verify(transactionalProducer).initTransactions();
    inOrder.verify(transactionalProducer).beginTransaction();
    inOrder.verify(transactionBlock).apply(transactionalProducer);
    inOrder.verify(transactionalProducer).commitTransaction();
  }

  @Test
  public void shouldThrowExecuteTransactionWhenKafkaProducerCannotBeCreated() {
    // Given:
    transactionManager = new TransactionManager<>(
        TRANSACTION_ID,
        Collections.emptyMap(),
        keySerializer,
        valueSerializer,
        mock(Errors.class),
        (a, b, c) -> { throw new RuntimeException("kafka-error"); }
    );

    // When:
    final Exception e = assertThrows(
        RuntimeException.class,
        () -> transactionManager.executeTransaction(transactionBlock)
    );

    // Then:
    assertThat(e.getMessage(), containsString("kafka-error"));
    verifyZeroInteractions(transactionalProducer);
    verifyZeroInteractions(transactionBlock);
  }

  @Test
  public void shouldThrowExecuteTransactionWhenInitTransactionFails() {
    // Given:
    doThrow(new RuntimeException("init-error"))
        .when(transactionalProducer).initTransactions();

    // When:
    final Exception e = assertThrows(
        RuntimeException.class,
        () -> transactionManager.executeTransaction(transactionBlock)
    );

    // Then:
    assertThat(e.getMessage(), containsString("init-error"));
    verifyZeroInteractions(transactionBlock);
    verify(transactionalProducer).initTransactions();
    verifyNoMoreInteractions(transactionalProducer);
  }

  @Test
  public void shouldThrowExecuteTransactionWhenBeginTransactionFails() {
    // Given:
    doThrow(new RuntimeException("begin-error"))
        .when(transactionalProducer).beginTransaction();

    // When:
    final Exception e = assertThrows(
        RuntimeException.class,
        () -> transactionManager.executeTransaction(transactionBlock)
    );

    // Then:
    assertThat(e.getMessage(), containsString("begin-error"));
    verifyZeroInteractions(transactionBlock);
    verify(transactionalProducer).initTransactions();
    verify(transactionalProducer).beginTransaction();
    verify(transactionalProducer).close();
    verifyNoMoreInteractions(transactionalProducer);
  }

  @Test
  public void shouldThrowAndAbortExecuteTransactionWhenTransactionBlockFails() {
    // Given:
    doThrow(new RuntimeException("block-error"))
        .when(transactionBlock).apply(transactionalProducer);

    // When:
    final Exception e = assertThrows(
        RuntimeException.class,
        () -> transactionManager.executeTransaction(transactionBlock)
    );

    // Then:
    assertThat(e.getMessage(), containsString("block-error"));
    verify(transactionalProducer).initTransactions();
    verify(transactionalProducer).beginTransaction();
    verify(transactionBlock).apply(transactionalProducer);
    verify(transactionalProducer).abortTransaction();
    verify(transactionalProducer).close();
    verifyNoMoreInteractions(transactionalProducer);
  }

  @Test
  public void shouldThrowAndAbortExecuteTransactionWhenCommitTransactionFails() {
    // Given:
    doThrow(new RuntimeException("commit-error"))
        .when(transactionalProducer).commitTransaction();

    // When:
    final Exception e = assertThrows(
        RuntimeException.class,
        () -> transactionManager.executeTransaction(transactionBlock)
    );

    // Then:
    assertThat(e.getMessage(), containsString("commit-error"));
    verify(transactionalProducer).initTransactions();
    verify(transactionalProducer).beginTransaction();
    verify(transactionBlock).apply(transactionalProducer);
    verify(transactionalProducer).commitTransaction();
    verify(transactionalProducer).abortTransaction();
    verify(transactionalProducer).close();
    verifyNoMoreInteractions(transactionalProducer);
  }

  @Test
  public void shouldThrowAndNotAbortExecuteTransactionOnNonAbortedExceptions() {
    // Given:
    doThrow(ProducerFencedException.class)
        .when(transactionalProducer).commitTransaction();

    // When:
    final Exception e = assertThrows(
        ProducerFencedException.class,
        () -> transactionManager.executeTransaction(transactionBlock)
    );

    // Then:
    assertThat(e, instanceOf(ProducerFencedException.class));
    verify(transactionalProducer).initTransactions();
    verify(transactionalProducer).beginTransaction();
    verify(transactionBlock).apply(transactionalProducer);
    verify(transactionalProducer).commitTransaction();
    verify(transactionalProducer).close();
    verifyNoMoreInteractions(transactionalProducer);
  }
}
