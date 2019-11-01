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

package io.confluent.ksql.rest.server;

import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.server.computation.Command;
import io.confluent.ksql.rest.server.computation.CommandRunner;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TransactionalProducerTest {

  private static final String COMMAND_TOPIC_NAME = "foo";
  private static final long COMMAND_TOPIC_OFFSET = 3L;
  private static final Duration TIMEOUT = Duration.ofMillis(1234);
  
  @Mock
  private Consumer<CommandId, Command> commandConsumer;
  @Mock
  private Producer<CommandId, Command> commandProducer;
  @Mock
  private CommandRunner commandRunner;

  private TransactionalProducer transactionalProducer;

  @Mock
  private Future<RecordMetadata> future;

  @Mock
  private CommandId commandId1;
  @Mock
  private Command command1;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final static TopicPartition TOPIC_PARTITION = new TopicPartition(COMMAND_TOPIC_NAME, 0);

  @Before
  @SuppressWarnings("unchecked")
  public void setup() throws TimeoutException, InterruptedException {
    transactionalProducer = new TransactionalProducerImpl(
        COMMAND_TOPIC_NAME,
        commandRunner,
        TIMEOUT,
        commandConsumer,
        commandProducer
    );
    when(commandProducer.send(any(ProducerRecord.class))).thenReturn(future);
    when(commandConsumer.endOffsets(any()))
        .thenReturn(Collections.singletonMap(TOPIC_PARTITION, COMMAND_TOPIC_OFFSET));
  }

  @Test
  public void shouldAssignCorrectPartitionToConsumerAndInitTransaction() {
    verify(commandConsumer)
        .assign(eq(Collections.singleton(new TopicPartition(COMMAND_TOPIC_NAME, 0))));
    verify(commandProducer).initTransactions();
  }

  @Test
  public void shouldBeginTransactionAndWaitForCommandConsumer() throws TimeoutException, InterruptedException {
    transactionalProducer.begin();
    
    verify(commandProducer).beginTransaction();
    verify(commandRunner).ensureProcessedPastOffset(COMMAND_TOPIC_OFFSET - 1,  TIMEOUT);
  }

  @Test
  public void shouldCloseAllResources() {
    // When:
    transactionalProducer.close();

    //Then:
    verify(commandProducer).close();
    verify(commandConsumer).close();
  }

  @Test
  public void shouldSendCommandCorrectly() throws Exception {
    // When
    transactionalProducer.send(commandId1, command1);

    // Then
    verify(commandProducer).send(new ProducerRecord<>(COMMAND_TOPIC_NAME, 0, commandId1, command1));
    verify(future).get();
  }

  @Test
  public void shouldThrowExceptionIfSendIsNotSuccessful() throws Exception {
    // Given:
    when(future.get())
        .thenThrow(new ExecutionException(new RuntimeException("Send was unsuccessful!")));
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("Send was unsuccessful!");

    // When
    transactionalProducer.send(commandId1, command1);
  }

  @Test
  public void shouldThrowRuntimeExceptionIfSendCausesNonRuntimeException() throws Exception {
    // Given:
    when(future.get()).thenThrow(new ExecutionException(
            new Exception("Send was unsuccessful because of non RunTime exception!")));
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage(
            "java.lang.Exception: Send was unsuccessful because of non RunTime exception!");

    // When
    transactionalProducer.send(commandId1, command1);
  }

  @Test
  public void shouldThrowRuntimeExceptionIfSendThrowsInterruptedException() throws Exception {
    // Given:
    when(future.get()).thenThrow(new InterruptedException("InterruptedException"));
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("InterruptedException");

    // When
    transactionalProducer.send(commandId1, command1);
  }

  @Test
  public void shouldCommitTransaction() {
    // When:
    transactionalProducer.commit();

    //Then:
    verify(commandProducer).commitTransaction();
  }
}
