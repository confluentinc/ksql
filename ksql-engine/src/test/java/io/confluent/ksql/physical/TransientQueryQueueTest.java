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

package io.confluent.ksql.physical;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.physical.TransientQueryQueue.QueuePopulator;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.structured.QueuedSchemaKStream;
import java.util.Optional;
import java.util.Queue;
import java.util.stream.IntStream;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("ConstantConditions")
@RunWith(MockitoJUnitRunner.class)
public class TransientQueryQueueTest {

  private static final int SOME_LIMIT = 4;
  private static final GenericRow ROW_ONE = mock(GenericRow.class);
  private static final GenericRow ROW_TWO = mock(GenericRow.class);

  @Mock
  private LimitHandler limitHandler;
  @Mock
  private KStream<String, GenericRow> kStreamsApp;
  @Mock
  private OutputNode outputNode;
  @Mock
  private QueuedSchemaKStream<String> queuedKStream;
  @Captor
  private ArgumentCaptor<QueuePopulator<String>> queuePopulatorCaptor;
  private Queue<KeyValue<String, GenericRow>> queue;

  @Before
  public void setUp() {
    when(queuedKStream.outputNode()).thenReturn(outputNode);
    when(queuedKStream.getKstream()).thenReturn(kStreamsApp);
  }

  @Test
  public void shouldQueue() {
    // Given:
    final QueuePopulator<String> queuePopulator = getQueuePopulator();

    // When:
    queuePopulator.apply("key1", ROW_ONE);
    queuePopulator.apply("key2", ROW_TWO);

    // Then:
    assertThat(queue, hasSize(2));
    assertThat(queue.peek().key, is("key1"));
    assertThat(queue.remove().value, is(ROW_ONE));
    assertThat(queue.peek().key, is("key2"));
    assertThat(queue.remove().value, is(ROW_TWO));
  }

  @Test
  public void shouldNotQueueNullValues() {
    // Given:
    final QueuePopulator<String> queuePopulator = getQueuePopulator();

    // When:
    queuePopulator.apply("key1", null);

    // Then:
    assertThat(queue, is(empty()));
  }

  @Test
  public void shouldQueueUntilLimitReached() {
    // Given:
    when(outputNode.getLimit()).thenReturn(Optional.of(SOME_LIMIT));
    final QueuePopulator<String> queuePopulator = getQueuePopulator();

    // When:
    IntStream.range(0, SOME_LIMIT + 2)
        .forEach(idx -> queuePopulator.apply("key1", ROW_ONE));

    // Then:
    assertThat(queue, hasSize(SOME_LIMIT));
  }

  @Test
  public void shouldNotCallLimitHandlerIfLimitNotReached() {
    // Given:
    when(outputNode.getLimit()).thenReturn(Optional.of(SOME_LIMIT));
    final QueuePopulator<String> queuePopulator = getQueuePopulator();

    // When:
    IntStream.range(0, SOME_LIMIT - 1)
        .forEach(idx -> queuePopulator.apply("key1", ROW_ONE));

    // Then:
    verify(limitHandler, never()).limitReached();
  }

  @Test
  public void shouldCallLimitHandlerAsLimitReached() {
    // Given:
    when(outputNode.getLimit()).thenReturn(Optional.of(SOME_LIMIT));
    final QueuePopulator<String> queuePopulator = getQueuePopulator();

    // When:
    IntStream.range(0, SOME_LIMIT)
        .forEach(idx -> queuePopulator.apply("key1", ROW_ONE));

    // Then:
    verify(limitHandler).limitReached();
  }

  @Test
  public void shouldCallLimitHandlerOnlyOnce() {
    // Given:
    when(outputNode.getLimit()).thenReturn(Optional.of(SOME_LIMIT));
    final QueuePopulator<String> queuePopulator = getQueuePopulator();

    // When:
    IntStream.range(0, SOME_LIMIT + 1)
        .forEach(idx -> queuePopulator.apply("key1", ROW_ONE));

    // Then:
    verify(limitHandler, times(1)).limitReached();
  }

  private QueuePopulator<String> getQueuePopulator() {
    final TransientQueryQueue<String> queuer = new TransientQueryQueue<>(queuedKStream);
    queue = queuer.getQueue();
    queuer.setLimitHandler(limitHandler);
    verify(kStreamsApp).foreach(queuePopulatorCaptor.capture());
    return queuePopulatorCaptor.getValue();
  }
}