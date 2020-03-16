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

package io.confluent.ksql.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.query.TransientQueryQueue.QueuePopulator;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class TransientQueryQueueTest {

  private static final int SOME_LIMIT = 4;
  private static final int MAX_LIMIT = SOME_LIMIT * 2;
  private static final GenericRow ROW_ONE = mock(GenericRow.class);
  private static final GenericRow ROW_TWO = mock(GenericRow.class);

  @Rule
  public final Timeout timeout = Timeout.seconds(10);

  @Mock
  private LimitHandler limitHandler;
  @Mock
  private KStream<String, GenericRow> kStreamsApp;
  @Captor
  private ArgumentCaptor<QueuePopulator<String>> queuePopulatorCaptor;
  private QueuePopulator<String> queuePopulator;
  private TransientQueryQueue queue;
  private ScheduledExecutorService executorService;

  @Before
  public void setUp() {
    givenQueue(OptionalInt.of(SOME_LIMIT));
  }

  @After
  public void tearDown() {
    if (executorService != null) {
      executorService.shutdownNow();
    }
  }

  @Test
  public void shouldQueue() {
    // When:
    queuePopulator.apply("key1", ROW_ONE);
    queuePopulator.apply("key2", ROW_TWO);

    // Then:
    assertThat(drainValues(), contains(
        new KeyValue<>("key1", ROW_ONE),
        new KeyValue<>("key2", ROW_TWO)
    ));
  }

  @Test
  public void shouldNotQueueNullValues() {
    // When:
    queuePopulator.apply("key1", null);

    // Then:
    assertThat(queue.size(), is(0));
  }

  @Test
  public void shouldQueueUntilLimitReached() {
    // When:
    IntStream.range(0, SOME_LIMIT + 2)
        .forEach(idx -> queuePopulator.apply("key1", ROW_ONE));

    // Then:
    assertThat(queue.size(), is(SOME_LIMIT));
  }

  @Test
  public void shouldPoll() throws Exception {
    // Given:
    queuePopulator.apply("key1", ROW_ONE);
    queuePopulator.apply("key2", ROW_TWO);

    // When:
    final KeyValue<String, GenericRow> result = queue.poll(1, TimeUnit.SECONDS);

    // Then:
    assertThat(result, is(new KeyValue<>("key1", ROW_ONE)));
    assertThat(drainValues(), contains(new KeyValue<>("key2", ROW_TWO)));
  }

  @Test
  public void shouldNotCallLimitHandlerIfLimitNotReached() {
    // When:
    IntStream.range(0, SOME_LIMIT - 1)
        .forEach(idx -> queuePopulator.apply("key1", ROW_ONE));

    // Then:
    verify(limitHandler, never()).limitReached();
  }

  @Test
  public void shouldCallLimitHandlerAsLimitReached() {
    // When:
    IntStream.range(0, SOME_LIMIT)
        .forEach(idx -> queuePopulator.apply("key1", ROW_ONE));

    // Then:
    verify(limitHandler).limitReached();
  }

  @Test
  public void shouldCallLimitHandlerOnlyOnce() {
    // When:
    IntStream.range(0, SOME_LIMIT + 1)
        .forEach(idx -> queuePopulator.apply("key1", ROW_ONE));

    // Then:
    verify(limitHandler, times(1)).limitReached();
  }

  @Test
  public void shouldBlockOnProduceOnceQueueLimitReachedAndUnblockOnClose() {
    // Given:
    givenQueue(OptionalInt.empty());

    IntStream.range(0, MAX_LIMIT)
        .forEach(idx -> queuePopulator.apply("key1", ROW_ONE));

    givenWillCloseQueueAsync();

    // When:
    queuePopulator.apply("should not be queued", ROW_TWO);

    // Then: did not block and:
    assertThat(queue.size(), is(MAX_LIMIT));
  }

  private void givenWillCloseQueueAsync() {
    executorService = Executors.newSingleThreadScheduledExecutor();
    executorService.schedule(queue::close, 200, TimeUnit.MILLISECONDS);
  }

  private void givenQueue(final OptionalInt limit) {
    clearInvocations(kStreamsApp);
    queue = new TransientQueryQueue(kStreamsApp, limit, MAX_LIMIT, 1);

    queue.setLimitHandler(limitHandler);

    verify(kStreamsApp).foreach(queuePopulatorCaptor.capture());
    queuePopulator = queuePopulatorCaptor.getValue();
  }

  private List<KeyValue<String, GenericRow>> drainValues() {
    final List<KeyValue<String, GenericRow>> entries = new ArrayList<>();
    queue.drainTo(entries);
    return entries;
  }
}