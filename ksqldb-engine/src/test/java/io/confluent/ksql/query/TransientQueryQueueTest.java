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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.confluent.ksql.GenericRow;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

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
    queue.acceptRow(ROW_ONE);
    queue.acceptRow(ROW_TWO);

    // Then:
    assertThat(drainValues(), contains(ROW_ONE, ROW_TWO));
  }

  @Test
  public void shouldNotQueueNullValues() {
    // When:
    queue.acceptRow(null);

    // Then:
    assertThat(queue.size(), is(0));
  }

  @Test
  public void shouldQueueUntilLimitReached() {
    // When:
    IntStream.range(0, SOME_LIMIT + 2)
        .forEach(idx -> queue.acceptRow(ROW_ONE));

    // Then:
    assertThat(queue.size(), is(SOME_LIMIT));
  }

  @Test
  public void shouldPoll() throws Exception {
    // Given:
    queue.acceptRow(ROW_ONE);
    queue.acceptRow(ROW_TWO);

    // When:
    final GenericRow result = queue.poll(1, TimeUnit.SECONDS);

    // Then:
    assertThat(result, is(ROW_ONE));
    assertThat(drainValues(), contains(ROW_TWO));
  }

  @Test
  public void shouldNotCallLimitHandlerIfLimitNotReached() {
    // When:
    IntStream.range(0, SOME_LIMIT - 1)
        .forEach(idx -> queue.acceptRow(ROW_ONE));

    // Then:
    verify(limitHandler, never()).limitReached();
  }

  @Test
  public void shouldCallLimitHandlerAsLimitReached() {
    // When:
    IntStream.range(0, SOME_LIMIT)
        .forEach(idx -> queue.acceptRow(ROW_ONE));

    // Then:
    verify(limitHandler).limitReached();
  }

  @Test
  public void shouldCallLimitHandlerOnlyOnce() {
    // When:
    IntStream.range(0, SOME_LIMIT + 1)
        .forEach(idx -> queue.acceptRow(ROW_ONE));

    // Then:
    verify(limitHandler, times(1)).limitReached();
  }

  @Test
  public void shouldBlockOnProduceOnceQueueLimitReachedAndUnblockOnClose() {
    // Given:
    givenQueue(OptionalInt.empty());

    IntStream.range(0, MAX_LIMIT)
        .forEach(idx -> queue.acceptRow(ROW_ONE));

    givenWillCloseQueueAsync();

    // When:
    queue.acceptRow(ROW_TWO);

    // Then: did not block and:
    assertThat(queue.size(), is(MAX_LIMIT));
  }

  private void givenWillCloseQueueAsync() {
    executorService = Executors.newSingleThreadScheduledExecutor();
    executorService.schedule(queue::close, 200, TimeUnit.MILLISECONDS);
  }

  private void givenQueue(final OptionalInt limit) {
    queue = new TransientQueryQueue(limit, MAX_LIMIT, 1);

    queue.setLimitHandler(limitHandler);
  }

  private List<GenericRow> drainValues() {
    final List<GenericRow> entries = new ArrayList<>();
    queue.drainTo(entries);
    return entries;
  }
}