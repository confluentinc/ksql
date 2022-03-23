/*
 * Copyright 2021 Confluent Inc.
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
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.Lists;
import io.confluent.ksql.execution.pull.PullQueryRow;
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
public class PullQueryQueueTest {
  private static final int QUEUE_SIZE = 5;

  private static final PullQueryRow VAL_ONE = mock(PullQueryRow.class);
  private static final PullQueryRow VAL_TWO = mock(PullQueryRow.class);

  @Rule
  public final Timeout timeout = Timeout.seconds(100);

  @Mock
  private LimitHandler limitHandler;
  @Mock
  private CompletionHandler completionHandler;
  @Mock
  private Runnable queuedCallback;
  private PullQueryQueue queue;
  private ScheduledExecutorService executorService;

  @Before
  public void setUp() {
    givenQueue();
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
    queue.acceptRow(VAL_ONE);
    queue.acceptRow(VAL_TWO);

    // Then:
    assertThat(drainValues(), contains(VAL_ONE, VAL_TWO));
    verify(queuedCallback, times(2)).run();
  }

  @Test
  public void shouldQueueUntilClosed() {
    // When:
    IntStream.range(0, QUEUE_SIZE)
        .forEach(idx -> {
          queue.acceptRow(VAL_ONE);
          if (idx == 2) {
            queue.close();
          }
        });

    // Then:
    assertThat(queue.size(), is(3));
    verify(queuedCallback, times(3)).run();
  }

  @Test
  public void shouldPoll() throws Exception {
    // Given:
    queue.acceptRow(VAL_ONE);
    queue.acceptRow(VAL_TWO);

    // When:
    final PullQueryRow result1 = queue.pollRow(1, TimeUnit.SECONDS);
    final PullQueryRow result2 = queue.pollRow(1, TimeUnit.SECONDS);
    final PullQueryRow result3 = queue.pollRow(1, TimeUnit.MICROSECONDS);

    // Then:
    assertThat(result1, is(VAL_ONE));
    assertThat(result2, is(VAL_TWO));
    assertThat(result3, is(nullValue()));
    verify(queuedCallback, times(2)).run();
  }

  @Test
  public void shouldCallCompleteOnClose() {
    // When:
    queue.close();
    queue.close();

    // Then:
    verify(completionHandler, times(1)).complete();
  }


  @Test
  public void shouldBlockOnProduceOnceQueueLimitReachedAndUnblockOnClose() {
    // Given:
    givenQueue();

    IntStream.range(0, QUEUE_SIZE)
        .forEach(idx -> queue.acceptRow(VAL_ONE));

    givenWillCloseQueueAsync();

    // When:
    queue.acceptRow(VAL_TWO);

    // Then: did not block and:
    assertThat(queue.size(), is(QUEUE_SIZE));
    verify(queuedCallback, times(QUEUE_SIZE)).run();
  }

  @Test
  public void shouldOnlyAcceptLimitRows() {
    // Given:
    final int LIMIT_SIZE = 3;
    queue = new PullQueryQueue(QUEUE_SIZE, 1, OptionalInt.of(LIMIT_SIZE));

    queue.setLimitHandler(limitHandler);
    queue.setQueuedCallback(queuedCallback);

    IntStream.range(0, QUEUE_SIZE)
            .forEach(idx -> queue.acceptRow(VAL_ONE));

    givenWillCloseQueueAsync();

    // Then
    assertThat(queue.getTotalRowsQueued(), is(Long.valueOf(LIMIT_SIZE)));
    verify(queuedCallback, times(LIMIT_SIZE)).run();
    verify(limitHandler, times(1)).limitReached();
  }

  @Test
  public void shouldOnlyAcceptLimitRowsWhenLimitMoreThanCapacity() {
    // Given:
    final List<PullQueryRow> rows = Lists.newArrayList();
    // limit is twice the queue capacity
    final int LIMIT_SIZE = QUEUE_SIZE * 2;
    queue = new PullQueryQueue(QUEUE_SIZE, 1, OptionalInt.of(LIMIT_SIZE));

    queue.setLimitHandler(limitHandler);
    queue.setQueuedCallback(queuedCallback);

    // fill up the queue the first time
    IntStream.range(0, QUEUE_SIZE)
            .forEach(idx -> queue.acceptRow(VAL_ONE));

    //drain them as capacity reached
    queue.drainRowsTo(rows);

    // fill up the queue the second time
    IntStream.range(0, QUEUE_SIZE)
            .forEach(idx -> queue.acceptRow(VAL_ONE));

    //drain them as capacity reached
    queue.drainRowsTo(rows);

    // assert that we have processed limit rows
    assertThat(queue.getTotalRowsQueued(), is(Long.valueOf(LIMIT_SIZE)));

    //try adding some more rows
    IntStream.range(0, 3)
            .forEach(idx -> queue.acceptRow(VAL_ONE));

    givenWillCloseQueueAsync();

    // Then
    assertThat(rows.size(), is(LIMIT_SIZE));
    verify(queuedCallback, times(LIMIT_SIZE)).run();
    verify(limitHandler, times(1)).limitReached();
  }

  private void givenWillCloseQueueAsync() {
    executorService = Executors.newSingleThreadScheduledExecutor();
    executorService.schedule(queue::close, 200, TimeUnit.MILLISECONDS);
  }

  private void givenQueue() {
    queue = new PullQueryQueue(QUEUE_SIZE, 1, OptionalInt.empty());

    queue.setLimitHandler(limitHandler);
    queue.setCompletionHandler(completionHandler);
    queue.setQueuedCallback(queuedCallback);
  }

  private List<PullQueryRow> drainValues() {
    final List<PullQueryRow> entries = new ArrayList<>();
    queue.drainRowsTo(entries);
    return entries;
  }

  @Test
  public void totalRowsQueued() {
    // When:
    queue.acceptRow(VAL_ONE);

    // Then:
    assertThat(queue.getTotalRowsQueued(), is(1L));
    queue.acceptRow(VAL_TWO);
    assertThat(queue.getTotalRowsQueued(), is(2L));
  }
}
