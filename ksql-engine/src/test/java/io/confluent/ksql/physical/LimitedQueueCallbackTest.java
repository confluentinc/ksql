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
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LimitedQueueCallbackTest {

  private static final int SOME_LIMIT = 3;

  @Mock
  private LimitHandler limitHandler;
  private LimitedQueueCallback callback;

  @Before
  public void setUp() {
    callback = new LimitedQueueCallback(SOME_LIMIT);
    callback.setLimitHandler(limitHandler);
  }

  @Test
  public void shouldAllowQueuingUpToTheLimit() {
    // When:
    IntStream.range(0, SOME_LIMIT).forEach(idx ->
        assertThat(callback.shouldQueue(), is(true)));

    // Then:
    IntStream.range(0, SOME_LIMIT).forEach(idx ->
        assertThat(callback.shouldQueue(), is(false)));
  }

  @Test
  public void shouldNotCallLimitHandlerIfLimitNotReached() {
    // When:
    IntStream.range(0, SOME_LIMIT - 1).forEach(idx -> callback.onQueued());

    // Then:
    verify(limitHandler, never()).limitReached();
  }

  @Test
  public void shouldCallLimitHandlerOnceLimitReached() {
    // When:
    IntStream.range(0, SOME_LIMIT).forEach(idx -> callback.onQueued());

    // Then:
    verify(limitHandler).limitReached();
  }

  @Test
  public void shouldOnlyCallLimitHandlerOnce() {
    // When:
    IntStream.range(0, SOME_LIMIT * 2).forEach(idx -> callback.onQueued());

    // Then:
    verify(limitHandler, times(1)).limitReached();
  }

  @Test
  public void shouldBeThreadSafe() {
    // Given:
    final int theLimit = 100;
    callback = new LimitedQueueCallback(theLimit);
    callback.setLimitHandler(limitHandler);

    // When:
    final int enqueued = IntStream.range(0, theLimit * 2)
        .parallel()
        .map(idx -> {
          if (!callback.shouldQueue()) {
            return 0;
          }
          callback.onQueued();
          return 1;
        })
        .sum();

    // Then:
    assertThat(enqueued, is(theLimit));
    verify(limitHandler, times(1)).limitReached();
  }
}