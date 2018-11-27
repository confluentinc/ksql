/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.rest.server.computation;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.concurrent.CompletableFuture;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class OffsetFutureStoreTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private OffsetFutureStore offsetFutureStore;

  @Before
  public void setUp() {
    offsetFutureStore = new OffsetFutureStore();
  }

  @Test
  public void shouldReturnFutureForNewOffset() {
    // When:
    final CompletableFuture<Void> future = offsetFutureStore.getFutureForOffset(2);

    // Then:
    assertFutureIsNotCompleted(future);
  }

  @Test
  public void shouldReturnFutureForExistingOffset() {
    // Given:
    final CompletableFuture<Void> existingFuture = offsetFutureStore.getFutureForOffset(2);

    // When:
    final CompletableFuture<Void> newFuture = offsetFutureStore.getFutureForOffset(2);

    // Then:
    assertThat(newFuture, is(sameInstance(existingFuture)));
  }

  @Test
  public void shouldReturnFutureForCompletedOffset() {
    // Given:
    final CompletableFuture<Void> firstFuture = offsetFutureStore.getFutureForOffset(2);
    offsetFutureStore.completeFuturesUpToOffset(3);
    assertFutureIsCompleted(firstFuture);

    // When:
    final CompletableFuture<Void> secondFuture = offsetFutureStore.getFutureForOffset(2);

    // Then:
    assertFutureIsNotCompleted(secondFuture);
    assertThat(secondFuture, is(not(sameInstance(firstFuture))));
  }

  @Test
  public void shouldCompleteFutures() {
    // Given:
    final CompletableFuture<Void> firstFuture = offsetFutureStore.getFutureForOffset(2);
    final CompletableFuture<Void> secondFuture = offsetFutureStore.getFutureForOffset(3);

    // When:
    offsetFutureStore.completeFuturesUpToOffset(3);

    // Then:
    assertFutureIsCompleted(firstFuture);
    assertFutureIsNotCompleted(secondFuture);
  }

  private static void assertFutureIsCompleted(CompletableFuture<Void> future) {
    assertThat(future.isDone(), is(true));
    assertThat(future.isCancelled(), is(false));
    assertThat(future.isCompletedExceptionally(), is(false));
  }

  private static void assertFutureIsNotCompleted(CompletableFuture<Void> future) {
    assertThat(future.isDone(), is(false));
  }
}