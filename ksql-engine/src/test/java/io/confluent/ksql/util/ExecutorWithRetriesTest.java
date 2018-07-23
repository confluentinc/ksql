/*
 * Copyright 2017 Confluent Inc.
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
 **/

package io.confluent.ksql.util;

import org.apache.kafka.common.errors.RetriableException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ExecutorWithRetriesTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldRetryAndEventuallyThrowIfNeverSucceeds() throws Exception {
    expectedException.expect(ExecutionException.class);
    expectedException.expectMessage("I will never succeed");

    ExecutorWithRetries.executeSync(() -> {
          final CompletableFuture<Void> f = new CompletableFuture<>();
          f.completeExceptionally(new TestRetriableException("I will never succeed"));
          return f;
        },
        ExecutorWithRetries.RetryBehaviour.ON_RETRYABLE);
  }

  @Test
  public void shouldRetryAndSucceed() throws Exception {
    final AtomicInteger counts = new AtomicInteger(5);

    ExecutorWithRetries.executeSync(() -> {
      if (counts.decrementAndGet() == 0) {
        return CompletableFuture.completedFuture(null);
      }

      final CompletableFuture<Void> f = new CompletableFuture<>();
      f.completeExceptionally(new TestRetriableException("I will never succeed"));
      return f;
    },
    ExecutorWithRetries.RetryBehaviour.ON_RETRYABLE);
  }

  @Test
  public void shouldReturnValue() throws Exception {
    final String expectedValue = "should return this";

    assertThat(ExecutorWithRetries.executeSync(
        () -> CompletableFuture.completedFuture(expectedValue),
        ExecutorWithRetries.RetryBehaviour.ON_RETRYABLE),
        is(expectedValue));
  }

  @Test
  public void shouldNotRetryOnNonRetryableException() throws Exception {
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("First non-retry exception");

    final AtomicBoolean firstCall = new AtomicBoolean(true);

    ExecutorWithRetries.executeSync(() -> {
      final CompletableFuture<Void> f = new CompletableFuture<>();

      if (firstCall.get()) {
        firstCall.set(false);
        f.completeExceptionally(new RuntimeException("First non-retry exception"));
      } else {
        f.completeExceptionally(new RuntimeException("Test should not retry"));
      }

      return f;
    }, ExecutorWithRetries.RetryBehaviour.ON_RETRYABLE);
  }

  @Test
  public void shouldNotRetryIfSupplierThrowsNonRetryableException() throws Exception {
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("First non-retry exception");

    final AtomicBoolean firstCall = new AtomicBoolean(true);

    ExecutorWithRetries.executeSync(() -> {
      if (firstCall.get()) {
        firstCall.set(false);
        throw new RuntimeException("First non-retry exception");
      }

      throw new RuntimeException("Test should not retry");
    }, ExecutorWithRetries.RetryBehaviour.ON_RETRYABLE);
  }

  @Test
  public void shouldRetryIfSupplierThrowsRetryableException() throws Exception {
    final AtomicInteger counts = new AtomicInteger(5);

    ExecutorWithRetries.executeSync(() -> {
      if (counts.decrementAndGet() == 0) {
        return CompletableFuture.completedFuture(null);
      }

      throw new TestRetriableException("Test should retry");
    }, ExecutorWithRetries.RetryBehaviour.ON_RETRYABLE);
  }

  private static final class TestRetriableException extends RetriableException {
    private TestRetriableException(final String msg) {
      super(msg);
    }
  }

}