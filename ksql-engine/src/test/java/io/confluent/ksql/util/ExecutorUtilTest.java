/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.util;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.errors.RetriableException;
import org.junit.Test;

public class ExecutorUtilTest {

  @Test
  public void shouldRetryAndEventuallyThrowIfNeverSucceeds() throws Exception {
    Exception e = assertThrows(ExecutionException.class, () -> ExecutorUtil.executeWithRetries(() -> {
          throw new ExecutionException(new TestRetriableException("I will never succeed"));
        },
        ExecutorUtil.RetryBehaviour.ON_RETRYABLE));
    assertEquals("I will never succeed", e.getMessage());
  }

  @Test
  public void shouldRetryAndSucceed() throws Exception {
    final AtomicInteger counts = new AtomicInteger(5);
    ExecutorUtil.executeWithRetries(() -> {
      if (counts.decrementAndGet() == 0) {
        return null;
      }
      throw new TestRetriableException("I will never succeed");
    },
    ExecutorUtil.RetryBehaviour.ON_RETRYABLE);
  }

  @Test
  public void shouldReturnValue() throws Exception {
    final String expectedValue = "should return this";
    assertThat(ExecutorUtil.executeWithRetries(
        () -> expectedValue,
        ExecutorUtil.RetryBehaviour.ON_RETRYABLE),
        is(expectedValue));
  }

  @Test
  public void shouldNotRetryOnNonRetryableException() throws Exception {
    final AtomicBoolean firstCall = new AtomicBoolean(true);
    Exception e = assertThrows(RuntimeException.class, () -> ExecutorUtil.executeWithRetries(() -> {
      if (firstCall.get()) {
        firstCall.set(false);
        throw new RuntimeException("First non-retry exception");
      } else {
        throw new RuntimeException("Test should not retry");
      }
    }, ExecutorUtil.RetryBehaviour.ON_RETRYABLE));
    assertEquals("First non-retry exception", e.getMessage());
  }

  @Test
  public void shouldNotRetryIfSupplierThrowsNonRetryableException() throws Exception {
    final AtomicBoolean firstCall = new AtomicBoolean(true);
    Exception e = assertThrows(RuntimeException.class, () -> ExecutorUtil.executeWithRetries(() -> {
      if (firstCall.get()) {
        firstCall.set(false);
        throw new RuntimeException("First non-retry exception");
      }
      throw new RuntimeException("Test should not retry");
    }, ExecutorUtil.RetryBehaviour.ON_RETRYABLE));
    assertEquals("First non-retry exception", e.getMessage());
  }

  @Test
  public void shouldRetryIfSupplierThrowsRetryableException() throws Exception {
    final AtomicInteger counts = new AtomicInteger(5);
    ExecutorUtil.executeWithRetries(() -> {
      if (counts.decrementAndGet() == 0) {
        return null;
      }
      throw new TestRetriableException("Test should retry");
    }, ExecutorUtil.RetryBehaviour.ON_RETRYABLE);
  }

  @Test
  public void shouldRetryIfSupplierThrowsExecutionException() throws Exception {
    final AtomicInteger counts = new AtomicInteger(5);
    ExecutorUtil.executeWithRetries(() -> {
      if (counts.decrementAndGet() == 0) {
        return null;
      }
      throw new ExecutionException(new TestRetriableException("Test should retry"));
    }, ExecutorUtil.RetryBehaviour.ON_RETRYABLE);
  }

  private static final class TestRetriableException extends RetriableException {
    private TestRetriableException(final String msg) {
      super(msg);
    }
  }

}
