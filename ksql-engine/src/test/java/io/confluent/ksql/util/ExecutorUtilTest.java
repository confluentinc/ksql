/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.util;

import static io.confluent.ksql.util.ExecutorUtil.RetryBehaviour.ON_RETRYABLE;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.errors.RetriableException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ExecutorUtilTest {

  private static final Duration SMALL_RETRY_BACKOFF = Duration.ofMillis(1);

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldRetryAndEventuallyThrowIfNeverSucceeds() throws Exception {
    // Given:
    final Callable<Object> neverSucceeds = () -> {
      throw new ExecutionException(new TestRetriableException("I will never succeed"));
    };

    // Expect:
    expectedException.expect(ExecutionException.class);
    expectedException.expectMessage("I will never succeed");

    // When:
    ExecutorUtil.executeWithRetries(neverSucceeds, ON_RETRYABLE, () -> SMALL_RETRY_BACKOFF);
  }

  @Test
  public void shouldRetryAndSucceed() throws Exception {
    // Given:
    final AtomicInteger counts = new AtomicInteger(5);
    final Callable<Object> eventuallySucceeds = () -> {
      if (counts.decrementAndGet() == 0) {
        return null;
      }
      throw new TestRetriableException("I will never succeed");
    };

    // When:
    ExecutorUtil.executeWithRetries(eventuallySucceeds, ON_RETRYABLE, () -> SMALL_RETRY_BACKOFF);

    // Then: Succeeded, i.e. did not throw.
  }

  @Test
  public void shouldReturnValue() throws Exception {
    // Given:
    final String expectedValue = "should return this";

    // When:
    final String result = ExecutorUtil.executeWithRetries(() -> expectedValue, ON_RETRYABLE);

    // Then:
    assertThat(result, is(expectedValue));
  }

  @Test
  public void shouldNotRetryOnNonRetriableException() throws Exception {
    // Expect
    final AtomicBoolean firstCall = new AtomicBoolean(true);
    final Callable<Object> throwsException = () -> {
      if (firstCall.get()) {
        firstCall.set(false);
        throw new RuntimeException("First non-retry exception");
      } else {
        throw new RuntimeException("Test should not retry");
      }
    };

    // Expect:
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("First non-retry exception");

    // When:
    ExecutorUtil.executeWithRetries(throwsException, ON_RETRYABLE);
  }

  @Test
  public void shouldNotRetryIfSupplierThrowsNonRetriableException() throws Exception {
    // Given:
    final AtomicBoolean firstCall = new AtomicBoolean(true);
    final Callable<Object> throwsNonRetriable = () -> {
      if (firstCall.get()) {
        firstCall.set(false);
        throw new RuntimeException("First non-retry exception");
      }
      throw new RuntimeException("Test should not retry");
    };

    // Expect:
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("First non-retry exception");

    // When:
    ExecutorUtil.executeWithRetries(throwsNonRetriable, ON_RETRYABLE);
  }

  @Test
  public void shouldRetryIfSupplierThrowsExecutionExceptionWrapingRetriable() throws Exception {
    // Given:
    final AtomicInteger counts = new AtomicInteger(5);
    final Callable<Object> throwsExecutionExceptionThenSucceeds = () -> {
      if (counts.decrementAndGet() == 0) {
        return null;
      }
      throw new ExecutionException(new TestRetriableException("Test should retry"));
    };

    // When:
    ExecutorUtil.executeWithRetries(throwsExecutionExceptionThenSucceeds, ON_RETRYABLE, () -> SMALL_RETRY_BACKOFF);

    // Then: Succeeded, i.e. did not throw.
  }

  private static final class TestRetriableException extends RetriableException {
    private TestRetriableException(final String msg) {
      super(msg);
    }
  }
}