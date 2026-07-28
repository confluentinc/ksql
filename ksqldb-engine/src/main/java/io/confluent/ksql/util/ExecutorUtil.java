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

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class ExecutorUtil {

  // Number of attempts for retryable Kafka admin requests (for example, waiting for a
  // just-created topic's metadata to become visible, where describeTopics retries
  // UNKNOWN_TOPIC_OR_PARTITION via RetryBehaviour.ON_RETRYABLE). This is derived from a
  // configurable timeout (see setRetryTimeoutMs) and the fixed backoff below, and defaults to
  // DEFAULT_NUM_RETRIES so behaviour is unchanged unless the timeout is configured.
  static final int DEFAULT_NUM_RETRIES = 5;
  private static final Duration RETRY_BACKOFF_MS = Duration.ofMillis(500);
  private static volatile int configuredNumRetries = DEFAULT_NUM_RETRIES;
  private static final Logger log = LogManager.getLogger(ExecutorUtil.class);

  private ExecutorUtil() {
  }

  /**
   * Sets the number of attempts used by the default {@code executeWithRetries} overloads for
   * retryable Kafka admin requests, derived from the given timeout and the fixed retry backoff.
   * At least one attempt is always made. Intended to be called once during startup from
   * configuration.
   *
   * @param retryTimeoutMs the total time to keep retrying a request for, in milliseconds
   */
  public static void setRetryTimeoutMs(final long retryTimeoutMs) {
    configuredNumRetries =
        Math.max(1, (int) Math.ceil((double) retryTimeoutMs / RETRY_BACKOFF_MS.toMillis()));
  }

  // Visible for testing: the number of attempts derived from the configured retry timeout.
  static int getConfiguredNumRetries() {
    return configuredNumRetries;
  }

  public enum RetryBehaviour implements Predicate<Throwable> {
    ALWAYS {
      @Override
      public boolean test(final Throwable throwable) {
        return throwable instanceof Exception;
      }
    },
    ON_RETRYABLE {
      @Override
      public boolean test(final Throwable throwable) {
        return throwable instanceof RetriableException;
      }
    }
  }

  @FunctionalInterface
  public interface Function {
    void call() throws Exception;
  }

  public static void executeWithRetries(
      final Function function,
      final RetryBehaviour retryBehaviour
  ) throws Exception {
    executeWithRetries(() -> {
      function.call();
      return null;
    }, retryBehaviour);
  }

  public static <T> T executeWithRetries(
      final Callable<T> executable,
      final RetryBehaviour retryBehaviour
  ) throws Exception {
    return executeWithRetries(executable, retryBehaviour, () -> RETRY_BACKOFF_MS);
  }

  public static <T> T executeWithRetries(
      final Callable<T> executable,
      final Predicate<Throwable> shouldRetry
  ) throws Exception {
    return executeWithRetries(executable, shouldRetry, () -> RETRY_BACKOFF_MS);
  }

  public static <T> T executeWithRetries(
      final Callable<T> executable,
      final Predicate<Throwable> shouldRetry,
      final Supplier<Duration> retryBackOff
  ) throws Exception {
    return executeWithRetries(
        executable, shouldRetry, (retry) -> retryBackOff.get(), configuredNumRetries);
  }

  public static <T> T executeWithRetries(
      final Callable<T> executable,
      final Predicate<Throwable> shouldRetry,
      final java.util.function.Function<Integer, Duration> retryBackOff,
      final int numRetries
  ) throws Exception {
    Exception lastException = null;
    for (int retries = 0; retries < numRetries; ++retries) {
      try {
        if (retries != 0) {
          Thread.sleep(retryBackOff.apply(retries).toMillis());
        }
        return executable.call();
      } catch (final Exception e) {
        final Throwable cause = e instanceof ExecutionException ? e.getCause() : e;
        if (shouldRetry.test(cause)) {
          log.info("Retrying request. Retry no: {} Cause: '{}'", retries, e.getMessage());
          lastException = e;
        } else if (cause instanceof Exception) {
          throw (Exception) cause;
        } else {
          throw new RuntimeException(e.getMessage());
        }
      }
    }
    throw lastException;
  }
}
