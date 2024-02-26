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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ExecutorUtil {

  private static final int NUM_RETRIES = 5;
  private static final Duration RETRY_BACKOFF_MS = Duration.ofMillis(500);
  private static final Logger log = LoggerFactory.getLogger(ExecutorUtil.class);

  private ExecutorUtil() {
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
    return executeWithRetries(executable, shouldRetry, (retry) -> retryBackOff.get(), NUM_RETRIES);
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
