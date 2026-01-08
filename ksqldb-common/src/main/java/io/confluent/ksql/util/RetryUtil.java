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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class RetryUtil {
  private static final Logger log = LogManager.getLogger(RetryUtil.class);

  private RetryUtil() {
  }

  public static void retryWithBackoff(
      final int maxRetries,
      final int initialWaitMs,
      final int maxWaitMs,
      final Runnable runnable,
      final Class<?>... passThroughExceptions) {
    retryWithBackoff(
        maxRetries,
        initialWaitMs,
        maxWaitMs,
        runnable,
        () -> false,
        Arrays.stream(passThroughExceptions)
            .map(c -> (Predicate<Exception>) c::isInstance)
            .collect(Collectors.toList())
    );
  }

  public static void retryWithBackoff(
      final int maxRetries,
      final int initialWaitMs,
      final int maxWaitMs,
      final Runnable runnable,
      final Supplier<Boolean> stopRetrying,
      final List<Predicate<Exception>> passThroughExceptions) {
    retryWithBackoff(
        maxRetries,
        initialWaitMs,
        maxWaitMs,
        runnable,
        duration -> {
          try {
            Thread.sleep(duration);
          } catch (final InterruptedException e) {
            log.debug("retryWithBackoff interrupted while sleeping");
          }
        },
        stopRetrying,
        passThroughExceptions
    );
  }

  static void retryWithBackoff(
      final int maxRetries,
      final int initialWaitMs,
      final int maxWaitMs,
      final Runnable runnable,
      final Consumer<Long> sleep,
      final Supplier<Boolean> stopRetrying,
      final List<Predicate<Exception>> passThroughExceptions) {
    long wait = initialWaitMs;
    int i = 0;
    while (true) {
      try {
        runnable.run();
        return;
      } catch (final RuntimeException exception) {
        passThroughExceptions.stream()
            .filter(pte -> pte.test(exception))
            .findFirst()
            .ifPresent(
                e -> {
                  throw exception;
                });
        i++;
        if (i > maxRetries) {
          throw exception;
        }
        final StringWriter stringWriter = new StringWriter();
        final PrintWriter printWriter = new PrintWriter(stringWriter);
        exception.printStackTrace(printWriter);
        log.error(
            "Exception encountered running command: {}. Retrying in {} ms",
            exception.getMessage(),
            wait);
        log.error("Stack trace: " + stringWriter.toString());
        sleep.accept(wait);
        wait = wait * 2 > maxWaitMs ? maxWaitMs : wait * 2;

        // If the stopRetrying flag is used, it's likely triggered during sleep and would interrupt
        // the thread, so just check it after sleep and if set, throw the last exception.
        if (stopRetrying.get()) {
          log.info("Stopping retries");
          throw exception;
        }
      }
    }
  }
}
