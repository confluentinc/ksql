/**
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
 **/

package io.confluent.ksql.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RetryUtil {
  private static final Logger log = LoggerFactory.getLogger(RetryUtil.class);

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
        duration -> {
          try {
            Thread.sleep(duration);
          } catch (final InterruptedException e) {
            log.debug("retryWithBackoff interrupted while sleeping");
          }
        },
        passThroughExceptions
    );
  }

  static void retryWithBackoff(
      final int maxRetries,
      final int initialWaitMs,
      final int maxWaitMs,
      final Runnable runnable,
      final Consumer<Long> sleep,
      final Class<?>... passThroughExceptions) {
    long wait = initialWaitMs;
    int i = 0;
    while (true) {
      try {
        runnable.run();
        return;
      } catch (final RuntimeException exception) {
        Arrays.stream(passThroughExceptions)
            .filter(pte -> pte.isInstance(exception))
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
      }
    }
  }
}
