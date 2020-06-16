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

package io.confluent.ksql.test.util;

import static org.hamcrest.MatcherAssert.assertThat;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.hamcrest.Matcher;

/**
 * Hamcrest async assert with timeout.
 */
public final class AssertEventually {
  private static final long DEFAULT_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(30);
  private static final long DEFAULT_INITIAL_PAUSE_PERIOD_MS = 1;
  private static final long DEFAULT_MAX_PAUSE_PERIOD_MS = TimeUnit.SECONDS.toMillis(1);

  public static <T> T assertThatEventually(final Supplier<? extends T> actualSupplier,
                                           final Matcher<? super T> expected) {
    return assertThatEventually("", actualSupplier, expected);
  }

  public static <T> T assertThatEventually(final String message,
                                           final Supplier<? extends T> actualSupplier,
                                           final Matcher<? super T> expected) {
    return assertThatEventually(
        message, actualSupplier, expected, DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
  }

  public static <T> T assertThatEventually(final String message,
      final Supplier<? extends T> actualSupplier,
      final Matcher<? super T> expected,
      final long timeout,
      final TimeUnit unit) {
    return assertThatEventually(
        message, actualSupplier, expected, timeout, unit,
        DEFAULT_INITIAL_PAUSE_PERIOD_MS, DEFAULT_MAX_PAUSE_PERIOD_MS);
  }

  public static <T> T assertThatEventually(final String message,
      final Supplier<? extends T> actualSupplier,
      final Matcher<? super T> expected,
      final long initialPausePeriodMs,
      final long maxPausePeriodMs) {
    return assertThatEventually(
        message, actualSupplier, expected, DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS,
        initialPausePeriodMs, maxPausePeriodMs);
  }

  public static <T> T assertThatEventually(final String message,
                                           final Supplier<? extends T> actualSupplier,
                                           final Matcher<? super T> expected,
                                           final long timeout,
                                           final TimeUnit unit,
                                           final long initialPausePeriodMs,
                                           final long maxPausePeriodMs) {
    try {

      final long end = System.currentTimeMillis() + unit.toMillis(timeout);

      long period = initialPausePeriodMs;
      while (System.currentTimeMillis() < end) {
        final T actual = actualSupplier.get();
        if (expected.matches(actual)) {
          return actual;
        }

        Thread.sleep(period);
        period = Math.min(period * 2, maxPausePeriodMs);
      }

      final T actual = actualSupplier.get();
      assertThat(message, actual, expected);
      return actual;
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private AssertEventually() {
  }
}
