/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.support.metrics.utils;

import java.util.concurrent.ThreadLocalRandom;

public final class Jitter {

  private Jitter() {
    throw new IllegalStateException("Utility class should not be instantiated");
  }

  /**
   * Adds 1% to a value. If value is 0, returns 0. If value is negative, adds 1% of abs(value) to it
   *
   * @param value Number to add 1% to. Could be negative.
   * @return Value +1% of abs(value)
   */
  public static long addOnePercentJitter(final long value) {
    if (value == 0 || value < 100) {
      return value;
    }
    return value + ThreadLocalRandom.current().nextInt((int) Math.abs(value) / 100);
  }
}
