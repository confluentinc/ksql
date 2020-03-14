/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.support.metrics.common.time;

public class TimeUtils {

  private static class SystemClock implements Clock {

    @Override
    public long currentTimeMs() {
      return System.currentTimeMillis();
    }

  }

  private final Clock clock;

  public TimeUtils() {
    this(new SystemClock());
  }

  public TimeUtils(final Clock clock) {
    this.clock = clock;
  }

  /**
   * Returns the current time in seconds since the epoch (aka Unix time).
   */
  public long nowInUnixTime() {
    return clock.currentTimeMs() / 1000;
  }

}
