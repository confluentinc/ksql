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

package io.confluent.support.metrics.common.time;

import static org.junit.Assert.assertEquals;

import io.confluent.support.metrics.common.time.Clock;
import io.confluent.support.metrics.common.time.TimeUtils;
import org.junit.Test;

public class TimeUtilsTest {

  private static class FixedClock implements Clock {

    private final long fixedTimeMs;

    public FixedClock(long fixedTimeMs) {
      this.fixedTimeMs = fixedTimeMs;
    }

    @Override
    public long currentTimeMs() {
      return fixedTimeMs;
    }

  }

  @Test
  public void returnsCurrentUnixTime() {
    // Given
    long expCurrentUnixTime = 12345678L;
    TimeUtils tu = new TimeUtils(new FixedClock(expCurrentUnixTime * 1000));

    // When/Then
    assertEquals(expCurrentUnixTime, tu.nowInUnixTime());
  }
}
