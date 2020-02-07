package io.confluent.support.metrics.common.time;

import static org.junit.Assert.assertEquals;

import io.confluent.support.metrics.common.time.Clock;
import io.confluent.support.metrics.common.time.TimeUtils;
import org.junit.Test;

public class TimeUtilsTest {

  private class FixedClock implements Clock {

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
