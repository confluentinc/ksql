/*
 * Copyright 2017 Confluent Inc.
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
 */

package io.confluent.ksql.version.metrics.collector;

import static org.junit.Assert.assertEquals;

import io.confluent.ksql.version.metrics.KsqlVersionMetrics;
import io.confluent.support.metrics.common.Version;
import io.confluent.support.metrics.common.time.Clock;
import io.confluent.support.metrics.common.time.TimeUtils;
import java.util.Collection;
import java.util.EnumSet;
import java.util.function.Supplier;
import org.easymock.EasyMock;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class BasicCollectorTest {

  public class MockClock implements Clock {
    private long currentTime = 0;

    public MockClock() {
    }

    public long currentTimeMs() {
      return currentTime;
    }

    public void setCurrentTimeMillis(final long timeMillis) {
      currentTime = timeMillis;
    }
  }

  @Parameterized.Parameters
  public static Collection<KsqlModuleType> data() {
    return EnumSet.allOf(KsqlModuleType.class);
  }

  @Parameterized.Parameter
  public KsqlModuleType moduleType;

  private MockClock mockClock;
  private TimeUtils timeUtils;
  private Supplier<Boolean> supplier;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    mockClock = new MockClock();
    timeUtils = new TimeUtils(mockClock);
    supplier = EasyMock.mock(Supplier.class);
    EasyMock.expect(supplier.get()).andReturn(true).times(2);
    EasyMock.replay(supplier);
  }

  @Test
  public void testGetCollector() {
    final BasicCollector basicCollector = new BasicCollector(moduleType, timeUtils, supplier);

    final KsqlVersionMetrics expectedMetrics = getExpectedMetrics(true);

    // should match because we don't advance the clock
    Assert.assertThat(basicCollector.collectMetrics(), CoreMatchers.equalTo(expectedMetrics));
  }

  @Test
  public void testCollectMetricsAssignsCurrentTime() {
    Long currentTimeSec = 1000L;

    mockClock.setCurrentTimeMillis(currentTimeSec * 1000);
    final BasicCollector basicCollector = new BasicCollector(moduleType, timeUtils, supplier);

    currentTimeSec += 12300L;
    mockClock.setCurrentTimeMillis(currentTimeSec * 1000);
    KsqlVersionMetrics metrics = (KsqlVersionMetrics) basicCollector.collectMetrics();
    assertEquals(currentTimeSec, metrics.getTimestamp());

    currentTimeSec += 734L;
    mockClock.setCurrentTimeMillis(currentTimeSec * 1000);
    metrics = (KsqlVersionMetrics) basicCollector.collectMetrics();
    assertEquals(currentTimeSec, metrics.getTimestamp());
  }

  @Test
  public void shouldSetActivenessToTrue() {
    EasyMock.reset(supplier);
    EasyMock.expect(supplier.get()).andReturn(true);
    EasyMock.replay(supplier);

    final BasicCollector basicCollector = new BasicCollector(moduleType, timeUtils, supplier);

    final KsqlVersionMetrics expectedMetrics = getExpectedMetrics(true);

    // should match because we don't advance the clock
    Assert.assertThat(basicCollector.collectMetrics(), CoreMatchers.equalTo(expectedMetrics));
  }

  @Test
  public void shouldSetActivenessToFalse() {
    EasyMock.reset(supplier);
    EasyMock.expect(supplier.get()).andReturn(false);
    EasyMock.replay(supplier);

    final BasicCollector basicCollector = new BasicCollector(moduleType, timeUtils, supplier);

    final KsqlVersionMetrics expectedMetrics = getExpectedMetrics(false);

    // should match because we don't advance the clock
    Assert.assertThat(basicCollector.collectMetrics(), CoreMatchers.equalTo(expectedMetrics));
  }

  private KsqlVersionMetrics getExpectedMetrics(final boolean isActive) {
    final KsqlVersionMetrics expectedMetrics = new KsqlVersionMetrics();
    expectedMetrics.setTimestamp(timeUtils.nowInUnixTime());
    expectedMetrics.setConfluentPlatformVersion(Version.getVersion());
    expectedMetrics.setKsqlComponentType(moduleType.name());
    expectedMetrics.setIsActive(isActive);
    return expectedMetrics;
  }

}
