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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.version.metrics.KsqlVersionMetrics;
import io.confluent.support.metrics.common.Version;
import java.time.Clock;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(EasyMockRunner.class)
public class BasicCollectorTest {

  private static final KsqlModuleType MODULE_TYPE = KsqlModuleType.SERVER;

  private BasicCollector basicCollector;
  @Mock(MockType.NICE)
  private Clock clock;
  @Mock(MockType.NICE)
  private Supplier<Boolean> activenessStatusSupplier;

  @Before
  public void setUp() throws Exception {
    EasyMock.expect(clock.millis()).andReturn(12345L).anyTimes();
    EasyMock.expect(activenessStatusSupplier.get()).andReturn(true).anyTimes();
    EasyMock.replay(activenessStatusSupplier, clock);

    basicCollector = new BasicCollector(MODULE_TYPE, activenessStatusSupplier, clock);
  }

  @Test
  public void testCollectMetricsAssignsCurrentTime() {
    final long t1 = 1000L;
    final long t2 = 1001L;
    EasyMock.reset(clock);
    EasyMock.expect(clock.millis())
        .andReturn(TimeUnit.SECONDS.toMillis(t1))
        .andReturn(TimeUnit.SECONDS.toMillis(t2));
    EasyMock.replay(clock);

    KsqlVersionMetrics metrics = basicCollector.collectMetrics();
    assertThat(metrics.getTimestamp(), is(t1));

    metrics = basicCollector.collectMetrics();
    assertThat(metrics.getTimestamp(), is(t2));
  }

  @Test
  public void shouldSetActivenessToTrue() {
    EasyMock.reset(activenessStatusSupplier);
    EasyMock.expect(activenessStatusSupplier.get()).andReturn(true);
    EasyMock.replay(activenessStatusSupplier);

    final KsqlVersionMetrics merics = basicCollector.collectMetrics();

    assertThat(merics.getIsActive(), is(true));
  }

  @Test
  public void shouldSetActivenessToFalse() {
    EasyMock.reset(activenessStatusSupplier);
    EasyMock.expect(activenessStatusSupplier.get()).andReturn(false);
    EasyMock.replay(activenessStatusSupplier);

    final KsqlVersionMetrics merics = basicCollector.collectMetrics();

    assertThat(merics.getIsActive(), is(false));
  }

  @Test
  public void shouldReportVersion() {
    final KsqlVersionMetrics merics = basicCollector.collectMetrics();

    assertThat(merics.getConfluentPlatformVersion(), is(Version.getVersion()));
  }

  @Test
  public void shouldReportComponentType() {
    final KsqlVersionMetrics merics = basicCollector.collectMetrics();

    assertThat(merics.getKsqlComponentType(), is(MODULE_TYPE.name()));
  }
}
