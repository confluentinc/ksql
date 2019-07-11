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

package io.confluent.ksql.version.metrics.collector;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import io.confluent.ksql.util.Version;
import io.confluent.ksql.version.metrics.KsqlVersionMetrics;
import java.time.Clock;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BasicCollectorTest {

  private static final KsqlModuleType MODULE_TYPE = KsqlModuleType.SERVER;

  private BasicCollector basicCollector;
  @Mock
  private Clock clock;
  @Mock
  private Supplier<Boolean> activenessStatusSupplier;

  @Before
  public void setUp() {
    when(clock.millis()).thenReturn(12345L);
    when(activenessStatusSupplier.get()).thenReturn(true);
    basicCollector = new BasicCollector(MODULE_TYPE, activenessStatusSupplier, clock);
  }

  @Test
  public void testCollectMetricsAssignsCurrentTime() {
    // Given:
    final long t1 = 1000L;
    final long t2 = 1001L;
    when(clock.millis())
        .thenReturn(TimeUnit.SECONDS.toMillis(t1))
        .thenReturn(TimeUnit.SECONDS.toMillis(t2));

    // When:
    KsqlVersionMetrics metrics = basicCollector.collectMetrics();

    // Then:
    assertThat(metrics.getTimestamp(), is(t1));

    metrics = basicCollector.collectMetrics();
    assertThat(metrics.getTimestamp(), is(t2));
  }

  @Test
  public void shouldSetActivenessToTrue() {
    // Given:
    when(activenessStatusSupplier.get()).thenReturn(true);

    // When:
    final KsqlVersionMetrics metrics = basicCollector.collectMetrics();

    // Then:
    assertThat(metrics.getIsActive(), is(true));
  }

  @Test
  public void shouldSetActivenessToFalse() {
    // Given:
    when(activenessStatusSupplier.get()).thenReturn(false);

    // When:
    final KsqlVersionMetrics metrics = basicCollector.collectMetrics();

    // Then:
    assertThat(metrics.getIsActive(), is(false));
  }

  @Test
  public void shouldReportVersion() {
    // When:
    final KsqlVersionMetrics metrics = basicCollector.collectMetrics();

    // Then:
    assertThat(metrics.getConfluentPlatformVersion(), is(Version.getVersion()));
  }

  @Test
  public void shouldReportComponentType() {
    // When:
    final KsqlVersionMetrics metrics = basicCollector.collectMetrics();

    // Then:
    assertThat(metrics.getKsqlComponentType(), is(MODULE_TYPE.name()));
  }
}

