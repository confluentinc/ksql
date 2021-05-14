/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.util.HostStatus;
import io.confluent.ksql.util.KsqlHostInfo;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LivenessFilterTest {

  @Mock
  private HeartbeatAgent heartbeatAgent;

  private Map<KsqlHostInfo, HostStatus> allHostsStatus;
  private KsqlHostInfo activeHost;
  private KsqlHostInfo standByHost1;
  private KsqlHostInfo standByHost2;
  private LivenessFilter livenessFilter;

  private static final HostStatus HOST_ALIVE = new HostStatus(true, 0L);
  private static final HostStatus HOST_DEAD = new HostStatus(false, 0L);

  @Before
  public void setUp() {
    activeHost = new KsqlHostInfo("activeHost", 2345);
    standByHost1 = new KsqlHostInfo("standby1", 1234);
    standByHost2 = new KsqlHostInfo("standby2", 5678);
    Optional<HeartbeatAgent> optionalHeartbeatAgent = Optional.of(heartbeatAgent);
    livenessFilter = new LivenessFilter(optionalHeartbeatAgent);
  }

  @Test
  public void shouldFilterActiveAlive() {
    // Given:
    allHostsStatus = ImmutableMap.of(
        activeHost, HOST_ALIVE,
        standByHost1, HOST_DEAD,
        standByHost2, HOST_DEAD
    );
    when(heartbeatAgent.getHostsStatus()).thenReturn(allHostsStatus);

    // When:
    final boolean filterActive = livenessFilter.filter(activeHost);
    final boolean filterStandby1 = livenessFilter.filter(standByHost1);
    final boolean filterStandby2 = livenessFilter.filter(standByHost2);

    // Then:
    assertThat(filterActive, is(true));
    assertThat(filterStandby1, is(false));
    assertThat(filterStandby2, is(false));
  }

  @Test
  public void shouldFilterStandbyAlive() {
    // Given:
    allHostsStatus = ImmutableMap.of(
        activeHost, HOST_DEAD,
        standByHost1, HOST_ALIVE,
        standByHost2, HOST_DEAD
    );
    when(heartbeatAgent.getHostsStatus()).thenReturn(allHostsStatus);

    // When:
    final boolean filterActive = livenessFilter.filter(activeHost);
    final boolean filterStandby1 = livenessFilter.filter(standByHost1);
    final boolean filterStandby2 = livenessFilter.filter(standByHost2);

    // Then:
    assertThat(filterActive, is(false));
    assertThat(filterStandby1, is(true));
    assertThat(filterStandby2, is(false));
  }

  @Test
  public void shouldFilterAllAlive() {
    // Given:
    allHostsStatus = ImmutableMap.of(
        activeHost, HOST_ALIVE,
        standByHost1, HOST_ALIVE,
        standByHost2, HOST_ALIVE
    );
    when(heartbeatAgent.getHostsStatus()).thenReturn(allHostsStatus);

    // When:
    final boolean filterActive = livenessFilter.filter(activeHost);
    final boolean filterStandby1 = livenessFilter.filter(standByHost1);
    final boolean filterStandby2 = livenessFilter.filter(standByHost2);

    // Then:
    assertThat(filterActive, is(true));
    assertThat(filterStandby1, is(true));
    assertThat(filterStandby2, is(true));
  }

  @Test
  public void shouldFilterAllDead() {
    // Given:
    allHostsStatus = ImmutableMap.of(
        activeHost, HOST_DEAD,
        standByHost1, HOST_DEAD,
        standByHost2, HOST_DEAD
    );
    when(heartbeatAgent.getHostsStatus()).thenReturn(allHostsStatus);

    // When:
    final boolean filterActive = livenessFilter.filter(activeHost);
    final boolean filterStandby1 = livenessFilter.filter(standByHost1);
    final boolean filterStandby2 = livenessFilter.filter(standByHost2);

    // Then:
    assertThat(filterActive, is(false));
    assertThat(filterStandby1, is(false));
    assertThat(filterStandby2, is(false));
  }

}
