/*
 * Copyright 2019 Confluent Inc.
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
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.rest.server.HeartbeatAgent.Builder;
import io.confluent.ksql.rest.server.HeartbeatAgent.CheckHeartbeatService;
import io.confluent.ksql.rest.server.HeartbeatAgent.DiscoverClusterService;
import io.confluent.ksql.rest.server.HeartbeatAgent.HostStatusListener;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.HostStatus;
import io.confluent.ksql.util.KsqlHostInfo;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.util.Map;
import org.apache.kafka.streams.StreamsMetadata;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class HeartbeatAgentTest {
  @Mock
  private PersistentQueryMetadata query0;
  @Mock
  private PersistentQueryMetadata query1;
  @Mock
  private StreamsMetadata streamsMetadata0;
  @Mock
  private StreamsMetadata streamsMetadata1;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private KsqlEngine ksqlEngine;
  @Mock
  private HostStatusListener hostStatusListener;

  private HeartbeatAgent heartbeatAgent;
  private KsqlHostInfo localHost;
  private KsqlHostInfo remoteHost;
  private Map<KsqlHostInfo, HostStatus> hostsStatus;
  private static final String LOCALHOST_URL = "http://localhost:8088";

  @Before
  public void setUp() {
    localHost = new KsqlHostInfo("localhost", 8088);
    remoteHost = new KsqlHostInfo("localhost", 8089);

    Builder builder = HeartbeatAgent.builder();
    heartbeatAgent = builder
        .heartbeatSendInterval(1)
        .heartbeatMissedThreshold(2)
        .addHostStatusListener(hostStatusListener)
        .build(ksqlEngine, serviceContext);
    heartbeatAgent.setLocalAddress(LOCALHOST_URL);
    hostsStatus = ImmutableMap
        .of(localHost, new HostStatus(true, 0L),
            remoteHost, new HostStatus(true, 0L));
  }

  @Test
  public void shouldDiscoverServersInCluster() {
    // Given:
    heartbeatAgent.setHostsStatus(hostsStatus);
    when(ksqlEngine.getPersistentQueries()).thenReturn(ImmutableList.of(query0, query1));

    DiscoverClusterService discoverService = heartbeatAgent.new DiscoverClusterService();

    // When:
    discoverService.runOneIteration();

    // Then:
    assertThat(heartbeatAgent.getHostsStatus(), hasKey(remoteHost));
  }

  @Test
  public void localhostOnlyShouldMarkServerAsUp() {
    // Given:
    long windowStart = 0;
    long windowEnd = 5;
    hostsStatus = ImmutableMap.of(localHost, new HostStatus(true, 0L));
    heartbeatAgent.setHostsStatus(hostsStatus);
    CheckHeartbeatService processService = heartbeatAgent.new CheckHeartbeatService();

    // When:
    processService.runWithWindow(windowStart, windowEnd);

    // Then:
    assertThat(heartbeatAgent.getHostsStatus().entrySet(), hasSize(1));
    assertThat(heartbeatAgent.getHostsStatus().get(localHost).isHostAlive(), is(true));
    verify(hostStatusListener).onHostStatusUpdated(Mockito.eq(heartbeatAgent.getHostsStatus()));
  }

  @Test
  public void shouldMarkServerAsUpNoMissingHeartbeat() {
    // Given:
    long windowStart = 0;
    long windowEnd = 5;
    heartbeatAgent.setHostsStatus(hostsStatus);
    heartbeatAgent.receiveHeartbeat(remoteHost, 0L);
    heartbeatAgent.receiveHeartbeat(remoteHost, 1L);
    heartbeatAgent.receiveHeartbeat(remoteHost, 2L);
    heartbeatAgent.receiveHeartbeat(remoteHost, 3L);
    heartbeatAgent.receiveHeartbeat(remoteHost, 4L);
    heartbeatAgent.receiveHeartbeat(remoteHost, 5L);
    CheckHeartbeatService processService = heartbeatAgent.new CheckHeartbeatService();

    // When:
    processService.runWithWindow(windowStart, windowEnd);

    // Then:
    assertThat(heartbeatAgent.getHostsStatus().entrySet(), hasSize(2));
    assertThat(heartbeatAgent.getHostsStatus().get(remoteHost).isHostAlive(), is(true));
    verify(hostStatusListener).onHostStatusUpdated(Mockito.eq(heartbeatAgent.getHostsStatus()));
  }

  @Test
  public void shouldMarkServerAsUpMissOneHeartbeat() {
    // Given:
    long windowStart = 1;
    long windowEnd = 10;
    heartbeatAgent.setHostsStatus(hostsStatus);
    heartbeatAgent.receiveHeartbeat(remoteHost, 0L);
    heartbeatAgent.receiveHeartbeat(remoteHost, 2L);
    heartbeatAgent.receiveHeartbeat(remoteHost, 4L);
    heartbeatAgent.receiveHeartbeat(remoteHost, 6L);
    heartbeatAgent.receiveHeartbeat(remoteHost, 8L);
    heartbeatAgent.receiveHeartbeat(remoteHost, 10L);
    CheckHeartbeatService processService = heartbeatAgent.new CheckHeartbeatService();

    // When:
    processService.runWithWindow(windowStart, windowEnd);

    // Then:
    assertThat(heartbeatAgent.getHostsStatus().entrySet(), hasSize(2));
    assertThat(heartbeatAgent.getHostsStatus().get(remoteHost).isHostAlive(), is(true));
    verify(hostStatusListener).onHostStatusUpdated(Mockito.eq(heartbeatAgent.getHostsStatus()));
  }

  @Test
  public void shouldMarkServerAsUpMissAtBeginning() {
    // Given:
    long windowStart = 0;
    long windowEnd = 10;
    heartbeatAgent.setHostsStatus(hostsStatus);
    heartbeatAgent.receiveHeartbeat(remoteHost, 8L);
    CheckHeartbeatService processService = heartbeatAgent.new CheckHeartbeatService();

    // When:
    processService.runWithWindow(windowStart, windowEnd);

    // Then:
    assertThat(heartbeatAgent.getHostsStatus().entrySet(), hasSize(2));
    assertThat(heartbeatAgent.getHostsStatus().get(remoteHost).isHostAlive(), is(true));
    verify(hostStatusListener).onHostStatusUpdated(Mockito.eq(heartbeatAgent.getHostsStatus()));
  }

  @Test
  public void shouldMarkServerAsUpMissInterleaved() {
    // Given:
    long windowStart = 0;
    long windowEnd = 10;
    heartbeatAgent.setHostsStatus(hostsStatus);
    heartbeatAgent.receiveHeartbeat(remoteHost, 0L);
    heartbeatAgent.receiveHeartbeat(remoteHost, 2L);
    heartbeatAgent.receiveHeartbeat(remoteHost, 5L);
    heartbeatAgent.receiveHeartbeat(remoteHost, 8L);
    CheckHeartbeatService processService = heartbeatAgent.new CheckHeartbeatService();

    // When:
    processService.runWithWindow(windowStart, windowEnd);

    // Then:
    assertThat(heartbeatAgent.getHostsStatus().entrySet(), hasSize(2));
    assertThat(heartbeatAgent.getHostsStatus().get(remoteHost).isHostAlive(), is(true));
    verify(hostStatusListener).onHostStatusUpdated(Mockito.eq(heartbeatAgent.getHostsStatus()));
  }

  @Test
  public void shouldMarkServerAsUpOutOfOrderHeartbeats() {
    // Given:
    long windowStart = 0;
    long windowEnd = 10;
    heartbeatAgent.setHostsStatus(hostsStatus);
    heartbeatAgent.receiveHeartbeat(remoteHost, 8L);
    heartbeatAgent.receiveHeartbeat(remoteHost, 0L);
    heartbeatAgent.receiveHeartbeat(remoteHost, 5L);
    heartbeatAgent.receiveHeartbeat(remoteHost, 2L);
    CheckHeartbeatService processService = heartbeatAgent.new CheckHeartbeatService();

    // When:
    processService.runWithWindow(windowStart, windowEnd);

    // Then:
    assertThat(heartbeatAgent.getHostsStatus().entrySet(), hasSize(2));
    assertThat(heartbeatAgent.getHostsStatus().get(remoteHost).isHostAlive(), is(true));
    verify(hostStatusListener).onHostStatusUpdated(Mockito.eq(heartbeatAgent.getHostsStatus()));
  }

  @Test
  public void shouldMarkServerAsDownMissAtEnd() {
    // Given:
    long windowStart = 0;
    long windowEnd = 10;
    heartbeatAgent.setHostsStatus(hostsStatus);
    heartbeatAgent.receiveHeartbeat(remoteHost, 0L);
    heartbeatAgent.receiveHeartbeat(remoteHost, 2L);
    heartbeatAgent.receiveHeartbeat(remoteHost, 4L);
    heartbeatAgent.receiveHeartbeat(remoteHost, 6L);
    heartbeatAgent.receiveHeartbeat(remoteHost, 7L);
    CheckHeartbeatService processService = heartbeatAgent.new CheckHeartbeatService();

    // When:
    processService.runWithWindow(windowStart, windowEnd);

    // Then:
    assertThat(heartbeatAgent.getHostsStatus().entrySet(), hasSize(2));
    assertThat(heartbeatAgent.getHostsStatus().get(remoteHost).isHostAlive(), is(false));
    verify(hostStatusListener).onHostStatusUpdated(Mockito.eq(heartbeatAgent.getHostsStatus()));
  }

  @Test
  public void shouldMarkServerAsDownIgnoreHeartbeatsOutOfWindow() {
    // Given:
    long windowStart = 5;
    long windowEnd = 8;
    heartbeatAgent.setHostsStatus(hostsStatus);
    heartbeatAgent.receiveHeartbeat(remoteHost, 0L);
    heartbeatAgent.receiveHeartbeat(remoteHost, 1L);
    heartbeatAgent.receiveHeartbeat(remoteHost, 2L);
    heartbeatAgent.receiveHeartbeat(remoteHost, 3L);
    heartbeatAgent.receiveHeartbeat(remoteHost, 4L);
    heartbeatAgent.receiveHeartbeat(remoteHost, 9L);
    heartbeatAgent.receiveHeartbeat(remoteHost, 10L);
    CheckHeartbeatService processService = heartbeatAgent.new CheckHeartbeatService();

    // When:
    processService.runWithWindow(windowStart, windowEnd);

    // Then:
    assertThat(heartbeatAgent.getHostsStatus().entrySet(), hasSize(2));
    assertThat(heartbeatAgent.getHostsStatus().get(remoteHost).isHostAlive(), is(false));
    verify(hostStatusListener).onHostStatusUpdated(Mockito.eq(heartbeatAgent.getHostsStatus()));
  }

  @Test
  public void shouldMarkServerAsDownOutOfOrderHeartbeats() {
    // Given:
    long windowStart = 5;
    long windowEnd = 8;
    heartbeatAgent.setHostsStatus(hostsStatus);
    heartbeatAgent.receiveHeartbeat(remoteHost, 10L);
    heartbeatAgent.receiveHeartbeat(remoteHost, 9L);
    heartbeatAgent.receiveHeartbeat(remoteHost, 0L);
    heartbeatAgent.receiveHeartbeat(remoteHost, 4L);
    heartbeatAgent.receiveHeartbeat(remoteHost, 2L);
    heartbeatAgent.receiveHeartbeat(remoteHost, 3L);
    heartbeatAgent.receiveHeartbeat(remoteHost, 1L);
    CheckHeartbeatService processService = heartbeatAgent.new CheckHeartbeatService();

    // When:
    processService.runWithWindow(windowStart, windowEnd);

    // Then:
    assertThat(heartbeatAgent.getHostsStatus().entrySet(), hasSize(2));
    assertThat(heartbeatAgent.getHostsStatus().get(remoteHost).isHostAlive(), is(false));
    verify(hostStatusListener).onHostStatusUpdated(Mockito.eq(heartbeatAgent.getHostsStatus()));
  }
}
