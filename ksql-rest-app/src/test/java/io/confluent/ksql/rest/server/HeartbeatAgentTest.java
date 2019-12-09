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
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.rest.entity.HostInfoEntity;
import io.confluent.ksql.rest.entity.HostStatusEntity;
import io.confluent.ksql.rest.server.HeartbeatAgent.Builder;
import io.confluent.ksql.rest.server.HeartbeatAgent.CheckHeartbeatService;
import io.confluent.ksql.rest.server.HeartbeatAgent.DiscoverClusterService;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
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

  private HeartbeatAgent heartbeatAgent;
  private HostInfo localHostInfo;
  private HostInfo remoteHostInfo;
  private List<StreamsMetadata> allMetadata0;
  private List<StreamsMetadata> allMetadata1;
  private static final String LOCALHOST_URL = "http://localhost:8088";

  @Before
  public void setUp() {
    localHostInfo = new HostInfo ("localhost", 8088);
    remoteHostInfo = new HostInfo("localhost", 8089);

    Builder builder = HeartbeatAgent.builder();
    heartbeatAgent = builder
        .heartbeatSendInterval(1)
        .heartbeatMissedThreshold(2)
        .build(ksqlEngine, serviceContext);
    heartbeatAgent.setLocalAddress(LOCALHOST_URL);
    Map<String, HostStatusEntity> hostsStatus = new ConcurrentHashMap<>();
    hostsStatus.put(localHostInfo.toString(), new HostStatusEntity(
        new HostInfoEntity(localHostInfo.host(), localHostInfo.port()), true, 0L));
    hostsStatus.put(remoteHostInfo.toString(), new HostStatusEntity(
        new HostInfoEntity(remoteHostInfo.host(), remoteHostInfo.port()), true, 0L));
    heartbeatAgent.setHostsStatus(hostsStatus);
    allMetadata0 = ImmutableList.of(streamsMetadata0);
    allMetadata1 = ImmutableList.of(streamsMetadata1);
  }

  @Test
  public void shouldDiscoverServersInCluster() {
    // Given:
    when(ksqlEngine.getPersistentQueries()).thenReturn(ImmutableList.of(query0, query1));

    when(query0.getAllMetadata()).thenReturn(allMetadata0);
    when(streamsMetadata0.hostInfo()).thenReturn(localHostInfo);

    when(query1.getAllMetadata()).thenReturn(allMetadata1);
    when(streamsMetadata1.hostInfo()).thenReturn(remoteHostInfo);

    DiscoverClusterService discoverService = heartbeatAgent.new DiscoverClusterService();

    // When:
    discoverService.runOneIteration();

    // Then:
    assertThat(heartbeatAgent.getHostsStatus().keySet().contains(remoteHostInfo.toString()), is(true));
  }

  @Test
  public void shouldMarkServerAsUpNoMissingHeartbeat() {
    // Given:
    long windowStart = 0;
    long windowEnd = 5;
    heartbeatAgent.receiveHeartbeat(remoteHostInfo, 0L);
    heartbeatAgent.receiveHeartbeat(remoteHostInfo, 1L);
    heartbeatAgent.receiveHeartbeat(remoteHostInfo, 2L);
    heartbeatAgent.receiveHeartbeat(remoteHostInfo, 3L);
    heartbeatAgent.receiveHeartbeat(remoteHostInfo, 4L);
    heartbeatAgent.receiveHeartbeat(remoteHostInfo, 5L);
    CheckHeartbeatService processService = heartbeatAgent.new CheckHeartbeatService();

    // When:
    processService.runWithWindow(windowStart, windowEnd);

    // Then:
    assertThat(heartbeatAgent.getHostsStatus().entrySet(), hasSize(2));
    assertThat(heartbeatAgent.getHostsStatus().get(remoteHostInfo.toString()).getHostAlive(), is(true));
  }

  @Test
  public void shouldMarkServerAsUpMissOneHeartbeat() {
    // Given:
    long windowStart = 1;
    long windowEnd = 10;
    heartbeatAgent.receiveHeartbeat(remoteHostInfo, 0L);
    heartbeatAgent.receiveHeartbeat(remoteHostInfo, 2L);
    heartbeatAgent.receiveHeartbeat(remoteHostInfo, 4L);
    heartbeatAgent.receiveHeartbeat(remoteHostInfo, 6L);
    heartbeatAgent.receiveHeartbeat(remoteHostInfo, 8L);
    heartbeatAgent.receiveHeartbeat(remoteHostInfo, 10L);
    CheckHeartbeatService processService = heartbeatAgent.new CheckHeartbeatService();

    // When:
    processService.runWithWindow(windowStart, windowEnd);

    // Then:
    assertThat(heartbeatAgent.getHostsStatus().entrySet(), hasSize(2));
    assertThat(heartbeatAgent.getHostsStatus().get(remoteHostInfo.toString()).getHostAlive(), is(true));
  }

  @Test
  public void shouldMarkServerAsUpMissAtBeginning() {
    // Given:
    long windowStart = 0;
    long windowEnd = 10;
    heartbeatAgent.receiveHeartbeat(remoteHostInfo, 8L);
    CheckHeartbeatService processService = heartbeatAgent.new CheckHeartbeatService();

    // When:
    processService.runWithWindow(windowStart, windowEnd);

    // Then:
    assertThat(heartbeatAgent.getHostsStatus().entrySet(), hasSize(2));
    assertThat(heartbeatAgent.getHostsStatus().get(remoteHostInfo.toString()).getHostAlive(), is(true));
  }

  @Test
  public void shouldMarkServerAsUpMissInterleaved() {
    // Given:
    long windowStart = 0;
    long windowEnd = 10;
    heartbeatAgent.receiveHeartbeat(remoteHostInfo, 0L);
    heartbeatAgent.receiveHeartbeat(remoteHostInfo, 2L);
    heartbeatAgent.receiveHeartbeat(remoteHostInfo, 5L);
    heartbeatAgent.receiveHeartbeat(remoteHostInfo, 8L);
    CheckHeartbeatService processService = heartbeatAgent.new CheckHeartbeatService();

    // When:
    processService.runWithWindow(windowStart, windowEnd);

    // Then:
    assertThat(heartbeatAgent.getHostsStatus().entrySet(), hasSize(2));
    assertThat(heartbeatAgent.getHostsStatus().get(remoteHostInfo.toString()).getHostAlive(), is(true));
  }

  @Test
  public void shouldMarkServerAsUpOutOfOrderHeartbeats() {
    // Given:
    long windowStart = 0;
    long windowEnd = 10;
    heartbeatAgent.receiveHeartbeat(remoteHostInfo, 8L);
    heartbeatAgent.receiveHeartbeat(remoteHostInfo, 0L);
    heartbeatAgent.receiveHeartbeat(remoteHostInfo, 5L);
    heartbeatAgent.receiveHeartbeat(remoteHostInfo, 2L);
    CheckHeartbeatService processService = heartbeatAgent.new CheckHeartbeatService();

    // When:
    processService.runWithWindow(windowStart, windowEnd);

    // Then:
    assertThat(heartbeatAgent.getHostsStatus().entrySet(), hasSize(2));
    assertThat(heartbeatAgent.getHostsStatus().get(remoteHostInfo.toString()).getHostAlive(), is(true));
  }

  @Test
  public void shouldMarkServerAsDownMissAtEnd() {
    // Given:
    long windowStart = 0;
    long windowEnd = 10;
    heartbeatAgent.receiveHeartbeat(remoteHostInfo, 0L);
    heartbeatAgent.receiveHeartbeat(remoteHostInfo, 2L);
    heartbeatAgent.receiveHeartbeat(remoteHostInfo, 4L);
    heartbeatAgent.receiveHeartbeat(remoteHostInfo, 6L);
    heartbeatAgent.receiveHeartbeat(remoteHostInfo, 7L);
    CheckHeartbeatService processService = heartbeatAgent.new CheckHeartbeatService();

    // When:
    processService.runWithWindow(windowStart, windowEnd);

    // Then:
    assertThat(heartbeatAgent.getHostsStatus().entrySet(), hasSize(2));
    assertThat(heartbeatAgent.getHostsStatus().get(remoteHostInfo.toString()).getHostAlive(), is(false));
  }

  @Test
  public void shouldMarkServerAsDownIgnoreHeartbeatsOutOfWindow() {
    // Given:
    long windowStart = 5;
    long windowEnd = 8;
    heartbeatAgent.receiveHeartbeat(remoteHostInfo, 0L);
    heartbeatAgent.receiveHeartbeat(remoteHostInfo, 1L);
    heartbeatAgent.receiveHeartbeat(remoteHostInfo, 2L);
    heartbeatAgent.receiveHeartbeat(remoteHostInfo, 3L);
    heartbeatAgent.receiveHeartbeat(remoteHostInfo, 4L);
    heartbeatAgent.receiveHeartbeat(remoteHostInfo, 9L);
    heartbeatAgent.receiveHeartbeat(remoteHostInfo, 10L);
    CheckHeartbeatService processService = heartbeatAgent.new CheckHeartbeatService();

    // When:
    processService.runWithWindow(windowStart, windowEnd);

    // Then:
    assertThat(heartbeatAgent.getHostsStatus().entrySet(), hasSize(2));
    assertThat(heartbeatAgent.getHostsStatus().get(remoteHostInfo.toString()).getHostAlive(), is(false));
  }

  @Test
  public void shouldMarkServerAsDownOutOfOrderHeartbeats() {
    // Given:
    long windowStart = 5;
    long windowEnd = 8;
    heartbeatAgent.receiveHeartbeat(remoteHostInfo, 10L);
    heartbeatAgent.receiveHeartbeat(remoteHostInfo, 9L);
    heartbeatAgent.receiveHeartbeat(remoteHostInfo, 0L);
    heartbeatAgent.receiveHeartbeat(remoteHostInfo, 4L);
    heartbeatAgent.receiveHeartbeat(remoteHostInfo, 2L);
    heartbeatAgent.receiveHeartbeat(remoteHostInfo, 3L);
    heartbeatAgent.receiveHeartbeat(remoteHostInfo, 1L);
    CheckHeartbeatService processService = heartbeatAgent.new CheckHeartbeatService();

    // When:
    processService.runWithWindow(windowStart, windowEnd);

    // Then:
    assertThat(heartbeatAgent.getHostsStatus().entrySet(), hasSize(2));
    assertThat(heartbeatAgent.getHostsStatus().get(remoteHostInfo.toString()).getHostAlive(), is(false));
  }

}
