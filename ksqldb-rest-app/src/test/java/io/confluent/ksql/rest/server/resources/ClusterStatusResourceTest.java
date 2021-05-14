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

package io.confluent.ksql.rest.server.resources;

import static io.confluent.ksql.rest.server.resources.ClusterStatusResource.EMPTY_HOST_STORE_LAGS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.entity.ClusterStatusResponse;
import io.confluent.ksql.rest.entity.HostStoreLags;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.LagInfoEntity;
import io.confluent.ksql.rest.entity.QueryStateStoreId;
import io.confluent.ksql.rest.entity.StateStoreLags;
import io.confluent.ksql.rest.server.HeartbeatAgent;
import io.confluent.ksql.rest.server.LagReportingAgent;
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
public class ClusterStatusResourceTest {

  private static final HostStoreLags HOST_STORE_LAGS = new HostStoreLags(
      ImmutableMap.of(QueryStateStoreId.of("a", "b"), new StateStoreLags(
          ImmutableMap.of(1, new LagInfoEntity(2, 10, 8)))
      ), 20L);

  private static final KsqlHostInfo HOST1 = new KsqlHostInfo("host1", 8088);
  private static final KsqlHostInfo HOST2 = new KsqlHostInfo("host2", 8089);
  private static final KsqlHostInfoEntity HOST1_ENTITY = new KsqlHostInfoEntity("host1", 8088);
  private static final KsqlHostInfoEntity HOST2_ENTITY = new KsqlHostInfoEntity("host2", 8089);

  private static final Map<KsqlHostInfo, HostStatus> HOSTS = ImmutableMap.of(
      HOST1, new HostStatus(true, 30L),
      HOST2, new HostStatus(false, 30L)
  );

  @Mock
  private HeartbeatAgent heartbeatAgent;
  @Mock
  private LagReportingAgent lagReportingAgent;
  @Mock
  private KsqlEngine ksqlEngine;

  private ClusterStatusResource clusterStatusResource;

  @Before
  public void setUp() {
    clusterStatusResource = new ClusterStatusResource(
        ksqlEngine,
        heartbeatAgent,
        Optional.of(lagReportingAgent));
  }

  @Test
  public void shouldReturnClusterStatus() {
    // Given:
    when(heartbeatAgent.getHostsStatus()).thenReturn(HOSTS);
    when(lagReportingAgent.getLagPerHost(any())).thenReturn(Optional.empty());

    // When:
    final EndpointResponse response = clusterStatusResource.checkClusterStatus();

    // Then:
    assertThat(response.getStatus(), is(200));
    assertThat(response.getEntity(), instanceOf(ClusterStatusResponse.class));

    ClusterStatusResponse clusterStatusResponse = (ClusterStatusResponse) response.getEntity();
    assertTrue(clusterStatusResponse.getClusterStatus().get(HOST1_ENTITY).getHostAlive());
    assertFalse(clusterStatusResponse.getClusterStatus().get(HOST2_ENTITY).getHostAlive());
  }

  @Test
  public void shouldReturnEmptyLagsForDeadHost() {
    // Given:
    when(heartbeatAgent.getHostsStatus()).thenReturn(HOSTS);
    when(lagReportingAgent.getLagPerHost(any())).thenReturn(Optional.of(HOST_STORE_LAGS));

    // When:
    final EndpointResponse response = clusterStatusResource.checkClusterStatus();

    // Then:
    assertThat(response.getStatus(), is(200));
    assertThat(response.getEntity(), instanceOf(ClusterStatusResponse.class));

    ClusterStatusResponse clusterStatusResponse = (ClusterStatusResponse) response.getEntity();
    assertEquals(HOST_STORE_LAGS,
        clusterStatusResponse.getClusterStatus().get(HOST1_ENTITY).getHostStoreLags());
    assertEquals(EMPTY_HOST_STORE_LAGS,
        clusterStatusResponse.getClusterStatus().get(HOST2_ENTITY).getHostStoreLags());
  }
}
