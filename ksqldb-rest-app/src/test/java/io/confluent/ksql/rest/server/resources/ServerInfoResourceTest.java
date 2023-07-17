/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.rest.server.resources;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.entity.ServerInfo;
import io.confluent.ksql.rest.server.computation.CommandRunner;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.AppInfo;
import io.confluent.ksql.util.KsqlConfig;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.KafkaFuture;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class ServerInfoResourceTest {
  private static final String KSQL_SERVICE_ID = "ksql.foo";
  private static final String KAFKA_CLUSTER_ID = "kafka.bar";

  @Mock
  private ServiceContext serviceContext;
  @Mock
  private AdminClient adminClient;
  @Mock
  private DescribeClusterResult describeClusterResult;
  @Mock
  private CommandRunner commandRunner;
  @Mock
  private KafkaFuture<String> future;

  private ServerInfoResource serverInfoResource = null;

  private final KsqlConfig ksqlConfig = new KsqlConfig(
      ImmutableMap.of(
        KsqlConfig.KSQL_SERVICE_ID_CONFIG, KSQL_SERVICE_ID
      )
  );

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  public void setup() throws InterruptedException, TimeoutException, ExecutionException {
    when(serviceContext.getAdminClient()).thenReturn(adminClient);
    when(adminClient.describeCluster()).thenReturn(describeClusterResult);
    when(describeClusterResult.clusterId()).thenReturn(future);
    when(future.get(anyLong(), any())).thenReturn(KAFKA_CLUSTER_ID);
    when(commandRunner.checkCommandRunnerStatus()).thenReturn(CommandRunner.CommandRunnerStatus.RUNNING);

    serverInfoResource = new ServerInfoResource(serviceContext, ksqlConfig, commandRunner);
  }

  @Test
  public void shouldReturnServerInfo() {
    // When:
    final EndpointResponse response = serverInfoResource.get();

    // Then:
    assertThat(response.getStatus(), equalTo(200));
    assertThat(response.getEntity(), instanceOf(ServerInfo.class));
    final ServerInfo serverInfo = (ServerInfo)response.getEntity();
    assertThat(
        serverInfo,
        equalTo(new ServerInfo(AppInfo.getVersion(), KAFKA_CLUSTER_ID, KSQL_SERVICE_ID, "RUNNING"))
    );
  }

  @Test
  public void shouldGetKafkaClusterIdWithTimeout()
      throws InterruptedException, ExecutionException, TimeoutException{
    // When:
    serverInfoResource.get();

    // Then:
    verify(future).get(30, TimeUnit.SECONDS);
  }

  @Test
  public void shouldGetKafkaClusterIdOnce() {
    // When:
    serverInfoResource.get();
    serverInfoResource.get();

    // Then:
    verify(adminClient).describeCluster();
    verifyNoMoreInteractions(adminClient);
  }
}