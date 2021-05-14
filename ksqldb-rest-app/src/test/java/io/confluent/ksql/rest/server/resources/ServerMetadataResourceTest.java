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

package io.confluent.ksql.rest.server.resources;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.entity.ServerClusterId;
import io.confluent.ksql.rest.entity.ServerMetadata;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.AppInfo;
import io.confluent.ksql.util.KsqlConfig;
import java.util.concurrent.ExecutionException;
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

public class ServerMetadataResourceTest {
  private static final String KSQL_SERVICE_ID = "ksql.foo";
  private static final String KAFKA_CLUSTER_ID = "kafka.bar";

  @Mock
  private ServiceContext serviceContext;
  @Mock
  private AdminClient adminClient;
  @Mock
  private DescribeClusterResult describeClusterResult;
  @Mock
  private KafkaFuture<String> future;

  private ServerMetadataResource serverMetadataResource;

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

    serverMetadataResource = ServerMetadataResource.create(serviceContext, ksqlConfig);
  }

  @Test
  public void shouldReturnServerMetadata() {
    // When:
    final EndpointResponse response = serverMetadataResource.getServerMetadata();

    // Then:
    assertThat(response.getStatus(), equalTo(200));
    assertThat(response.getEntity(), instanceOf(ServerMetadata.class));
    final ServerMetadata serverMetadata = (ServerMetadata)response.getEntity();
    assertThat(
        serverMetadata,
        equalTo(new ServerMetadata(
            AppInfo.getVersion(),
            ServerClusterId.of(KAFKA_CLUSTER_ID, KSQL_SERVICE_ID))
        )
    );
  }

  @Test
  public void shouldReturnServerClusterId() {
    // When:
    final EndpointResponse response = serverMetadataResource.getServerClusterId();

    // Then:
    assertThat(response.getStatus(), equalTo(200));
    assertThat(response.getEntity(), instanceOf(ServerClusterId.class));
    final ServerClusterId serverClusterId = (ServerClusterId)response.getEntity();
    assertThat(
        serverClusterId,
        equalTo(ServerClusterId.of(KAFKA_CLUSTER_ID, KSQL_SERVICE_ID))
    );
  }
}
