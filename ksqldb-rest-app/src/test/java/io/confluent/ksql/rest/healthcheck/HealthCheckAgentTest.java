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

package io.confluent.ksql.rest.healthcheck;

import static io.confluent.ksql.rest.healthcheck.HealthCheckAgent.KAFKA_CHECK_NAME;
import static io.confluent.ksql.rest.healthcheck.HealthCheckAgent.METASTORE_CHECK_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.exception.KafkaResponseGetFailedException;
import io.confluent.ksql.exception.KsqlTopicAuthorizationException;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.HealthCheckResponse;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.SimpleKsqlClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.rest.RestConfig;
import java.net.URI;
import java.net.URL;
import java.util.Collections;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class HealthCheckAgentTest {

  private static final String SERVER_ADDRESS = "http://serverhost:8088";
  private static URI SERVER_URI;
  static {
    try {
      SERVER_URI = new URL(SERVER_ADDRESS).toURI();
    } catch (final Exception e) {
      fail("failed to create server URI");
    }
  }

  @Mock
  private SimpleKsqlClient ksqlClient;
  @Mock
  private KsqlRestConfig restConfig;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private Admin adminClient;
  @Mock
  private RestResponse<KsqlEntityList> successfulResponse;
  @Mock
  private RestResponse<KsqlEntityList> unSuccessfulResponse;
  private HealthCheckAgent healthCheckAgent;

  @Before
  public void setUp() {
    when(ksqlClient.makeKsqlRequest(eq(SERVER_URI), any())).thenReturn(successfulResponse);
    when(restConfig.getList(RestConfig.LISTENERS_CONFIG))
        .thenReturn(ImmutableList.of(SERVER_ADDRESS));
    when(successfulResponse.isSuccessful()).thenReturn(true);
    when(unSuccessfulResponse.isSuccessful()).thenReturn(false);
    when(serviceContext.getAdminClient()).thenReturn(adminClient);

    final DescribeTopicsResult topicsResult = mock(DescribeTopicsResult.class);
    when(adminClient.describeTopics(any(), any())).thenReturn(topicsResult);
    when(topicsResult.all()).thenReturn(KafkaFuture.completedFuture(Collections.emptyMap()));

    final KsqlConfig ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_SERVICE_ID_CONFIG,
        "default_"
    ));

    healthCheckAgent = new HealthCheckAgent(ksqlClient, restConfig, serviceContext, ksqlConfig);
  }

  @Test
  public void shouldCheckHealth() {
    // When:
    final HealthCheckResponse response = healthCheckAgent.checkHealth();

    // Then:
    verify(ksqlClient, atLeastOnce()).makeKsqlRequest(eq(SERVER_URI), any());
    assertThat(response.getDetails().get(METASTORE_CHECK_NAME).getIsHealthy(), is(true));
    assertThat(response.getDetails().get(KAFKA_CHECK_NAME).getIsHealthy(), is(true));
    assertThat(response.getIsHealthy(), is(true));
  }

  @Test
  public void shouldReturnUnhealthyIfMetastoreCheckFails() {
    // Given:
    when(ksqlClient.makeKsqlRequest(SERVER_URI, "list streams; list tables; list queries;"))
        .thenReturn(unSuccessfulResponse);

    // When:
    final HealthCheckResponse response = healthCheckAgent.checkHealth();

    // Then:
    assertThat(response.getDetails().get(METASTORE_CHECK_NAME).getIsHealthy(), is(false));
    assertThat(response.getIsHealthy(), is(false));
  }

  @Test
  public void shouldReturnUnhealthyIfKafkaCheckFails() {
    // Given:
    doThrow(KafkaResponseGetFailedException.class).when(adminClient).describeTopics(any(), any());

    // When:
    final HealthCheckResponse response = healthCheckAgent.checkHealth();

    // Then:
    assertThat(response.getDetails().get(KAFKA_CHECK_NAME).getIsHealthy(), is(false));
    assertThat(response.getIsHealthy(), is(false));
  }

  @Test
  public void shouldReturnHealthyIfKafkaCheckFailsWithAuthorizationException() {
    // Given:
    doThrow(KsqlTopicAuthorizationException.class).when(adminClient).describeTopics(any(), any());

    // When:
    final HealthCheckResponse response = healthCheckAgent.checkHealth();

    // Then:
    assertThat(response.getDetails().get(KAFKA_CHECK_NAME).getIsHealthy(), is(true));
    assertThat(response.getIsHealthy(), is(true));
  }
}