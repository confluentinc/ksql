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

import static io.confluent.ksql.rest.healthcheck.HealthCheckAgent.COMMAND_RUNNER_CHECK_NAME;
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
import io.confluent.ksql.rest.server.computation.CommandRunner;
import io.confluent.ksql.rest.server.state.ServerState.State;
import io.confluent.ksql.services.SimpleKsqlClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlRequestConfig;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class HealthCheckAgentTest {

  private static final String SERVER_ADDRESS = "http://serverhost:8088";
  private static final Map<String, Object> REQUEST_PROPERTIES =
      ImmutableMap.of(KsqlRequestConfig.KSQL_REQUEST_INTERNAL_REQUEST, true);
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
  private Admin internalAdminClient;
  @Mock
  private CommandRunner commandRunner;
  @Mock
  private RestResponse<KsqlEntityList> successfulResponse;
  @Mock
  private RestResponse<KsqlEntityList> unSuccessfulResponse;
  private HealthCheckAgent healthCheckAgent;

  @Before
  public void setUp() {
    when(ksqlClient.makeKsqlRequest(eq(SERVER_URI), any(), eq(REQUEST_PROPERTIES))).thenReturn(successfulResponse);
    when(restConfig.getList(KsqlRestConfig.LISTENERS_CONFIG))
        .thenReturn(ImmutableList.of(SERVER_ADDRESS));
    when(successfulResponse.isSuccessful()).thenReturn(true);
    when(unSuccessfulResponse.isSuccessful()).thenReturn(false);

    final DescribeTopicsResult topicsResult = mock(DescribeTopicsResult.class);
    givenDescribeTopicsReturns(topicsResult);
    when(topicsResult.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Collections.emptyMap()));
    when(commandRunner.checkCommandRunnerStatus()).thenReturn(CommandRunner.CommandRunnerStatus.RUNNING);
    when(commandRunner.checkServerState()).thenReturn(State.READY);
    final KsqlConfig ksqlConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_SERVICE_ID_CONFIG,
        "default_"
    ));

    healthCheckAgent = new HealthCheckAgent(ksqlClient, restConfig, ksqlConfig, commandRunner, internalAdminClient);
  }

  @Test
  public void shouldCheckHealth() {
    // When:
    final HealthCheckResponse response = healthCheckAgent.checkHealth();

    // Then:
    verify(ksqlClient, atLeastOnce()).makeKsqlRequest(eq(SERVER_URI), any(), eq(REQUEST_PROPERTIES));
    assertThat(response.getDetails().get(METASTORE_CHECK_NAME).getIsHealthy(), is(true));
    assertThat(response.getDetails().get(KAFKA_CHECK_NAME).getIsHealthy(), is(true));
    assertThat(response.getDetails().get(COMMAND_RUNNER_CHECK_NAME).getIsHealthy(), is(true));
    assertThat(response.getIsHealthy(), is(true));
  }

  @Test
  public void shouldReturnUnhealthyIfMetastoreCheckFails() {
    // Given:
    when(ksqlClient.makeKsqlRequest(SERVER_URI, "list streams; list tables; list queries;", REQUEST_PROPERTIES))
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
    givenDescribeTopicsThrows(KafkaResponseGetFailedException.class);

    // When:
    final HealthCheckResponse response = healthCheckAgent.checkHealth();

    // Then:
    assertThat(response.getDetails().get(KAFKA_CHECK_NAME).getIsHealthy(), is(false));
    assertThat(response.getIsHealthy(), is(false));
  }

  @Test
  public void shouldReturnUnhealthyIfCommandRunnerCheckFails() {
    // Given:
    when(commandRunner.checkCommandRunnerStatus()).thenReturn(CommandRunner.CommandRunnerStatus.DEGRADED);

    // When:
    final HealthCheckResponse response = healthCheckAgent.checkHealth();

    // Then:
    assertThat(response.getDetails().get(COMMAND_RUNNER_CHECK_NAME).getIsHealthy(), is(false));
    assertThat(response.getIsHealthy(), is(false));
  }

  @Test
  public void shouldReturnHealthyIfKafkaCheckFailsWithAuthorizationException() {
    // Given:
    givenDescribeTopicsThrows(KsqlTopicAuthorizationException.class);

    // When:
    final HealthCheckResponse response = healthCheckAgent.checkHealth();

    // Then:
    assertThat(response.getDetails().get(KAFKA_CHECK_NAME).getIsHealthy(), is(true));
    assertThat(response.getIsHealthy(), is(true));
  }

  @Test
  public void shouldReturnHealthyIfKafkaCheckFailsWithUnknownTopicOrPartitionExceptionAsRootCause() {
    // Given:
    givenDescribeTopicsThrows(new RuntimeException(new UnknownTopicOrPartitionException()));

    // When:
    final HealthCheckResponse response = healthCheckAgent.checkHealth();

    // Then:
    assertThat(response.getDetails().get(KAFKA_CHECK_NAME).getIsHealthy(), is(true));
    assertThat(response.getIsHealthy(), is(true));
  }

  @Test
  public void shouldReturnHealthyIfKafkaCheckFailsWithUnknownTopicOrPartitionException() {
    // Given:
    givenDescribeTopicsThrows(new UnknownTopicOrPartitionException());

    // When:
    final HealthCheckResponse response = healthCheckAgent.checkHealth();

    // Then:
    assertThat(response.getDetails().get(KAFKA_CHECK_NAME).getIsHealthy(), is(true));
    assertThat(response.getIsHealthy(), is(true));
  }

  // isolate suppressed calls to their own methods
  @SuppressWarnings("unchecked")
  private void givenDescribeTopicsReturns(final DescribeTopicsResult topicsResult) {
    when(internalAdminClient.describeTopics(any(Collection.class), any())).thenReturn(topicsResult);
  }

  @SuppressWarnings("unchecked")
  private void givenDescribeTopicsThrows(final Class<? extends Throwable> clazz) {
    doThrow(clazz).when(internalAdminClient).describeTopics(any(Collection.class), any());
  }

  @SuppressWarnings("unchecked")
  private void givenDescribeTopicsThrows(final Throwable t) {
    doThrow(t).when(internalAdminClient).describeTopics(any(Collection.class), any());
  }
}