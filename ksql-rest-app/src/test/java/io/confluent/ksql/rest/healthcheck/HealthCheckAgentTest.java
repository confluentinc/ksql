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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.HealthCheckResponse;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.services.SimpleKsqlClient;
import io.confluent.rest.RestConfig;
import java.net.URI;
import java.net.URL;
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
    } catch (Exception e) {
      fail("failed to create server URI");
    }
  }

  @Mock
  private SimpleKsqlClient ksqlClient;
  @Mock
  private KsqlRestConfig restConfig;
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

    healthCheckAgent = new HealthCheckAgent(ksqlClient, restConfig);
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
    when(ksqlClient.makeKsqlRequest(SERVER_URI, "list topics extended;"))
        .thenReturn(unSuccessfulResponse);

    // When:
    final HealthCheckResponse response = healthCheckAgent.checkHealth();

    // Then:
    assertThat(response.getDetails().get(KAFKA_CHECK_NAME).getIsHealthy(), is(false));
    assertThat(response.getIsHealthy(), is(false));
  }
}