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

import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.entity.HealthCheckResponse;
import io.confluent.ksql.rest.healthcheck.HealthCheckAgent;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class HealthCheckResourceTest {

  @Mock
  private HealthCheckAgent healthCheckAgent;
  @Mock
  private HealthCheckResponse response1;
  @Mock
  private HealthCheckResponse response2;
  private HealthCheckResource healthCheckResource;

  @Before
  public void setUp() {
    when(healthCheckAgent.checkHealth())
        .thenReturn(response1)
        .thenReturn(response2);

    healthCheckResource = new HealthCheckResource(healthCheckAgent, Duration.ofMillis(1000));
  }

  @Test
  public void shouldCheckHealth() {
    // When:
    final EndpointResponse response = healthCheckResource.checkHealth();

    // Then:
    verify(healthCheckAgent).checkHealth();
    assertThat(response.getStatus(), is(200));
    assertThat(response.getEntity(), instanceOf(HealthCheckResponse.class));
  }

  @Test
  public void shouldGetCachedResponse() {
    // Given:
    healthCheckResource.checkHealth();

    // When:
    final EndpointResponse response = healthCheckResource.checkHealth();

    // Then:
    assertThat(response.getEntity(), sameInstance(response1));
  }

  @Test
  public void shouldRecheckHealthIfCachedResponseExpired() throws Exception {
    // Given:
    healthCheckResource = new HealthCheckResource(healthCheckAgent, Duration.ofMillis(10));
    healthCheckResource.checkHealth();

    // When / Then:
    assertThatEventually(
        "Should receive response2 once response1 expires.",
        () -> healthCheckResource.checkHealth().getEntity(),
        is(response2),
        1000,
        TimeUnit.MILLISECONDS
    );
  }
}