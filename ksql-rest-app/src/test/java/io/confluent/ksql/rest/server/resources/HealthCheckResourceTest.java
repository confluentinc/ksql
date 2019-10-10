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
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.rest.entity.HealthCheckResponse;
import io.confluent.ksql.rest.healthcheck.HealthCheckAgent;
import java.time.Duration;
import java.util.function.Supplier;
import javax.ws.rs.core.Response;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class HealthCheckResourceTest {

  private static final Duration HEALTHCHECK_INTERVAL = Duration.ofMillis(15);

  @Mock
  private HealthCheckAgent healthCheckAgent;
  @Mock
  private Supplier<Long> currentTimeSupplier;
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
    when(currentTimeSupplier.get()).thenReturn(1000L);

    healthCheckResource = new HealthCheckResource(
        healthCheckAgent,
        HEALTHCHECK_INTERVAL,
        currentTimeSupplier
    );
  }

  @Test
  public void shouldCheckHealth() {
    // When:
    final Response response = healthCheckResource.checkHealth();

    // Then:
    verify(healthCheckAgent).checkHealth();
    assertThat(response.getStatus(), is(200));
    assertThat(response.getEntity(), instanceOf(HealthCheckResponse.class));
  }

  @Test
  public void shouldGetCachedResponse() {
    // Given:
    when(currentTimeSupplier.get())
        .thenReturn(1000L)  // time when first response is cached
        .thenReturn(1010L); // time of second checkHealth()
    healthCheckResource.checkHealth();

    // When:
    final Response response = healthCheckResource.checkHealth();

    // Then:
    assertThat(response.getEntity(), sameInstance(response1));
  }

  @Test
  public void shouldRecheckHealthIfCachedResponseExpired() {
    // Given:
    when(currentTimeSupplier.get())
        .thenReturn(1000L)  // time when first response is cached
        .thenReturn(1020L)  // time of second checkHealth()
        .thenReturn(1020L); // time when second response is cached
    healthCheckResource.checkHealth();

    // When:
    final Response response = healthCheckResource.checkHealth();

    // Then:
    assertThat(response.getEntity(), sameInstance(response2));
  }
}