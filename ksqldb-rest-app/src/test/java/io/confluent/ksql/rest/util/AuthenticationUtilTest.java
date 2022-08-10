/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.rest.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

import io.confluent.ksql.security.KsqlAuthTokenProvider;
import io.confluent.ksql.util.KsqlConfig;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AuthenticationUtilTest {

  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private KsqlAuthTokenProvider authTokenProvider;
  private static final String TOKEN = "TOKEN";
  private final AuthenticationUtil authenticationUtil
      = new AuthenticationUtil(Clock.fixed(Instant.ofEpochMilli(0), ZoneId.of("UTC")));

  @Before
  public void init() {
    when(authTokenProvider.getLifetimeMs(TOKEN)).thenReturn(50000L);
    when(ksqlConfig.getLong(KsqlConfig.KSQL_WEBSOCKET_CONNECTION_MAX_TIMEOUT_MS)).thenReturn(60000L);
  }

  @Test
  public void shouldReturnEmptyWhenConfigSetToZero() {
    // Given:
    when(ksqlConfig.getLong(KsqlConfig.KSQL_WEBSOCKET_CONNECTION_MAX_TIMEOUT_MS)).thenReturn(0L);

    // Then:
    assertThat(authenticationUtil.getTokenTimeout(Optional.of(TOKEN), ksqlConfig, Optional.of(authTokenProvider)), equalTo(Optional.empty()));
  }

  @Test
  public void shouldReturnDefaultWhenNoTokenPresent() {
    assertThat(authenticationUtil.getTokenTimeout(Optional.empty(), ksqlConfig, Optional.of(authTokenProvider)), equalTo(Optional.of(60000L)));
  }

  @Test
  public void shouldReturnDefaultWhenNoAuthTokenProviderPresent() {
    assertThat(authenticationUtil.getTokenTimeout(Optional.of(TOKEN), ksqlConfig, Optional.empty()), equalTo(Optional.of(60000L)));
  }

  @Test
  public void shouldReturnDefaultWhenProviderThrows() {
    // Given:
    when(authTokenProvider.getLifetimeMs(TOKEN)).then(invokation -> { throw new Exception();});

    // Then:
    assertThat(authenticationUtil.getTokenTimeout(Optional.of(TOKEN), ksqlConfig, Optional.of(authTokenProvider)), equalTo(Optional.of(60000L)));
  }

  @Test
  public void shouldReturnTokenExpiryTime() {
    assertThat(authenticationUtil.getTokenTimeout(Optional.of(TOKEN), ksqlConfig, Optional.of(authTokenProvider)), equalTo(Optional.of(50000L)));
  }

  @Test
  public void shouldReturnMaxTimeout() {
    // Given:
    when(authTokenProvider.getLifetimeMs(TOKEN)).thenReturn(50000000L);

    // Then:
    assertThat(authenticationUtil.getTokenTimeout(Optional.of(TOKEN), ksqlConfig, Optional.of(authTokenProvider)), equalTo(Optional.of(60000L)));
  }
}
