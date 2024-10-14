/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.api.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.api.auth.ApiSecurityContext;
import io.confluent.ksql.rest.client.KsqlClient;
import io.confluent.ksql.rest.server.services.RestServiceContextFactory;
import io.confluent.ksql.security.KsqlPrincipal;
import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.security.KsqlSecurityExtension;
import io.confluent.ksql.security.KsqlUserContextProvider;
import io.confluent.ksql.services.ConnectClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DefaultKsqlSecurityContextProviderTest {
  @Mock
  private KsqlSecurityExtension securityExtension;
  @Mock
  private RestServiceContextFactory.DefaultServiceContextFactory defaultServiceContextFactory;
  @Mock
  private RestServiceContextFactory.UserServiceContextFactory userServiceContextFactory;
  @Mock
  private KsqlUserContextProvider userContextProvider;
  @Mock
  private ServiceContext defaultServiceContext;
  @Mock
  private ServiceContext userServiceContext;
  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private SchemaRegistryClient schemaRegistryClientFactory;
  @Mock
  private ConnectClient connectClient;
  @Mock
  private KsqlClient ksqlClient;
  @Mock
  private KsqlPrincipal user1;
  @Mock
  private List<Entry<String, String>> incomingRequestHeaders;
  @Mock
  private ApiSecurityContext apiSecurityContext;

  private DefaultKsqlSecurityContextProvider ksqlSecurityContextProvider;

  @Before
  public void setup() {
    ksqlSecurityContextProvider = new DefaultKsqlSecurityContextProvider(
        securityExtension,
        defaultServiceContextFactory,
        userServiceContextFactory,
        ksqlConfig,
        () -> schemaRegistryClientFactory,
        (authHeader, requestHeaders, userPrincipal) -> connectClient,
        ksqlClient
    );

    when(apiSecurityContext.getPrincipal()).thenReturn(Optional.of(user1));
    when(apiSecurityContext.getAuthToken()).thenReturn(Optional.empty());
    when(apiSecurityContext.getRequestHeaders()).thenReturn(incomingRequestHeaders);

    when(defaultServiceContextFactory.create(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(defaultServiceContext);
    when(userServiceContextFactory.create(any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(userServiceContext);
  }

  @Test
  public void shouldCreateDefaultServiceContextIfUserContextProviderIsNotEnabled() {
    // Given:
    when(securityExtension.getUserContextProvider()).thenReturn(Optional.empty());

    // When:
    final KsqlSecurityContext ksqlSecurityContext =
        ksqlSecurityContextProvider.provide(apiSecurityContext);

    // Then:
    assertThat(ksqlSecurityContext.getUserPrincipal(), is(Optional.of(user1)));
    assertThat(ksqlSecurityContext.getServiceContext(), is(defaultServiceContext));
  }

  @Test
  public void shouldCreateDefaultServiceContextIfUserPrincipalIsMissing() {
    // Given:
    when(securityExtension.getUserContextProvider()).thenReturn(Optional.of(userContextProvider));
    when(apiSecurityContext.getPrincipal()).thenReturn(Optional.empty());

    // When:
    final KsqlSecurityContext ksqlSecurityContext =
        ksqlSecurityContextProvider.provide(apiSecurityContext);

    // Then:
    assertThat(ksqlSecurityContext.getUserPrincipal(), is(Optional.empty()));
    assertThat(ksqlSecurityContext.getServiceContext(), is(defaultServiceContext));
  }

  @Test
  public void shouldCreateUserServiceContextIfUserContextProviderIsEnabled() {
    // Given:
    when(securityExtension.getUserContextProvider()).thenReturn(Optional.of(userContextProvider));

    // When:
    final KsqlSecurityContext ksqlSecurityContext =
        ksqlSecurityContextProvider.provide(apiSecurityContext);

    // Then:
    verify(userServiceContextFactory)
        .create(eq(ksqlConfig), eq(Optional.empty()), any(), any(), any(), any(), any(), any());
    assertThat(ksqlSecurityContext.getUserPrincipal(), is(Optional.of(user1)));
    assertThat(ksqlSecurityContext.getServiceContext(), is(userServiceContext));
  }

  @Test
  public void shouldPassAuthHeaderToDefaultFactory() {
    // Given:
    when(securityExtension.getUserContextProvider()).thenReturn(Optional.empty());
    when(apiSecurityContext.getAuthToken()).thenReturn(Optional.of("some-auth"));

    // When:
    ksqlSecurityContextProvider.provide(apiSecurityContext);

    // Then:
    verify(defaultServiceContextFactory).create(any(), eq(Optional.of("some-auth")), any(), any(), any(), any(), any());
  }

  @Test
  public void shouldPassAuthHeaderToUserFactory() {
    // Given:
    when(securityExtension.getUserContextProvider()).thenReturn(Optional.of(userContextProvider));
    when(apiSecurityContext.getAuthToken()).thenReturn(Optional.of("some-auth"));

    // When:
    ksqlSecurityContextProvider.provide(apiSecurityContext);

    // Then:
    verify(userServiceContextFactory)
        .create(any(), eq(Optional.of("some-auth")), any(), any(), any(), any(), any(), any());
  }

  @Test
  public void shouldPassRequestHeadersToDefaultFactory() {
    // Given:
    when(securityExtension.getUserContextProvider()).thenReturn(Optional.empty());

    // When:
    ksqlSecurityContextProvider.provide(apiSecurityContext);

    // Then:
    verify(defaultServiceContextFactory).create(any(), any(), any(), any(), any(), eq(incomingRequestHeaders), any());
  }

  @Test
  public void shouldPassRequestHeadersToUserFactory() {
    // Given:
    when(securityExtension.getUserContextProvider()).thenReturn(Optional.of(userContextProvider));

    // When:
    ksqlSecurityContextProvider.provide(apiSecurityContext);

    // Then:
    verify(userServiceContextFactory)
        .create(any(), any(), any(), any(), any(), any(), eq(incomingRequestHeaders), any());
  }

  @Test
  public void shouldPassUserPrincipalToDefaultFactory() {
    // Given:
    when(securityExtension.getUserContextProvider()).thenReturn(Optional.empty());

    // When:
    ksqlSecurityContextProvider.provide(apiSecurityContext);

    // Then:
    verify(defaultServiceContextFactory).create(any(), any(), any(), any(), any(), any(), eq(Optional.of(user1)));
  }

  @Test
  public void shouldPassUserPrincipalToUserFactory() {
    // Given:
    when(securityExtension.getUserContextProvider()).thenReturn(Optional.of(userContextProvider));

    // When:
    ksqlSecurityContextProvider.provide(apiSecurityContext);

    // Then:
    verify(userServiceContextFactory)
        .create(any(), any(), any(), any(), any(), any(), any(), eq(Optional.of(user1)));
  }
}
