/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.server.context;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.rest.server.services.RestServiceContextFactory.DefaultServiceContextFactory;
import io.confluent.ksql.rest.server.services.RestServiceContextFactory.UserServiceContextFactory;
import io.confluent.ksql.security.KsqlSecurityExtension;
import io.confluent.ksql.security.KsqlUserContextProvider;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import java.security.Principal;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.SecurityContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlRestServiceContextFactoryTest {
  private KsqlRestServiceContextFactory serviceContextFactory;

  @Mock
  private SecurityContext securityContext;
  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private KsqlSecurityExtension securityExtension;
  @Mock
  private KsqlUserContextProvider userContextProvider;
  @Mock
  private Principal user1;
  @Mock
  private DefaultServiceContextFactory defaultServiceContextProvider;
  @Mock
  private UserServiceContextFactory userServiceContextFactory;
  @Mock
  private ServiceContext defaultServiceContext;
  @Mock
  private ServiceContext userServiceContext;
  @Mock
  private HttpServletRequest request;

  @Before
  public void setUp() {
    KsqlRestServiceContextFactory.configure(ksqlConfig, securityExtension);
    serviceContextFactory = new KsqlRestServiceContextFactory(
        securityContext,
        request,
        defaultServiceContextProvider,
        userServiceContextFactory
    );

    when(securityContext.getUserPrincipal()).thenReturn(user1);
    when(defaultServiceContextProvider.create(any(), any())).thenReturn(defaultServiceContext);
    when(userServiceContextFactory.create(any(), any(), any(), any()))
        .thenReturn(userServiceContext);
  }

  @Test
  public void shouldCreateDefaultServiceContextIfUserContextProviderIsNotEnabled() {
    // Given:
    when(securityExtension.getUserContextProvider()).thenReturn(Optional.empty());

    // When:
    final ServiceContext serviceContext = serviceContextFactory.provide();

    // Then:
    verify(defaultServiceContextProvider).create(ksqlConfig, Optional.empty());
    assertThat(serviceContext, is(defaultServiceContext));
  }

  @Test
  public void shouldCreateUserServiceContextIfUserContextProviderIsEnabled() {
    // Given:
    when(securityExtension.getUserContextProvider()).thenReturn(Optional.of(userContextProvider));

    // When:
    final ServiceContext serviceContext = serviceContextFactory.provide();

    // Then:
    verify(userServiceContextFactory).create(eq(ksqlConfig), eq(Optional.empty()), any(), any());
    assertThat(serviceContext, is(userServiceContext));
  }

  @Test
  public void shouldPassAuthHeaderToDefaultFactory() {
    // Given:
    when(securityExtension.getUserContextProvider()).thenReturn(Optional.empty());
    when(request.getHeader(HttpHeaders.AUTHORIZATION)).thenReturn("some-auth");

    // When:
    serviceContextFactory.provide();

    // Then:
    verify(defaultServiceContextProvider).create(any(), eq(Optional.of("some-auth")));
  }

  @Test
  public void shouldPassAuthHeaderToUserFactory() {
    // Given:
    when(securityExtension.getUserContextProvider()).thenReturn(Optional.of(userContextProvider));
    when(request.getHeader(HttpHeaders.AUTHORIZATION)).thenReturn("some-auth");

    // When:
    serviceContextFactory.provide();

    // Then:
    verify(userServiceContextFactory).create(any(), eq(Optional.of("some-auth")), any(), any());
  }
}
