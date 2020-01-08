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

import io.confluent.ksql.rest.server.services.RestServiceContextFactory.UserServiceContextFactory;
import io.confluent.ksql.security.KsqlSecurityContext;
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
public class KsqlSecurityContextBinderFactoryTest {
  private KsqlSecurityContextBinderFactory securityContextBinderFactory;

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
  private UserServiceContextFactory userServiceContextFactory;
  @Mock
  private ServiceContext defaultServiceContext;
  @Mock
  private ServiceContext userServiceContext;
  @Mock
  private HttpServletRequest request;

  @Before
  public void setUp() {
    KsqlSecurityContextBinderFactory.configure(ksqlConfig, securityExtension,
        defaultServiceContext);
    securityContextBinderFactory = new KsqlSecurityContextBinderFactory(
        securityContext,
        request,
        userServiceContextFactory
    );

    when(securityContext.getUserPrincipal()).thenReturn(user1);
    when(userServiceContextFactory.create(any(), any(), any(), any()))
        .thenReturn(userServiceContext);
  }

  @Test
  public void shouldCreateDefaultServiceContextIfUserContextProviderIsNotEnabled() {
    // Given:
    when(securityExtension.getUserContextProvider()).thenReturn(Optional.empty());
    when(securityContext.getUserPrincipal()).thenReturn(null);

    // When:
    final KsqlSecurityContext ksqlSecurityContext = securityContextBinderFactory.provide();

    // Then:
    assertThat(ksqlSecurityContext.getUserPrincipal(), is(Optional.empty()));
    assertThat(ksqlSecurityContext.getServiceContext(), is(defaultServiceContext));
  }

  @Test
  public void shouldCreateUserServiceContextIfUserContextProviderIsEnabled() {
    // Given:
    when(securityExtension.getUserContextProvider()).thenReturn(Optional.of(userContextProvider));

    // When:
    final KsqlSecurityContext ksqlSecurityContext = securityContextBinderFactory.provide();

    // Then:
    verify(userServiceContextFactory).create(eq(ksqlConfig), eq(Optional.empty()), any(), any());
    assertThat(ksqlSecurityContext.getUserPrincipal(), is(Optional.of(user1)));
    assertThat(ksqlSecurityContext.getServiceContext(), is(userServiceContext));
  }

  @Test
  public void shouldPassAuthHeaderToUserFactory() {
    // Given:
    when(securityExtension.getUserContextProvider()).thenReturn(Optional.of(userContextProvider));
    when(request.getHeader(HttpHeaders.AUTHORIZATION)).thenReturn("some-auth");

    // When:
    securityContextBinderFactory.provide();

    // Then:
    verify(userServiceContextFactory).create(any(), eq(Optional.of("some-auth")), any(), any());
  }
}
