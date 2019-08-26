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

import io.confluent.ksql.security.KsqlSecurityExtension;
import io.confluent.ksql.security.KsqlUserContextProvider;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import java.security.Principal;
import java.util.Optional;
import java.util.function.Function;
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
  private Principal principal;
  @Mock
  private Function<KsqlConfig, ServiceContext> defaultServiceContextProvider;
  @Mock
  private KsqlRestServiceContextFactory.UserServiceContextFactory userServiceContextFactory;
  @Mock
  private ServiceContext defaultServiceContext;
  @Mock
  private ServiceContext userServiceContext;

  @Before
  public void setUp() {
    KsqlRestServiceContextFactory.configure(ksqlConfig, securityExtension);
    serviceContextFactory = new KsqlRestServiceContextFactory(
        securityContext,
        defaultServiceContextProvider,
        userServiceContextFactory
    );

    when(principal.getName()).thenReturn("user1");
    when(securityContext.getUserPrincipal()).thenReturn(principal);
    when(userServiceContext.getContextType())
        .thenReturn(ServiceContext.ContextType.CLIENT_CONTEXT);
    when(userServiceContext.getUsername()).thenReturn(Optional.of("user1"));
    when(defaultServiceContext.getContextType())
        .thenReturn(ServiceContext.ContextType.SERVER_CONTEXT);
    when(defaultServiceContextProvider.apply(ksqlConfig)).thenReturn(defaultServiceContext);
    when(userServiceContextFactory.create(any(), any(), any(), any(), any()))
        .thenReturn(userServiceContext);
  }

  @Test
  public void shouldCreateDefaultServiceContextIfUserContextProviderIsNotEnabled() {
    // Given:
    when(securityExtension.getUserContextProvider()).thenReturn(Optional.empty());

    // When:
    final ServiceContext serviceContext = serviceContextFactory.provide();

    // Then:
    verify(defaultServiceContextProvider).apply(ksqlConfig);
    assertThat(serviceContext, is(defaultServiceContext));
    assertThat(serviceContext.getContextType(), is(ServiceContext.ContextType.SERVER_CONTEXT));
    assertThat(serviceContext.getUsername(), is(Optional.empty()));
  }

  @Test
  public void shouldCreateUserServiceContextIfUserContextProviderIsEnabled() {
    // Given:
    when(securityExtension.getUserContextProvider()).thenReturn(Optional.of(userContextProvider));

    // When:
    final ServiceContext serviceContext = serviceContextFactory.provide();

    // Then:
    verify(userServiceContextFactory).create(any(), any(), eq(ksqlConfig), any(), any());
    assertThat(serviceContext, is(userServiceContext));
    assertThat(serviceContext.getContextType(), is(ServiceContext.ContextType.CLIENT_CONTEXT));
    assertThat(serviceContext.getUsername(), is(Optional.of("user1")));
  }
}
