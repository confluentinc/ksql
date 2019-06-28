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

package io.confluent.ksql.rest.server.filters;

import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.server.security.KsqlAuthorizationProvider;
import io.confluent.ksql.util.KsqlException;
import io.confluent.rest.entities.ErrorMessage;
import org.glassfish.jersey.internal.PropertiesDelegate;
import org.glassfish.jersey.server.ContainerRequest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import java.net.URI;
import java.security.Principal;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KsqlAuthorizationFilterTest {
  private static final int FORBIDDEN = Response.Status.FORBIDDEN.getStatusCode();

  @Mock
  private KsqlAuthorizationProvider authorizationProvider;
  @Mock
  private SecurityContext securityContext;
  @Mock
  private Principal userPrincipal;

  private KsqlAuthorizationFilter authorizationFilter;

  @Before
  public void setUp() {
    authorizationFilter = new KsqlAuthorizationFilter(authorizationProvider);

    reset(securityContext);
  }

  @Test
  public void filterShouldContinueIfAuthorizationIsAllowed() {
    // Given:
    ContainerRequest request = givenRequestContext(userPrincipal, "GET", "query");

    // When:
    authorizationFilter.filter(request);

    // Then:
    assertThat(request.getAbortResponse(), is(nullValue()));
  }

  @Test
  public void filterShouldAbortIfAuthorizationIsDenied() {
    // Given:
    ContainerRequest request = givenRequestContext(userPrincipal, "GET", "query");
    doThrow(new KsqlException("access denied"))
        .when(authorizationProvider).checkEndpointAccess(userPrincipal, "GET", "/query");

    // When:
    authorizationFilter.filter(request);

    // Then:
    assertThat(request.getAbortResponse().getStatus(), is(FORBIDDEN));
    assertThat(((KsqlErrorMessage)request.getAbortResponse().getEntity()).getMessage(),
        is("access denied"));
  }

  private ContainerRequest givenRequestContext(
      final Principal principal,
      final String method,
      final String path
  ) {
    when(securityContext.getUserPrincipal()).thenReturn(principal);

    ContainerRequest requestContext = new ContainerRequest(
        URI.create(""),
        URI.create(path),
        method,
        securityContext,
        mock(PropertiesDelegate.class)
    );

    return requestContext;
  }
}
