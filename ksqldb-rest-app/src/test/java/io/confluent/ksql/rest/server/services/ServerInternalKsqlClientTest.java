/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.rest.server.services;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.server.resources.KsqlResource;
import io.confluent.ksql.security.KsqlSecurityContext;
import java.net.URI;
import java.util.Collections;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ServerInternalKsqlClientTest {

  private static final String KSQL_STATEMENT = "awesome ksql request;";
  private static final KsqlRequest EXPECTED_REQUEST =
      new KsqlRequest(KSQL_STATEMENT, Collections.emptyMap(), Collections.emptyMap(), null);

  @Mock
  private KsqlResource ksqlResource;
  @Mock
  private KsqlSecurityContext securityContext;
  @Mock
  private URI unused;
  @Mock
  private EndpointResponse response;
  @Mock
  private KsqlEntityList entities;
  private ServerInternalKsqlClient ksqlClient;

  @Before
  public void setUp() {
    when(response.getStatus()).thenReturn(OK.code());
    when(response.getEntity()).thenReturn(entities);

    ksqlClient = new ServerInternalKsqlClient(ksqlResource, securityContext);
  }

  @Test
  public void shouldMakeKsqlRequest() {
    // Given:
    when(ksqlResource.handleKsqlStatements(securityContext, EXPECTED_REQUEST)).thenReturn(response);

    // When:
    final RestResponse<KsqlEntityList> restResponse =
        ksqlClient.makeKsqlRequest(unused, KSQL_STATEMENT, ImmutableMap.of());

    // Then:
    assertThat("is successful", restResponse.isSuccessful());
    assertThat(restResponse.getResponse(), sameInstance(entities));
  }
}