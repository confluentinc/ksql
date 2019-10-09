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

package io.confluent.ksql.rest.server.services;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.when;

import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.server.resources.KsqlResource;
import io.confluent.ksql.services.ServiceContext;
import java.net.URI;
import java.util.Collections;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ServerInternalKsqlClientTest {

  private static final String KSQL_STATEMENT = "awesome ksql request;";
  private static final KsqlRequest EXPECTED_REQUEST =
      new KsqlRequest(KSQL_STATEMENT, Collections.emptyMap(), null);

  @Mock
  private KsqlResource ksqlResource;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private URI unused;
  @Mock
  private Response response;
  @Mock
  private KsqlEntityList entities;
  private ServerInternalKsqlClient ksqlClient;

  @Before
  public void setUp() {
    when(response.getStatus()).thenReturn(Status.OK.getStatusCode());
    when(response.readEntity(KsqlEntityList.class)).thenReturn(entities);

    ksqlClient = new ServerInternalKsqlClient(ksqlResource, serviceContext);
  }

  @Test
  public void shouldMakeKsqlRequest() {
    // Given:
    when(ksqlResource.handleKsqlStatements(serviceContext, EXPECTED_REQUEST)).thenReturn(response);

    // When:
    final RestResponse<KsqlEntityList> restResponse =
        ksqlClient.makeKsqlRequest(unused, KSQL_STATEMENT);

    // Then:
    assertThat("is successful", restResponse.isSuccessful());
    assertThat(restResponse.getResponse(), sameInstance(entities));
  }
}