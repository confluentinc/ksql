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

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.rest.client.KsqlClient;
import io.confluent.ksql.rest.client.KsqlTarget;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.util.KsqlConfig;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_INFERRED")
@RunWith(MockitoJUnitRunner.class)
public class DefaultKsqlClientTest {

  private static final String AUTH_HEADER = "BASIC auth header";
  private static final URI SERVER_ENDPOINT = mock(URI.class);

  @Mock
  private KsqlClient sharedClient;
  @Mock
  private KsqlTarget target;
  @Mock
  private RestResponse<KsqlEntityList> response;
  @Mock
  private RestResponse<List<StreamedRow>> queryResponse;
  @Mock
  private KsqlConfig ksqlConfig;
  private DefaultKsqlClient client;

  @Before
  public void setUp() {
    client = new DefaultKsqlClient(Optional.of(AUTH_HEADER), sharedClient, ksqlConfig);

    when(sharedClient.target(any())).thenReturn(target);
    when(target.authorizationHeader(any())).thenReturn(target);
    when(target.properties(any())).thenReturn(target);
    when(target.timeout(anyLong())).thenReturn(target);
    when(target.postKsqlRequest(any(), any(), any())).thenReturn(response);
    when(target.postQueryRequest(any(), any(), any())).thenReturn(queryResponse);
    when(queryResponse.getStatusCode()).thenReturn(OK.code());
  }

  @Test
  public void shouldGetRightTraget() {
    // When:
    client.makeKsqlRequest(SERVER_ENDPOINT, "Sql", ImmutableMap.of());

    // Then:
    verify(sharedClient).target(SERVER_ENDPOINT);
  }

  @Test
  public void shouldSetAuthHeaderOnTarget() {
    // When:
    client.makeKsqlRequest(SERVER_ENDPOINT, "Sql", ImmutableMap.of());

    // Then:
    verify(target).authorizationHeader(AUTH_HEADER);
  }

  @Test
  public void shouldHandleNoAuthHeader() {
    // Given:
    client = new DefaultKsqlClient(Optional.empty(), sharedClient, ksqlConfig);

    // When:
    final RestResponse<KsqlEntityList> result = client.makeKsqlRequest(SERVER_ENDPOINT, "Sql", ImmutableMap.of());

    // Then:
    verify(target, never()).authorizationHeader(any());
    assertThat(result, is(response));
  }

  @Test
  public void shouldPostRequest() {
    // When:
    final RestResponse<KsqlEntityList> result = client.makeKsqlRequest(SERVER_ENDPOINT, "Sql", ImmutableMap.of());

    // Then:
    verify(target).postKsqlRequest("Sql", ImmutableMap.of(), Optional.empty());
    assertThat(result, is(response));
  }

  @Test
  public void shouldSetQueryTimeout() {
    // Given:
    when(ksqlConfig.getLong(KsqlConfig.KSQL_QUERY_PULL_FORWARDING_TIMEOUT_MS_CONFIG))
        .thenReturn(300L);

    // When:
    final RestResponse<List<StreamedRow>> result = client.makeQueryRequest(SERVER_ENDPOINT, "Sql",
        ImmutableMap.of(), ImmutableMap.of());

    // Then:
    verify(target).postQueryRequest("Sql", ImmutableMap.of(), Optional.empty());
    verify(target).timeout(300L);
    assertThat(result.getStatusCode(), is(queryResponse.getStatusCode()));
  }
}