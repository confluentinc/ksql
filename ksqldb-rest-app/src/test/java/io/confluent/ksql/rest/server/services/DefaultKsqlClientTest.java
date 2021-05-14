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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
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
import java.net.URI;
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
  private DefaultKsqlClient client;

  @Before
  public void setUp() {
    client = new DefaultKsqlClient(Optional.of(AUTH_HEADER), sharedClient);

    when(sharedClient.target(any())).thenReturn(target);
    when(target.authorizationHeader(any())).thenReturn(target);
    when(target.postKsqlRequest(any(), any(), any())).thenReturn(response);
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
    client = new DefaultKsqlClient(Optional.empty(), sharedClient);

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
}