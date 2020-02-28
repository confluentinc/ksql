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

package io.confluent.ksql.rest.client;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.properties.LocalProperties;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatuses;
import io.confluent.ksql.rest.entity.HealthCheckResponse;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.ServerInfo;
import java.io.InputStream;
import java.util.Map;
import java.util.Optional;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import org.eclipse.jetty.http.HttpStatus.Code;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlTargetTest {

  private static final Optional<Long> NO_PREVIOUS_CMD = Optional.empty();

  @Mock
  private WebTarget webTarget;
  @Mock
  private Builder invocationBuilder;
  @Mock
  private Response response;
  @Mock
  private LocalProperties localProperties;
  @Mock
  private Map<String, Object> localPropertiesAsMap;
  private KsqlTarget target;

  @Before
  public void setUp() {
    target = new KsqlTarget(webTarget, localProperties, Optional.empty());

    when(webTarget.path(any())).thenReturn(webTarget);
    when(webTarget.request(any(MediaType.class))).thenReturn(invocationBuilder);
    when(invocationBuilder.headers(any())).thenReturn(invocationBuilder);
    when(invocationBuilder.property(any(), any())).thenReturn(invocationBuilder);
    when(invocationBuilder.get()).thenReturn(response);
    when(invocationBuilder.post(any())).thenReturn(response);
    when(response.getStatus()).thenReturn(Code.OK.getCode());

    when(localProperties.toMap()).thenReturn(localPropertiesAsMap);
  }

  @Test
  public void shouldIssueGetRequests() {
    // Given:
    when(response.readEntity(ServerInfo.class)).thenReturn(mock(ServerInfo.class));

    // When:
    target.getServerInfo();

    // Then:
    verify(webTarget).path(any());
    verify(webTarget).request(MediaType.APPLICATION_JSON_TYPE);
    verify(invocationBuilder).headers(any());
    verify(invocationBuilder).get();
  }

  @Test
  public void shouldIssueGetRequestWithAuthHeader() {
    // Given:
    when(response.readEntity(ServerInfo.class)).thenReturn(mock(ServerInfo.class));

    // When:
    target
        .authorizationHeader("Auth header")
        .getServerInfo();

    // Then:
    verify(invocationBuilder).headers(authHeaders("Auth header"));
  }

  @Test
  public void shouldHandleGetFailure() {
    // Given:
    when(response.getStatus()).thenReturn(Code.BAD_REQUEST.getCode());
    when(response.getStatusInfo()).thenReturn(Response.Status.BAD_REQUEST);

    // When:
    final RestResponse<?> result = target.getServerInfo();

    // Then:
    assertThat("is erroneous", result.isErroneous());
    assertThat(result.get(), is(instanceOf(KsqlErrorMessage.class)));
    assertThat(result.getStatusCode(), is(Code.BAD_REQUEST));
  }

  @Test
  public void shouldIssuePostRequests() {
    // When:
    target.postPrintTopicRequest("statement", Optional.empty());

    // Then:
    verify(webTarget).path(any());
    verify(webTarget).request(MediaType.APPLICATION_JSON_TYPE);
    verify(invocationBuilder).property(any(), any());
    verify(invocationBuilder).headers(any());
    verify(invocationBuilder).post(any());
  }

  @Test
  public void shouldIssuePostRequestsWithAuthHeaders() {
    // When:
    target
        .authorizationHeader("BASIC skhfhsknks")
        .postPrintTopicRequest("statement", Optional.empty());

    // Then:
    verify(invocationBuilder).headers(authHeaders("BASIC skhfhsknks"));
  }

  @Test
  public void shouldHandlePostFailure() {
    // Given:
    when(response.getStatus()).thenReturn(Code.BAD_REQUEST.getCode());
    when(response.getStatusInfo()).thenReturn(Response.Status.BAD_REQUEST);

    // When:
    final RestResponse<?> result = target.postPrintTopicRequest("request", Optional.empty());

    // Then:
    assertThat("is erroneous", result.isErroneous());
    assertThat(result.get(), is(instanceOf(KsqlErrorMessage.class)));
    assertThat(result.getStatusCode(), is(Code.BAD_REQUEST));
  }

  @Test
  public void shouldGetServerInfo() {
    // Given:
    final ServerInfo serverInfo = mock(ServerInfo.class);
    when(response.readEntity(ServerInfo.class)).thenReturn(serverInfo);

    // When:
    final RestResponse<ServerInfo> result = target.getServerInfo();

    // Then:
    verify(webTarget).path("/info");
    verify(invocationBuilder).get();
    verify(response).close();
    assertThat(result.get(), is(sameInstance(serverInfo)));
  }

  @Test
  public void shouldGetServerHealth() {
    // Given:
    final HealthCheckResponse serverHealth = mock(HealthCheckResponse.class);
    when(response.readEntity(HealthCheckResponse.class)).thenReturn(serverHealth);

    // When:
    final RestResponse<HealthCheckResponse> result = target.getServerHealth();

    // Then:
    verify(webTarget).path("/healthcheck");
    verify(invocationBuilder).get();
    verify(response).close();
    assertThat(result.get(), is(sameInstance(serverHealth)));
  }

  @Test
  public void shouldGetStatuses() {
    // Given:
    final CommandStatuses statuses = mock(CommandStatuses.class);
    when(response.readEntity(CommandStatuses.class)).thenReturn(statuses);

    // When:
    final RestResponse<CommandStatuses> result = target.getStatuses();

    // Then:
    verify(webTarget).path("/status");
    verify(invocationBuilder).get();
    verify(response).close();
    assertThat(result.get(), is(sameInstance(statuses)));
  }

  @Test
  public void shouldGetStatus() {
    // Given:
    final CommandStatus status = mock(CommandStatus.class);
    when(response.readEntity(CommandStatus.class)).thenReturn(status);

    // When:
    final RestResponse<CommandStatus> result = target.getStatus("cmdId");

    // Then:
    verify(webTarget).path("/status/cmdId");
    verify(invocationBuilder).get();
    verify(response).close();
    assertThat(result.get(), is(sameInstance(status)));
  }

  @Test
  public void shouldPostKsqlRequest() {
    // Given:
    final KsqlEntityList entityList = mock(KsqlEntityList.class);
    when(response.readEntity(KsqlEntityList.class)).thenReturn(entityList);

    // When:
    final RestResponse<KsqlEntityList> result = target
        .postKsqlRequest("Test request", NO_PREVIOUS_CMD);

    // Then:
    verify(webTarget).path("/ksql");
    verify(invocationBuilder).post(jsonKsqlRequest("Test request", NO_PREVIOUS_CMD));
    verify(response).close();
    assertThat(result.get(), is(sameInstance(entityList)));
  }

  @Test
  public void shouldPostKsqlRequestWithPreviousCmdSeqNum() {
    // Given:
    final KsqlEntityList entityList = mock(KsqlEntityList.class);
    when(response.readEntity(KsqlEntityList.class)).thenReturn(entityList);

    // When:
    target
        .postKsqlRequest("ksql request", Optional.of(24L));

    // Then:
    verify(invocationBuilder).post(jsonKsqlRequest("ksql request", Optional.of(24L)));
  }

  @Test
  public void shouldPostQueryRequest() {
    // Given:
    final InputStream is = mock(InputStream.class);
    when(response.getEntity()).thenReturn(is);

    // When:
    target
        .postQueryRequest("query request", NO_PREVIOUS_CMD);

    // Then:
    verify(webTarget).path("/query");
    verify(invocationBuilder).post(jsonKsqlRequest("query request", NO_PREVIOUS_CMD));
    verify(response, never()).close();
  }

  @Test
  public void shouldPostQueryRequestWithPreviousCmdSeqNum() {
    // Given:
    final InputStream is = mock(InputStream.class);
    when(response.getEntity()).thenReturn(is);

    // When:
    target
        .postQueryRequest("query request", Optional.of(42L));

    // Then:
    verify(invocationBuilder).post(jsonKsqlRequest("query request", Optional.of(42L)));
  }

  @Test
  public void shouldPostPrintTopicRequest() {
    // Given:
    final InputStream is = mock(InputStream.class);
    when(response.getEntity()).thenReturn(is);

    // When:
    final RestResponse<InputStream> result = target
        .postPrintTopicRequest("print request", NO_PREVIOUS_CMD);

    // Then:
    verify(webTarget).path("/query");
    verify(invocationBuilder).post(jsonKsqlRequest("print request", NO_PREVIOUS_CMD));
    verify(response, never()).close();
    assertThat(result.get(), is(is));
  }

  @Test
  public void shouldPostPrintTopicRequestWithPreviousCmdSeqNum() {
    // Given:
    final InputStream is = mock(InputStream.class);
    when(response.getEntity()).thenReturn(is);

    // When:
    target
        .postPrintTopicRequest("print request", Optional.of(42L));

    // Then:
    verify(invocationBuilder).post(jsonKsqlRequest("print request", Optional.of(42L)));
  }

  private Entity<Object> jsonKsqlRequest(
      final String ksql,
      final Optional<Long> previousCmdSeqNum
  ) {
    return Entity.json(new KsqlRequest(
        ksql,
        localPropertiesAsMap,
        previousCmdSeqNum.orElse(null)
    ));
  }

  private static MultivaluedMap<String, Object> authHeaders(final String value) {
    final MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
    headers.add(HttpHeaders.AUTHORIZATION, value);
    return headers;
  }
}