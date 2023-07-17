/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.api.server;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.server.resources.KsqlRestException;
import io.confluent.ksql.util.VertxCompletableFuture;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class OldApiUtilsTest {

  @Mock
  private Server server;
  @Mock
  private HttpServerRequest request;
  @Mock
  private RoutingContext routingContext;

  @Before
  public void setUp() {
    when(routingContext.request()).thenReturn(request);
  }

  @Test
  public void shouldMaskKsqlRequestQuery() throws ExecutionException, InterruptedException {
    // Given
    final String query = "--this is a comment. \n"
        + "CREATE SOURCE CONNECTOR `test-connector` WITH ("
        + "    \"connector.class\" = 'PostgresSource', \n"
        + "    'connection.url' = 'jdbc:postgresql://localhost:5432/my.db',\n"
        + "    \"mode\"='bulk',\n"
        + "    \"topic.prefix\"='jdbc-',\n"
        + "    \"table.whitelist\"='users',\n"
        + "    \"key\"='username');";
    final KsqlRequest req = new KsqlRequest(query, ImmutableMap.of(), ImmutableMap.of(), 1L);
    when(routingContext.getBody()).thenReturn(ServerUtils.serializeObject(req));

    // when, Then
    final String expected = "CREATE SOURCE CONNECTOR `test-connector` WITH "
        + "(\"connector.class\"='PostgresSource', "
        + "'connection.url'='[string]', "
        + "\"mode\"='[string]', "
        + "\"topic.prefix\"='[string]', "
        + "\"table.whitelist\"='[string]', "
        + "\"key\"='[string]');";

    OldApiUtils.handleOldApiRequest(server, routingContext, KsqlRequest.class, Optional.empty(),
        (ksqlRequest, securityContext) -> {
      assertThat(ksqlRequest.getMaskedKsql(), is(expected));
      final VertxCompletableFuture<EndpointResponse> vcf = new VertxCompletableFuture<>();
      vcf.complete(EndpointResponse.failed(1));
      return vcf;
    });
  }

  @Test
  public void shouldReturnEmbeddedResponseForKsqlRestException() {
    final EndpointResponse response = EndpointResponse.failed(400);
    assertThat(
        OldApiUtils.mapException(new KsqlRestException(response)),
        sameInstance(response));
  }

  @Test
  public void shouldReturnCorrectResponseForUnspecificException() {
    final EndpointResponse response = OldApiUtils
        .mapException(new Exception("error msg"));
    assertThat(response.getEntity(), instanceOf(KsqlErrorMessage.class));
    final KsqlErrorMessage errorMessage = (KsqlErrorMessage) response.getEntity();
    assertThat(errorMessage.getMessage(), equalTo("error msg"));
    assertThat(errorMessage.getErrorCode(), equalTo(Errors.ERROR_CODE_SERVER_ERROR));
    assertThat(response.getStatus(), equalTo(INTERNAL_SERVER_ERROR.code()));
  }
}
