/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.api;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import io.confluent.ksql.api.auth.ApiServerConfig;
import io.confluent.ksql.test.util.TestBasicJaasConfig;
import io.confluent.ksql.util.VertxCompletableFuture;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(CoreApiTestRunner.class)
public class BasicAuthTest extends ApiTest {

  protected static final Logger log = LoggerFactory.getLogger(BasicAuthTest.class);

  private static final String PROPS_JAAS_REALM = "KsqlServer-Props";
  private static final String KSQL_RESOURCE = "ksql-user";
  private static final String OTHER_RESOURCE = "not-ksql";
  private static final String USER_WITH_ACCESS = "harry";
  private static final String USER_WITH_ACCESS_PWD = "changeme";
  private static final String USER_WITHOUT_ACCESS = "john";
  private static final String USER_WITHOUT_ACCESS_PWD = "bad";
  private static final String USER_WITH_INCORRECT_ROLE = "maud";
  private static final String USER_WITH_INCORRECT_ROLE_PWD = "1234";

  @ClassRule
  public static final TestBasicJaasConfig JAAS_CONFIG = TestBasicJaasConfig
      .builder(PROPS_JAAS_REALM)
      .addUser(USER_WITH_ACCESS, USER_WITH_ACCESS_PWD, KSQL_RESOURCE)
      .addUser(USER_WITH_INCORRECT_ROLE, USER_WITH_INCORRECT_ROLE_PWD, OTHER_RESOURCE)
      .build();

  @Override
  protected ApiServerConfig createServerConfig() {
    ApiServerConfig config = super.createServerConfig();
    Map<String, Object> origs = config.originals();
    origs.put(
        ApiServerConfig.AUTHENTICATION_METHOD_CONFIG,
        ApiServerConfig.AUTHENTICATION_METHOD_BASIC);
    origs.put(
        ApiServerConfig.AUTHENTICATION_REALM_CONFIG,
        PROPS_JAAS_REALM
    );
    origs.put(
        ApiServerConfig.AUTHENTICATION_ROLES_CONFIG,
        KSQL_RESOURCE
    );
    return new ApiServerConfig(origs);
  }

  @Override
  protected HttpResponse<Buffer> sendRequest(final WebClient client, final String uri,
      final Buffer requestBody)
      throws Exception {
    return sendRequestWithCreds(client, uri, requestBody, USER_WITH_ACCESS, USER_WITH_ACCESS_PWD);
  }

  @Override
  protected void sendRequest(
      final WebClient client,
      final String uri,
      final Consumer<HttpRequest<Buffer>> requestSender) {
    requestSender.accept(
        client.post(uri)
            .basicAuthentication(USER_WITH_ACCESS, USER_WITH_ACCESS_PWD)
    );
  }

  @Test
  public void shouldFailQueryWithBadCredentials() throws Exception {
    shouldFailQuery(USER_WITHOUT_ACCESS, USER_WITHOUT_ACCESS_PWD);
  }

  @Test
  public void shouldFailCloseQueryWithBadCredentials() throws Exception {
    shouldFailCloseQuery(USER_WITHOUT_ACCESS, USER_WITHOUT_ACCESS_PWD);
  }

  @Test
  public void shouldFailInsertRequestWithBadCredentials() throws Exception {
    shouldFailInsertRequest(USER_WITHOUT_ACCESS, USER_WITHOUT_ACCESS_PWD);
  }

  @Test
  public void shouldFailQueryWithNoCredentials() throws Exception {
    shouldFailQuery(null, null);
  }

  @Test
  public void shouldFailCloseQueryWithNoCredentials() throws Exception {
    shouldFailCloseQuery(null, null);
  }

  @Test
  public void shouldFailInsertRequestWithNoCredentials() throws Exception {
    shouldFailInsertRequest(null, null);
  }

  @Test
  public void shouldFailQueryWithIncorrectRole() throws Exception {
    shouldFailQuery(USER_WITH_INCORRECT_ROLE, USER_WITH_INCORRECT_ROLE_PWD);
  }

  @Test
  public void shouldFailCloseQueryWithIncorrectRole() throws Exception {
    shouldFailCloseQuery(USER_WITH_INCORRECT_ROLE, USER_WITH_INCORRECT_ROLE_PWD);
  }

  @Test
  public void shouldFailInsertRequestWithIncorrectRole() throws Exception {
    shouldFailInsertRequest(USER_WITH_INCORRECT_ROLE, USER_WITH_INCORRECT_ROLE_PWD);
  }

  @Test
  public void shouldExecutePullQueryWithApiSecurityContext() throws Exception {
    super.shouldExecutePullQuery();
    assertAuthorisedSecurityContext(USER_WITH_ACCESS);
  }

  @Test
  public void shouldStreamInsertsWithApiSecurityContext() throws Exception {
    super.shouldStreamInserts();
    assertAuthorisedSecurityContext(USER_WITH_ACCESS);
  }

  @Test
  public void shouldCloseQueryWithApiSecurityContext() throws Exception {
    super.shouldCloseQuery();
    assertAuthorisedSecurityContext(USER_WITH_ACCESS);
  }

  private void shouldFailQuery(final String username, final String password) throws Exception {
    // When
    HttpResponse<Buffer> response = sendRequestWithCreds(
        "/query-stream",
        DEFAULT_PUSH_QUERY_REQUEST_BODY.toBuffer(),
        username,
        password
    );

    // Then
    assertThat(response.statusCode(), is(401));
    assertThat(response.statusMessage(), is("Unauthorized"));
  }

  private void shouldFailCloseQuery(final String username, final String password) throws Exception {
    // Given
    JsonObject requestBody = new JsonObject().put("queryId", "foo");

    // When
    HttpResponse<Buffer> response = sendRequestWithCreds(
        "/close-query",
        requestBody.toBuffer(),
        username,
        password
    );

    // Then
    assertThat(response.statusCode(), is(401));
    assertThat(response.statusMessage(), is("Unauthorized"));
  }

  private void shouldFailInsertRequest(final String username, final String password) throws Exception {
    // Given
    JsonObject params = new JsonObject().put("target", "test-stream");
    Buffer requestBody = Buffer.buffer();
    requestBody.appendBuffer(params.toBuffer()).appendString("\n");
    for (JsonObject row : DEFAULT_INSERT_ROWS) {
      requestBody.appendBuffer(row.toBuffer()).appendString("\n");
    }

    // When
    HttpResponse<Buffer> response = sendRequestWithCreds(
        "/inserts-stream",
        requestBody,
        username,
        password
    );

    // Then
    assertThat(response.statusCode(), is(401));
    assertThat(response.statusMessage(), is("Unauthorized"));
  }

  private HttpResponse<Buffer> sendRequestWithCreds(
      final String uri,
      final Buffer requestBody,
      final String username,
      final String password
  ) throws Exception {
    return sendRequestWithCreds(client, uri, requestBody, username, password);
  }

  // auth header is omitted if username and password are null
  private static HttpResponse<Buffer> sendRequestWithCreds(
      final WebClient client,
      final String uri,
      final Buffer requestBody,
      final String username,
      final String password
  ) throws Exception {
    VertxCompletableFuture<HttpResponse<Buffer>> requestFuture = new VertxCompletableFuture<>();
    HttpRequest<Buffer> request = client.post(uri);
    if (username != null || password != null) {
      request = request.basicAuthentication(username, password);
    }
    request.sendBuffer(requestBody, requestFuture);
    return requestFuture.get();
  }

  private void assertNotAuthorisedSecurityContext() {
    assertThat(testEndpoints.getLastApiSecurityContext(), is(notNullValue()));
    assertThat(testEndpoints.getLastApiSecurityContext().getPrincipal().isPresent(), is(false));
  }

  private void assertAuthorisedSecurityContext(String username) {
    assertThat(testEndpoints.getLastApiSecurityContext(), is(notNullValue()));
    assertThat(testEndpoints.getLastApiSecurityContext().getPrincipal().isPresent(), is(true));
    assertThat(testEndpoints.getLastApiSecurityContext().getPrincipal().get().getName(),
        is(username));
  }

}
