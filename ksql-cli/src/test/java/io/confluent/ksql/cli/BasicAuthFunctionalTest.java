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

package io.confluent.ksql.cli;

import static io.confluent.ksql.rest.server.resources.Errors.ERROR_CODE_FORBIDDEN;
import static io.confluent.ksql.rest.server.resources.Errors.ERROR_CODE_UNAUTHORIZED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import com.google.common.base.Charsets;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.ServerInfo;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster;
import io.confluent.rest.RestConfig;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.ws.rs.core.HttpHeaders;
import org.apache.kafka.common.security.JaasUtils;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.http.HttpStatus.Code;
import org.eclipse.jetty.jaas.spi.PropertyFileLoginModule;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.UpgradeException;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketError;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.glassfish.jersey.internal.util.Base64;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

@Category({IntegrationTest.class})
public class BasicAuthFunctionalTest {

  private static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  static {
    createTmpFolder();
  }

  private static final String PROPS_JAAS_REALM = "KsqlServer-Props";
  private static final String KSQL_CLUSTER_ID = "ksql-11";
  private static final String USER_WITH_ACCESS = "harry";
  private static final String USER_WITH_ACCESS_PWD = "changeme";
  private static final String USER_NO_ACCESS = "tom";
  private static final String USER_NO_ACCESS_PWD = "changeme";

  private static final String BASIC_PASSWORDS_FILE_CONTENT =
      "# Each line generated using org.eclipse.jetty.util.security.Password\n"
          + USER_WITH_ACCESS + ": " + USER_WITH_ACCESS_PWD + "," + KSQL_CLUSTER_ID + "\n"
          + USER_NO_ACCESS + ": " + USER_NO_ACCESS_PWD + ",ksql-other\n";

  private static final EmbeddedSingleNodeKafkaCluster CLUSTER = EmbeddedSingleNodeKafkaCluster
      .newBuilder()
      .withAdditionalJaasConfig(createJaasConfigContent())
      .build();

  private static final TestKsqlRestApp REST_APP = TestKsqlRestApp
      .builder(CLUSTER::bootstrapServers)
      .withProperty(RestConfig.AUTHENTICATION_METHOD_CONFIG, "BASIC")
      .withProperty(RestConfig.AUTHENTICATION_REALM_CONFIG, PROPS_JAAS_REALM)
      .withProperty(RestConfig.AUTHENTICATION_ROLES_CONFIG, KSQL_CLUSTER_ID)
      .withProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, CLUSTER.getJaasConfigPath())
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain.outerRule(CLUSTER).around(REST_APP);

  @Test
  public void shouldNotBeAbleToUseWsWithNoCreds() throws Exception {
    assertThat(makeWsRequest("", ""), is(Code.UNAUTHORIZED));
  }

  @Test
  public void shouldNotBeAbleToUseCliWithInvalidPassword() {
    assertThat(canMakeCliRequest(USER_WITH_ACCESS, "wrong pwd"), is(ERROR_CODE_UNAUTHORIZED));
  }

  @Test
  public void shouldNotBeAbleToUseWsWithInvalidPassword() throws Exception {
    assertThat(makeWsRequest(USER_WITH_ACCESS, "wrong pwd"), is(Code.UNAUTHORIZED));
  }

  @Test
  public void shouldNotBeAbleToUseCliWithUnknownUser() {
    assertThat(canMakeCliRequest("Unknown-user", "some password"), is(ERROR_CODE_UNAUTHORIZED));
  }

  @Test
  public void shouldNotBeAbleToUseWsWithUnknownUser() throws Exception {
    assertThat(makeWsRequest("Unknown-user", "some password"), is(Code.UNAUTHORIZED));
  }

  @Test
  public void shouldNotBeAbleToUseCliWithValidCredsIfUserHasNoAccessToThisCluster() {
    assertThat(canMakeCliRequest(USER_NO_ACCESS, USER_NO_ACCESS_PWD), is(ERROR_CODE_FORBIDDEN));
  }

  @Test
  public void shouldNotBeAbleToUseWsWithValidCredsIfUserHasNoAccessToThisCluster() throws Exception {
    assertThat(makeWsRequest(USER_NO_ACCESS, USER_NO_ACCESS_PWD), is(Code.FORBIDDEN));
  }

  @Test
  public void shouldBeAbleToUseCliWithValidCreds() {
    assertThat(canMakeCliRequest(USER_WITH_ACCESS, USER_WITH_ACCESS_PWD), is(Code.OK.getCode()));
  }

  @Test
  public void shouldBeAbleToUseWsWithValidCreds() throws Exception {
    assertThat(makeWsRequest(USER_WITH_ACCESS, USER_WITH_ACCESS_PWD), is(Code.OK));
  }

  private int canMakeCliRequest(final String username, final String password) {
    try (KsqlRestClient restClient = new KsqlRestClient(REST_APP.getHttpListener().toString())) {

      if (!username.isEmpty()) {
        restClient.setupAuthenticationCredentials(username, password);
      }

      final RestResponse<ServerInfo> response = restClient.getServerInfo();
      if (response.isSuccessful()) {
        return Code.OK.getCode();
      }

      return response.getErrorMessage().getErrorCode();
    }
  }

  private Code makeWsRequest(final String username, final String password) throws Exception {
    final WebSocketClient wsClient = new WebSocketClient();
    wsClient.start();

    try {
      final ClientUpgradeRequest request = new ClientUpgradeRequest();
      if (!username.isEmpty()) {
        final String authHeader = "Basic " + buildBasicAuthHeader(username, password);
        request.setHeader(HttpHeaders.AUTHORIZATION, authHeader);
      }

      final WebSocketListener listener = new WebSocketListener();
      final URI wsUri = REST_APP.getWsListener().resolve("/ws/query");

      wsClient.connect(listener, wsUri, request);

      assertThat("Response received",
          listener.latch.await(30, TimeUnit.SECONDS), is(true));

      final Throwable error = listener.error.get();
      return error == null ? Code.OK : extractStatusCode(error);
    } finally {
      wsClient.stop();
    }
  }

  private static Code extractStatusCode(final Throwable message) {
    assertThat(message, is(instanceOf(UpgradeException.class)));
    return HttpStatus.getCode(((UpgradeException) message).getResponseStatusCode());
  }

  private static String buildBasicAuthHeader(final String userName, final String password) {
    return Base64.encodeAsString(userName + ":" + password);
  }

  private static String createJaasConfigContent() {
    try {
      final Path credFile = TMP_FOLDER.newFile("password-file").toPath();
      Files.write(credFile, BASIC_PASSWORDS_FILE_CONTENT.getBytes(Charsets.UTF_8));

      return PROPS_JAAS_REALM + " {\n  "
          + PropertyFileLoginModule.class.getName() + " required\n"
          + "  file=\"" + credFile + "\"\n"
          + "  debug=\"true\";\n"
          + "};\n";

    } catch (final Exception e) {
      throw new RuntimeException("Failed to create Jaas config", e);
    }
  }

  private static void createTmpFolder() {
    try {
      TMP_FOLDER.create();
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unused")
  @WebSocket
  public static class WebSocketListener {

    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicReference<Throwable> error = new AtomicReference<>();

    @OnWebSocketConnect
    public void onConnect(final Session session) {
      latch.countDown();
    }

    @OnWebSocketError
    public void onError(final Throwable t) {
      error.set(t);
      latch.countDown();
    }
  }
}
