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

import static io.confluent.ksql.rest.Errors.ERROR_CODE_FORBIDDEN;
import static io.confluent.ksql.rest.Errors.ERROR_CODE_UNAUTHORIZED;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static io.vertx.core.http.HttpHeaders.AUTHORIZATION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.UrlEscapers;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.rest.client.BasicCredentials;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.ServerInfo;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.test.util.KsqlTestFolder;
import io.vertx.core.MultiMap;
import io.vertx.core.http.UpgradeRejectedException;
import io.vertx.core.json.JsonObject;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.common.security.JaasUtils;
import org.eclipse.jetty.jaas.spi.PropertyFileLoginModule;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

@Category({IntegrationTest.class})
public class BasicAuthFunctionalTest {

  private static final TemporaryFolder TMP_FOLDER = KsqlTestFolder.temporaryFolder();

  static {
    createTmpFolder();
  }

  private static final String PROPS_JAAS_REALM = "KsqlServer-Props";
  private static final String KSQL_CLUSTER_ID = "ksql-11";
  private static final BasicCredentials USER_WITH_ACCESS =
      BasicCredentials.of("harry", "changeme");
  private static final BasicCredentials USER_NO_ACCESS =
      BasicCredentials.of("tom", "changeme");
  private static final BasicCredentials UNKNOWN_USER =
      BasicCredentials.of("Unknown-user", "some password");

  private static final String BASIC_PASSWORDS_FILE_CONTENT =
      "# Each line generated using org.eclipse.jetty.util.security.Password\n"
          + USER_WITH_ACCESS.username() + ": " + USER_WITH_ACCESS.password() + "," + KSQL_CLUSTER_ID + "\n"
          + USER_NO_ACCESS.username() + ": " + USER_NO_ACCESS.password() + ",ksql-other\n";

  private static final EmbeddedSingleNodeKafkaCluster CLUSTER = EmbeddedSingleNodeKafkaCluster
      .newBuilder()
      .withAdditionalJaasConfig(createJaasConfigContent())
      .build();

  private static final TestKsqlRestApp REST_APP = TestKsqlRestApp
      .builder(CLUSTER::bootstrapServers)
      .withProperty(KsqlRestConfig.AUTHENTICATION_METHOD_CONFIG,
          KsqlRestConfig.AUTHENTICATION_METHOD_BASIC)
      .withProperty(KsqlRestConfig.AUTHENTICATION_REALM_CONFIG, PROPS_JAAS_REALM)
      .withProperty(KsqlRestConfig.AUTHENTICATION_ROLES_CONFIG, KSQL_CLUSTER_ID)
      .withProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, CLUSTER.getJaasConfigPath())
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(CLUSTER)
      .around(REST_APP);

  @Test
  public void shouldNotBeAbleToUseWsWithNoCreds() throws Exception {
    assertThat(makeWsRequest(Optional.empty()), is(UNAUTHORIZED.code()));
  }

  @Test
  public void shouldNotBeAbleToUseCliWithInvalidPassword() {
    // Given:
    final BasicCredentials wrongPassword = BasicCredentials.of(USER_NO_ACCESS.username(), "wrong");

    // Then:
    assertThat(canMakeCliRequest(wrongPassword), is(ERROR_CODE_UNAUTHORIZED));
  }

  @Test
  public void shouldNotBeAbleToUseWsWithInvalidPassword() throws Exception {
    // Given:
    final BasicCredentials wrongPassword = BasicCredentials.of(USER_NO_ACCESS.username(), "wrong");

    // Then:
    assertThat(makeWsRequest(Optional.of(wrongPassword)), is(UNAUTHORIZED.code()));
  }

  @Test
  public void shouldNotBeAbleToUseCliWithUnknownUser() {
    assertThat(canMakeCliRequest(UNKNOWN_USER), is(ERROR_CODE_UNAUTHORIZED));
  }

  @Test
  public void shouldNotBeAbleToUseWsWithUnknownUser() throws Exception {
    assertThat(makeWsRequest(Optional.of(UNKNOWN_USER)), is(UNAUTHORIZED.code()));
  }

  @Test
  public void shouldNotBeAbleToUseCliWithValidCredsIfUserHasNoAccessToThisCluster() {
    assertThat(canMakeCliRequest(USER_NO_ACCESS), is(ERROR_CODE_FORBIDDEN));
  }

  @Test
  public void shouldNotBeAbleToUseWsWithValidCredsIfUserHasNoAccessToThisCluster() throws Exception {
    assertThat(makeWsRequest(Optional.of(USER_NO_ACCESS)), is(FORBIDDEN.code()));
  }

  @Test
  public void shouldBeAbleToUseCliWithValidCreds() {
    assertThat(canMakeCliRequest(USER_WITH_ACCESS), is(OK.code()));
  }

  @Test
  public void shouldBeAbleToUseWsWithValidCreds() throws Exception {
    assertThat(makeWsRequest(Optional.of(USER_WITH_ACCESS)), is(OK.code()));
  }

  private static int canMakeCliRequest(final BasicCredentials credentials) {
    try (KsqlRestClient restClient = KsqlRestClient.create(
        REST_APP.getHttpListener().toString(),
        ImmutableMap.of(),
        ImmutableMap.of(),
        Optional.of(credentials),
        Optional.empty()
    )) {
      final RestResponse<ServerInfo> response = restClient.getServerInfo();
      if (response.isSuccessful()) {
        return OK.code();
      }

      return response.getErrorMessage().getErrorCode();
    }
  }

  private static int makeWsRequest(final Optional<BasicCredentials> creds) throws Exception {
    JsonObject request = new JsonObject()
        .put("ksql", "SELECT * FROM KSQL_PROCESSING_LOG EMIT CHANGES;");
    MultiMap headers = MultiMap.caseInsensitiveMultiMap();
    creds.ifPresent(c -> {
      final String authHeader = "Basic " + buildBasicAuthHeader(c.username(), c.password());
      headers.add(AUTHORIZATION.toString(), authHeader);
    });
    try {
      String escaped = UrlEscapers.urlFormParameterEscaper().escape(request.toString());
      WebsocketUtils
          .makeWsRequest(escaped, Collections.emptyMap(), headers, REST_APP, false);
    } catch (UpgradeRejectedException e) {
      return e.getStatus();
    }
    return OK.code();
  }

  private static String buildBasicAuthHeader(final String userName, final String password) {
    final String credentials = userName + ":" + password;
    return Base64.getEncoder().encodeToString(credentials.getBytes(Charset.defaultCharset()));
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

}
