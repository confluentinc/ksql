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

package io.confluent.ksql.cli;

import static io.confluent.ksql.serde.FormatFactory.JSON;
import static io.confluent.ksql.serde.FormatFactory.KAFKA;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.junit.internal.matchers.ThrowableCauseMatcher.hasCause;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.UrlEscapers;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.rest.client.KsqlClient;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.KsqlRestClientException;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.test.util.secure.ClientTrustStore;
import io.confluent.ksql.test.util.secure.ServerKeyStore;
import io.confluent.ksql.util.OrderDataProvider;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLHandshakeException;

import io.netty.handler.codec.http.websocketx.WebSocketHandshakeException;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

@Category({IntegrationTest.class})
public class SslClientAuthFunctionalTest {

  private static final ServerKeyStore SERVER_KEY_STORE = new ServerKeyStore();

  private static final String TOPIC_NAME = new OrderDataProvider().topicName();

  private static final String JSON_KSQL_REQUEST = UrlEscapers.urlFormParameterEscaper()
      .escape("{"
          + " \"ksql\": \"PRINT " + TOPIC_NAME + " FROM BEGINNING;\""
          + "}");

  public static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  private static final TestKsqlRestApp REST_APP = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperties(SERVER_KEY_STORE.keyStoreProps())
      .withProperty(KsqlRestConfig.KSQL_SSL_KEYSTORE_ALIAS_EXTERNAL_CONFIG,
          SERVER_KEY_STORE.getKeyAlias())
      .withProperty(KsqlRestConfig.KSQL_SSL_KEYSTORE_ALIAS_INTERNAL_CONFIG,
          SERVER_KEY_STORE.getKeyAlias())
      .withProperty(KsqlRestConfig.SSL_CLIENT_AUTHENTICATION_CONFIG,
          KsqlRestConfig.SSL_CLIENT_AUTHENTICATION_REQUIRED)
      .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "https://localhost:0")
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS)
      .around(REST_APP);

  private Map<String, String> clientProps;

  @BeforeClass
  public static void classSetUp() {
    final OrderDataProvider dataProvider = new OrderDataProvider();
    TEST_HARNESS.getKafkaCluster().createTopics(TOPIC_NAME);
    TEST_HARNESS.produceRows(dataProvider.topicName(), dataProvider, KAFKA, JSON);
  }

  @Before
  public void setUp() {
    clientProps = ImmutableMap.of();
  }

  @Test
  public void shouldNotBeAbleToUseCliIfClientDoesNotProvideCertificate() {
    // Given:
    givenClientConfiguredWithoutCertificate();// Then:

    // When:
    final Exception e = assertThrows(
        KsqlRestClientException.class,
        this::canMakeCliRequest
    );

    // Then:
    assertThat(e.getCause(), (is(instanceOf(ExecutionException.class))));
    // Make this compatible with both java 8 and 11.
    assertThat(e.getCause(), anyOf(
            hasCause(hasCause(is(instanceOf(SSLHandshakeException.class)))),
            hasCause(is(instanceOf(SSLHandshakeException.class)))));
  }

  @Test
  public void shouldBeAbleToUseCliOverHttps() {
    // Given:
    givenClientConfiguredWithTls();

    // When:
    final int result = canMakeCliRequest();

    // Then:
    assertThat(result, is(OK.code()));
  }

  @Test
  public void shouldNotBeAbleToUseWssIfClientDoesNotTrustServerCert() {
    // Given:
    givenClientConfiguredWithoutCertificate();

    // When:
    final Exception e = assertThrows(
            Exception.class,
            () -> WebsocketUtils.makeWsRequest(JSON_KSQL_REQUEST, clientProps, REST_APP)
    );
    assertThat(e, anyOf(is(instanceOf(RuntimeException.class)),
            is(instanceOf(SSLHandshakeException.class))));
    if (e instanceof RuntimeException) {
      assertThat(e.getCause(), is(instanceOf(ExecutionException.class)));
      assertThat(e.getCause(), hasCause(instanceOf(WebSocketHandshakeException.class)));
    }
  }

  @Test
  public void shouldBeAbleToUseWss() throws Exception {
    // Given:
    givenClientConfiguredWithTls();

    // When:
    WebsocketUtils.makeWsRequest(JSON_KSQL_REQUEST, clientProps, REST_APP);

    // Then: did not throw.
  }

  private void givenClientConfiguredWithTls() {

    String clientKeystorePath = SERVER_KEY_STORE.keyStoreProps()
        .get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
    String clientKeystorePassword = SERVER_KEY_STORE.keyStoreProps()
        .get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
    String clientKeyPassword = SERVER_KEY_STORE.keyStoreProps()
        .get(SslConfigs.SSL_KEY_PASSWORD_CONFIG);

    // HTTP:
    clientProps = ImmutableMap.<String, String>builder()
        .putAll(ClientTrustStore.trustStoreProps())
        .put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, clientKeystorePath)
        .put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, clientKeystorePassword)
        .put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, clientKeyPassword)
        .put(KsqlClient.SSL_KEYSTORE_ALIAS_CONFIG, SERVER_KEY_STORE.getKeyAlias())
        .build();
  }

  private void givenClientConfiguredWithoutCertificate() {

    // HTTP:
    clientProps = ImmutableMap.<String, String>builder()
        .putAll(ClientTrustStore.trustStoreProps())
        .build();
  }

  private int canMakeCliRequest() {
    final String serverAddress = REST_APP.getHttpsListener().toString();

    try (KsqlRestClient restClient = KsqlRestClient.create(
        serverAddress,
        emptyMap(),
        clientProps,
        Optional.empty()
    )) {
      final RestResponse<?> response = restClient.makeKsqlRequest("show topics;");
      if (response.isSuccessful()) {
        return OK.code();
      }

      return response.getErrorMessage().getErrorCode();
    }
  }


}