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
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.junit.internal.matchers.ThrowableCauseMatcher.hasCause;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.UrlEscapers;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
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
import kafka.zookeeper.ZooKeeperClientException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

@Category({IntegrationTest.class})
public class SslFunctionalTest {

  private static final String TOPIC_NAME = new OrderDataProvider().topicName();
  private static final ServerKeyStore SERVER_KEY_STORE = new ServerKeyStore();

  private static final String JSON_KSQL_REQUEST = UrlEscapers.urlFormParameterEscaper()
      .escape("{"
          + " \"ksql\": \"PRINT " + TOPIC_NAME + " FROM BEGINNING;\""
          + "}");

  public static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  private static final TestKsqlRestApp REST_APP = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperties(SERVER_KEY_STORE.keyStoreProps())
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
    TEST_HARNESS.produceRows(dataProvider.topicName(), dataProvider, JSON);
  }

  @Before
  public void setUp() {
    clientProps = ImmutableMap.of();
  }

  @Test
  public void shouldNotBeAbleToUseCliIfClientDoesNotTrustServerCert() {

    // Given:
    givenClientConfguredWithoutTruststore();// Then:


    // When:
    final Exception e = assertThrows(
        KsqlRestClientException.class,
        () -> canMakeCliRequest()
    );

    // Then:
    assertThat(e.getCause(), (is(instanceOf(ExecutionException.class))));
    assertThat(e.getCause(), (hasCause(is(instanceOf(SSLHandshakeException.class)))));
  }

  @Test
  public void shouldBeAbleToUseCliOverHttps() {
    // Given:
    givenTrustStoreConfigured();

    // When:
    final int result = canMakeCliRequest();

    // Then:
    assertThat(result, is(OK.code()));
  }

  @Test
  public void shouldNotBeAbleToUseWssIfClientDoesNotTrustServerCert() throws Exception {
    // Given:
    givenClientConfguredWithoutTruststore();

    // When:
    assertThrows(
        SSLHandshakeException.class,
        () -> WebsocketUtils.makeWsRequest(JSON_KSQL_REQUEST, clientProps, REST_APP)
    );
  }

  @Test
  public void shouldBeAbleToUseWss() throws Exception {
    // Given:
    givenTrustStoreConfigured();

    // When:
    WebsocketUtils.makeWsRequest(JSON_KSQL_REQUEST, clientProps, REST_APP);

    // Then: did not throw.
  }

  private void givenTrustStoreConfigured() {
    // HTTP:
    clientProps = ImmutableMap.<String, String>builder()
        .putAll(ClientTrustStore.trustStoreProps())
        .build();
  }

  private void givenClientConfguredWithoutTruststore() {
    clientProps = ImmutableMap.of();
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