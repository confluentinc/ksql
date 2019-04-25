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

package io.confluent.ksql.rest.integration;

import static io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster.VALID_USER1;
import static io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster.VALID_USER2;
import static io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster.ops;
import static io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster.prefixedResource;
import static io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster.resource;
import static org.apache.kafka.common.acl.AclOperation.ALL;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE_CONFIGS;
import static org.apache.kafka.common.resource.ResourceType.CLUSTER;
import static org.apache.kafka.common.resource.ResourceType.GROUP;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.rest.entity.Versions;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.serde.DataSource.DataSourceSerDe;
import io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.test.util.secure.ClientTrustStore;
import io.confluent.ksql.test.util.secure.Credentials;
import io.confluent.ksql.test.util.secure.SecureKafkaHelper;
import io.confluent.ksql.util.PageViewDataProvider;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.websocket.CloseReason.CloseCodes;
import javax.ws.rs.client.Client;
import javax.ws.rs.core.MediaType;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketError;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

@Category({IntegrationTest.class})
public class RestApiTest {

  private static final int HEADER = 1;  // <-- some responses include a header as the first message.
  private static final int LIMIT = 2;
  private static final String PAGE_VIEW_TOPIC = "pageviews";
  private static final String PAGE_VIEW_STREAM = "pageviews_original";
  private static final Credentials SUPER_USER = VALID_USER1;
  private static final Credentials NORMAL_USER = VALID_USER2;

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.builder()
      .withKafkaCluster(
          EmbeddedSingleNodeKafkaCluster.newBuilder()
              .withoutPlainListeners()
              .withSaslSslListeners()
              .withAclsEnabled(SUPER_USER.username)
              .withAcl(
                  NORMAL_USER,
                  resource(CLUSTER, "kafka-cluster"),
                  ops(DESCRIBE_CONFIGS)
              )
              .withAcl(
                  NORMAL_USER,
                  resource(TOPIC, "_confluent-ksql-default__command_topic"),
                  ops(ALL)
              )
              .withAcl(
                  NORMAL_USER,
                  resource(TOPIC, PAGE_VIEW_TOPIC),
                  ops(ALL)
              )
              .withAcl(
                  NORMAL_USER,
                  prefixedResource(GROUP, "_confluent-ksql-default_transient_"),
                  ops(ALL)
              )
      )
      .build();

  private static final TestKsqlRestApp REST_APP = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty("security.protocol", "SASL_SSL")
      .withProperty("sasl.mechanism", "PLAIN")
      .withProperty("sasl.jaas.config", SecureKafkaHelper.buildJaasConfig(NORMAL_USER))
      .withProperties(ClientTrustStore.trustStoreProps())
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain.outerRule(TEST_HARNESS).around(REST_APP);

  private Client restClient;

  @BeforeClass
  public static void setUpClass() {
    TEST_HARNESS.ensureTopics(PAGE_VIEW_TOPIC);

    TEST_HARNESS.produceRows(PAGE_VIEW_TOPIC, new PageViewDataProvider(), DataSourceSerDe.JSON);

    RestIntegrationTestUtil.createStreams(REST_APP, PAGE_VIEW_STREAM, PAGE_VIEW_TOPIC);
  }

  @Before
  public void setUp() {
    restClient = TestKsqlRestApp.buildClient();
  }

  @After
  public void cleanUp() {
    restClient.close();
  }

  @Test
  public void shouldExecuteStreamingQueryWithV1ContentType() throws Exception {
    // When:
    final List<String> messages = makeStreamingRequest(
        "SELECT * from " + PAGE_VIEW_STREAM + " LIMIT " + LIMIT + ";",
        Versions.KSQL_V1_JSON_TYPE,
        Versions.KSQL_V1_JSON_TYPE
    );

    // Then:
    assertThat(messages, hasSize(is(HEADER + LIMIT)));
  }

  @Test
  public void shouldExecuteStreamingQueryWithJsonContentType() throws Exception {
    // When:
    final List<String> messages = makeStreamingRequest(
        "SELECT * from " + PAGE_VIEW_STREAM + " LIMIT " + LIMIT + ";",
        MediaType.APPLICATION_JSON_TYPE,
        MediaType.APPLICATION_JSON_TYPE
    );

    // Then:
    assertThat(messages, hasSize(is(HEADER + LIMIT)));
  }

  @Test
  public void shouldPrintTopic() throws Exception {
    // When:
    final List<String> messages = makeStreamingRequest(
        "PRINT '" + PAGE_VIEW_TOPIC + "' FROM BEGINNING LIMIT " + LIMIT + ";",
        MediaType.APPLICATION_JSON_TYPE,
        MediaType.APPLICATION_JSON_TYPE);

    // Then:
    assertThat(messages, hasSize(is(LIMIT)));
  }

  private static List<String> makeStreamingRequest(
      final String sql,
      final MediaType mediaType,
      final MediaType contentType
  ) throws Exception {
    final WebSocketListener listener = new WebSocketListener();

    final WebSocketClient wsClient = RestIntegrationTestUtil.makeWsRequest(
        REST_APP.getWsListener(),
        sql,
        listener,
        Optional.of(mediaType),
        Optional.of(contentType),
        Optional.of(SUPER_USER)
    );

    try {
      return listener.awaitMessages();
    } finally {
      wsClient.stop();
    }
  }

  @SuppressWarnings("unused") // Invoked via reflection
  @WebSocket
  public static class WebSocketListener {

    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicReference<Throwable> error = new AtomicReference<>();
    final List<String> messages = new CopyOnWriteArrayList<>();

    @OnWebSocketMessage
    public void onMessage(final Session session, final String text) {
      messages.add(text);
    }

    @OnWebSocketClose
    public void onClose(final int statusCode, final String reason) {
      if (statusCode != CloseCodes.NORMAL_CLOSURE.getCode()) {
        error.set(new RuntimeException("non-normal close: " + reason + ", code: " + statusCode));
      }
      latch.countDown();
    }

    @OnWebSocketError
    public void onError(final Throwable t) {
      error.set(t);
      latch.countDown();
    }

    List<String> awaitMessages() {
      assertThat("Response received", await(), is(true));

      if (error.get() != null) {
        throw new AssertionError("Error in web socket listener:", error.get());
      }

      return messages;
    }

    private boolean await() {
      try {
        return latch.await(30, TimeUnit.SECONDS);
      } catch (final Exception e) {
        throw new AssertionError("Timed out waiting for WS response", e);
      }
    }
  }
}
