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

import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster.VALID_USER1;
import static io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster.VALID_USER2;
import static io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster.ops;
import static io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster.prefixedResource;
import static io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster.resource;
import static junit.framework.TestCase.fail;
import static org.apache.kafka.common.acl.AclOperation.ALL;
import static org.apache.kafka.common.acl.AclOperation.CREATE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE_CONFIGS;
import static org.apache.kafka.common.resource.ResourceType.CLUSTER;
import static org.apache.kafka.common.resource.ResourceType.GROUP;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

import com.fasterxml.jackson.core.type.TypeReference;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.json.JsonMapper;
import io.confluent.ksql.rest.entity.Versions;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.test.util.secure.ClientTrustStore;
import io.confluent.ksql.test.util.secure.Credentials;
import io.confluent.ksql.test.util.secure.SecureKafkaHelper;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PageViewDataProvider;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import javax.websocket.CloseReason.CloseCodes;
import javax.ws.rs.core.MediaType;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketError;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

@Category({IntegrationTest.class})
public class RestApiTest {

  private static final int HEADER = 1;  // <-- some responses include a header as the first message.
  private static final int FOOTER = 1;  // <-- some responses include a footer as the last message.
  private static final int LIMIT = 2;
  private static final String PAGE_VIEW_TOPIC = "pageviews";
  private static final String PAGE_VIEW_STREAM = "pageviews_original";
  private static final String AGG_TABLE = "AGG_TABLE";
  private static final Credentials SUPER_USER = VALID_USER1;
  private static final Credentials NORMAL_USER = VALID_USER2;
  private static final String AN_AGG_KEY = "USER_1";

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.builder()
      .withKafkaCluster(
          EmbeddedSingleNodeKafkaCluster.newBuilder()
              .withoutPlainListeners()
              .withSaslSslListeners()
              .withAclsEnabled(SUPER_USER.username)
              .withAcl(
                  NORMAL_USER,
                  resource(CLUSTER, "kafka-cluster"),
                  ops(DESCRIBE_CONFIGS, CREATE)
              )
              .withAcl(
                  NORMAL_USER,
                  prefixedResource(TOPIC, "_confluent-ksql-default_"),
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
              .withAcl(
                  NORMAL_USER,
                  prefixedResource(GROUP, "_confluent-ksql-default_query"),
                  ops(ALL)
              )
              .withAcl(
                  NORMAL_USER,
                  resource(TOPIC, "X"),
                  ops(ALL)
              )
              .withAcl(
                  NORMAL_USER,
                  resource(TOPIC, "AGG_TABLE"),
                  ops(ALL)
              )
              .withAcl(
                  NORMAL_USER,
                  resource(TOPIC, "__consumer_offsets"),
                  ops(DESCRIBE)
              )
      )
      .build();

  private static final TestKsqlRestApp REST_APP = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty("security.protocol", "SASL_SSL")
      .withProperty("sasl.mechanism", "PLAIN")
      .withProperty("sasl.jaas.config", SecureKafkaHelper.buildJaasConfig(NORMAL_USER))
      .withProperty(KsqlConfig.KSQL_PULL_QUERIES_SKIP_ACCESS_VALIDATOR_CONFIG, true)
      .withProperties(ClientTrustStore.trustStoreProps())
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain.outerRule(TEST_HARNESS).around(REST_APP);

  private ServiceContext serviceContext;

  @BeforeClass
  public static void setUpClass() {
    TEST_HARNESS.ensureTopics(PAGE_VIEW_TOPIC);

    TEST_HARNESS.produceRows(PAGE_VIEW_TOPIC, new PageViewDataProvider(), Format.JSON);

    RestIntegrationTestUtil.createStreams(REST_APP, PAGE_VIEW_STREAM, PAGE_VIEW_TOPIC);

    makeKsqlRequest("CREATE TABLE " + AGG_TABLE + " AS "
        + "SELECT COUNT(1) AS COUNT FROM " + PAGE_VIEW_STREAM + " GROUP BY USERID;");
  }

  @After
  public void tearDown() {
    if (serviceContext != null) {
      serviceContext.close();
    }
  }

  @Test
  public void shouldExecutePushQueryOverWebSocketWithV1ContentType() {
    // When:
    final List<String> messages = makeWebSocketRequest(
        "SELECT * from " + PAGE_VIEW_STREAM + " EMIT CHANGES LIMIT " + LIMIT + ";",
        Versions.KSQL_V1_JSON_TYPE,
        Versions.KSQL_V1_JSON_TYPE
    );

    // Then:
    assertThat(messages, hasSize(HEADER + LIMIT));
    assertValidJsonMessages(messages);
  }

  @Test
  public void shouldExecutePushQueryOverWebSocketWithJsonContentType() {
    // When:
    final List<String> messages = makeWebSocketRequest(
        "SELECT * from " + PAGE_VIEW_STREAM + " EMIT CHANGES LIMIT " + LIMIT + ";",
        MediaType.APPLICATION_JSON_TYPE,
        MediaType.APPLICATION_JSON_TYPE
    );

    // Then:
    assertThat(messages, hasSize(HEADER + LIMIT));
    assertValidJsonMessages(messages);
  }

  @Test
  public void shouldExecutePushQueryOverRest() {
    // When:
    final String response = rawRestQueryRequest(
        "SELECT USERID, PAGEID, VIEWTIME, ROWKEY from " + PAGE_VIEW_STREAM + " EMIT CHANGES LIMIT "
            + LIMIT + ";"
    );

    // Then:
    assertThat(parseRawRestQueryResponse(response), hasSize(HEADER + LIMIT + FOOTER));
    final String[] messages = response.split(System.lineSeparator());
    assertThat(messages[0],
        is("[{\"header\":{\"queryId\":\"none\",\"schema\":\"`USERID` STRING, `PAGEID` STRING, `VIEWTIME` BIGINT, `ROWKEY` STRING\"}},"));
    assertThat(messages[1], is("{\"row\":{\"columns\":[\"USER_1\",\"PAGE_1\",1,\"1\"]}},"));
    assertThat(messages[2], is("{\"row\":{\"columns\":[\"USER_2\",\"PAGE_2\",2,\"2\"]}},"));
    assertThat(messages[3], is("{\"finalMessage\":\"Limit Reached\"}]"));
  }

  @Test
  public void shouldExecutePullQueryOverWebSocketWithV1ContentType() {
    // When:
    final Supplier<List<String>> call = () -> makeWebSocketRequest(
        "SELECT * from " + AGG_TABLE + " WHERE ROWKEY='" + AN_AGG_KEY + "';",
        Versions.KSQL_V1_JSON_TYPE,
        Versions.KSQL_V1_JSON_TYPE
    );

    // Then:
    final List<String> messages = assertThatEventually(call, hasSize(HEADER + 1));
    assertValidJsonMessages(messages);
    assertThat(messages.get(0),
        is("["
            + "{\"name\":\"ROWKEY\",\"schema\":{\"type\":\"STRING\",\"fields\":null,\"memberSchema\":null}},"
            + "{\"name\":\"COUNT\",\"schema\":{\"type\":\"BIGINT\",\"fields\":null,\"memberSchema\":null}}"
            + "]"));
    assertThat(messages.get(1),
        is("{\"row\":{\"columns\":[\"USER_1\",1]}}"));
  }

  @Test
  public void shouldExecutePullQueryOverWebSocketWithJsonContentType() {
    // When:
    final Supplier<List<String>> call = () -> makeWebSocketRequest(
        "SELECT COUNT, ROWKEY from " + AGG_TABLE + " WHERE ROWKEY='" + AN_AGG_KEY + "';",
        MediaType.APPLICATION_JSON_TYPE,
        MediaType.APPLICATION_JSON_TYPE
    );

    // Then:
    final List<String> messages = assertThatEventually(call, hasSize(HEADER + 1));
    assertValidJsonMessages(messages);
    assertThat(messages.get(0),
        is("["
            + "{\"name\":\"ROWKEY\",\"schema\":{\"type\":\"STRING\",\"fields\":null,\"memberSchema\":null}},"
            + "{\"name\":\"COUNT\",\"schema\":{\"type\":\"BIGINT\",\"fields\":null,\"memberSchema\":null}}"
            + "]"));
    assertThat(messages.get(1),
        is("{\"row\":{\"columns\":[1,\"USER_1\"]}}"));
  }

  @Test
  public void shouldReturnCorrectSchemaForPullQueryWithOnlyKeyInSelect() {
    // When:
    final Supplier<List<String>> call = () -> makeWebSocketRequest(
        "SELECT * from " + AGG_TABLE + " WHERE ROWKEY='" + AN_AGG_KEY + "';",
        MediaType.APPLICATION_JSON_TYPE,
        MediaType.APPLICATION_JSON_TYPE
    );

    // Then:
    final List<String> messages = assertThatEventually(call, hasSize(HEADER + 1));
    assertValidJsonMessages(messages);
    assertThat(messages.get(0),
        is("["
            + "{\"name\":\"ROWKEY\",\"schema\":{\"type\":\"STRING\",\"fields\":null,\"memberSchema\":null}},"
            + "{\"name\":\"COUNT\",\"schema\":{\"type\":\"BIGINT\",\"fields\":null,\"memberSchema\":null}}"
            + "]"));
    assertThat(messages.get(1),
        is("{\"row\":{\"columns\":[\"USER_1\",1]}}"));
  }

  @Test
  public void shouldReturnCorrectSchemaForPullQueryWithOnlyValueColumnInSelect() {
    // When:
    final Supplier<List<String>> call = () -> makeWebSocketRequest(
        "SELECT COUNT from " + AGG_TABLE + " WHERE ROWKEY='" + AN_AGG_KEY + "';",
        MediaType.APPLICATION_JSON_TYPE,
        MediaType.APPLICATION_JSON_TYPE
    );

    // Then:
    final List<String> messages = assertThatEventually(call, hasSize(HEADER + 1));
    assertValidJsonMessages(messages);
    assertThat(messages.get(0),
        is("["
            + "{\"name\":\"COUNT\",\"schema\":{\"type\":\"BIGINT\",\"fields\":null,\"memberSchema\":null}}"
            + "]"));
    assertThat(messages.get(1),
        is("{\"row\":{\"columns\":[1]}}"));
  }

  @Test
  public void shouldExecutePullQueryOverRest() {
    // When:
    final Supplier<List<String>> call = () -> {
      final String response = rawRestQueryRequest(
          "SELECT COUNT, ROWKEY from " + AGG_TABLE + " WHERE ROWKEY='" + AN_AGG_KEY + "';"
      );
      return Arrays.asList(response.split(System.lineSeparator()));
    };

    // Then:
    final List<String> messages = assertThatEventually(call, hasSize(HEADER + 1));
    assertThat(messages, hasSize(HEADER + 1));

    assertThat(messages.get(0), startsWith("[{\"header\":{\"queryId\":\""));
    assertThat(messages.get(0),
        endsWith("\",\"schema\":\"`COUNT` BIGINT, `ROWKEY` STRING KEY\"}},"));
    assertThat(messages.get(1), is("{\"row\":{\"columns\":[1,\"USER_1\"]}}]"));
  }

  @Test
  public void shouldReportErrorOnInvalidPullQueryOverRest() {
    // When:
    final String response = rawRestQueryRequest(
        "SELECT * from " + AGG_TABLE + ";"
    );

    // Then:
    assertThat(response, containsString("Missing WHERE clause"));
  }

  @Test
  public void shouldPrintTopicOverWebSocket() {
    // When:
    final List<String> messages = makeWebSocketRequest(
        "PRINT '" + PAGE_VIEW_TOPIC + "' FROM BEGINNING LIMIT " + LIMIT + ";",
        MediaType.APPLICATION_JSON_TYPE,
        MediaType.APPLICATION_JSON_TYPE);

    // Then:
    assertThat(messages, hasSize(LIMIT));
  }

  @Test
  public void shouldDeleteTopic() {
    // Given:
    makeKsqlRequest("CREATE STREAM X AS SELECT * FROM " + PAGE_VIEW_STREAM + ";"
        + "TERMINATE QUERY CSAS_X_2; ");

    assertThat("Expected topic X to be created", topicExists("X"));

    // When:
    makeKsqlRequest("DROP STREAM X DELETE TOPIC;");

    // Then:
    assertThat("Expected topic X to be deleted", !topicExists("X"));
  }

  private boolean topicExists(final String topicName) {
    return getServiceContext().getTopicClient().isTopicExists(topicName);
  }

  private ServiceContext getServiceContext() {
    if (serviceContext == null) {
      serviceContext = REST_APP.getServiceContext();
    }
    return serviceContext;
  }

  private static void makeKsqlRequest(final String sql) {
    RestIntegrationTestUtil.makeKsqlRequest(REST_APP, sql);
  }

  private static String rawRestQueryRequest(final String sql) {
    return RestIntegrationTestUtil.rawRestQueryRequest(REST_APP, sql, Optional.empty());
  }

  private static List<Map<String, Object>> parseRawRestQueryResponse(final String response) {
    try {
      return JsonMapper.INSTANCE.mapper.readValue(
          response,
          new TypeReference<List<Map<String, Object>>>() {
          }
      );
    } catch (final Exception e) {
      throw new AssertionError("Invalid JSON received: " + response);
    }
  }

  private static List<String> makeWebSocketRequest(
      final String sql,
      final MediaType mediaType,
      final MediaType contentType
  ) {
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
      try {
        wsClient.stop();
      } catch (final Exception e) {
        fail("Failed to close ws");
      }
    }
  }

  private static void assertValidJsonMessages(final Iterable<String> messages) {
    for (final String msg : messages) {
      try {
        JsonMapper.INSTANCE.mapper.readValue(msg, Object.class);
      } catch (final Exception e) {
        throw new AssertionError("Invalid JSON message received: " + msg, e);
      }
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
