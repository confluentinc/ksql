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
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static io.vertx.core.http.HttpMethod.POST;
import static io.vertx.core.http.HttpVersion.HTTP_1_1;
import static io.vertx.core.http.HttpVersion.HTTP_2;
import static org.apache.kafka.common.acl.AclOperation.ALL;
import static org.apache.kafka.common.acl.AclOperation.CREATE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE_CONFIGS;
import static org.apache.kafka.common.acl.AclOperation.WRITE;
import static org.apache.kafka.common.resource.ResourceType.CLUSTER;
import static org.apache.kafka.common.resource.ResourceType.GROUP;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;
import static org.apache.kafka.common.resource.ResourceType.TRANSACTIONAL_ID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.api.utils.QueryResponse;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.rest.ApiJsonMapper;
import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.entity.CommandId.Action;
import io.confluent.ksql.rest.entity.CommandId.Type;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatus.Status;
import io.confluent.ksql.rest.entity.CommandStatuses;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlMediaType;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.Queries;
import io.confluent.ksql.rest.entity.QueryStreamArgs;
import io.confluent.ksql.rest.entity.RunningQuery;
import io.confluent.ksql.rest.entity.ServerClusterId;
import io.confluent.ksql.rest.entity.ServerInfo;
import io.confluent.ksql.rest.entity.ServerMetadata;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.test.util.secure.ClientTrustStore;
import io.confluent.ksql.test.util.secure.Credentials;
import io.confluent.ksql.test.util.secure.SecureKafkaHelper;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlRequestConfig;
import io.confluent.ksql.util.PageViewDataProvider;
import io.confluent.ksql.util.PageViewDataProvider.Batch;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.TestDataProvider;
import io.confluent.ksql.util.TombstoneProvider;
import io.confluent.ksql.util.VertxCompletableFuture;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpVersion;
import io.vertx.ext.web.client.HttpResponse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.ws.rs.core.MediaType;
import org.apache.hc.core5.http.HttpStatus;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

  @Category({IntegrationTest.class})
public class RestApiTest {

    private static final Logger LOG = LoggerFactory.getLogger(RestApiTest.class);

  private static final int HEADER = 1;  // <-- some responses include a header as the first message.
  private static final int FOOTER = 1;  // <-- some responses include a footer as the last message.
  private static final int LIMIT = 2;

  private static final PageViewDataProvider PAGE_VIEWS_PROVIDER = new PageViewDataProvider();
  private static final String PAGE_VIEW_TOPIC = PAGE_VIEWS_PROVIDER.topicName();
  private static final String PAGE_VIEW_STREAM = PAGE_VIEWS_PROVIDER.sourceName();

  // Used to test scalable push queries since we produce to the topics in the tests
  private static final PageViewDataProvider PAGE_VIEWS2_PROVIDER = new PageViewDataProvider(
      "PAGEVIEWS2", Batch.BATCH3);
  private static final String PAGE_VIEW2_TOPIC = PAGE_VIEWS2_PROVIDER.topicName();
  private static final String PAGE_VIEW2_STREAM = PAGE_VIEWS2_PROVIDER.sourceName();

  private static final TestDataProvider TOMBSTONE_PROVIDER = new TombstoneProvider();
  private static final String TOMBSTONE_TOPIC = TOMBSTONE_PROVIDER.topicName();
  private static final String TOMBSTONE_TABLE = TOMBSTONE_PROVIDER.sourceName();

  private static final String AGG_TABLE = "AGG_TABLE";
  private static final String PAGE_VIEW_CSAS = "PAGE_VIEW_CSAS";
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
                  resource(TOPIC, PAGE_VIEW2_TOPIC),
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
                  resource(TOPIC, "Y"),
                  ops(ALL)
              )
              .withAcl(
                  NORMAL_USER,
                  resource(TOPIC, AGG_TABLE),
                  ops(ALL)
              )
              .withAcl(
                  NORMAL_USER,
                  resource(TOPIC, TOMBSTONE_TOPIC),
                  ops(ALL)
              )
              .withAcl(
                  NORMAL_USER,
                  resource(TOPIC, PAGE_VIEW_CSAS),
                  ops(ALL)
              )
              .withAcl(
                  NORMAL_USER,
                  resource(TRANSACTIONAL_ID, "default_"),
                  ops(WRITE)
              )
              .withAcl(
                  NORMAL_USER,
                  resource(TRANSACTIONAL_ID, "default_"),
                  ops(DESCRIBE)
              )
      )
      .build();

  private static final TestKsqlRestApp REST_APP = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty("security.protocol", "SASL_SSL")
      .withProperty("sasl.mechanism", "PLAIN")
      .withProperty("sasl.jaas.config", SecureKafkaHelper.buildJaasConfig(NORMAL_USER))
      .withProperties(ClientTrustStore.trustStoreProps())
      .withProperty(KsqlConfig.KSQL_QUERY_PUSH_V2_REGISTRY_INSTALLED, true)
      .withProperty(KsqlConfig.KSQL_QUERY_PUSH_V2_ENABLED, true)
      .withProperty(KsqlConfig.KSQL_QUERY_PUSH_V2_NEW_LATEST_DELAY_MS, 0L)
      .withProperty(KsqlConfig.KSQL_QUERY_STREAM_PULL_QUERY_ENABLED, true)
      .withProperty(KsqlConfig.KSQL_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR_ENABLED, true)
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain.outerRule(TEST_HARNESS).around(REST_APP);

  @Rule
  public final Retry retry = Retry.of(5, AssertionError.class, 3, TimeUnit.SECONDS);

  @Rule
  public final Timeout timeout = Timeout.seconds(60);

  private ServiceContext serviceContext;

  @BeforeClass
  public static void setUpClass() {
    TEST_HARNESS.ensureTopics(PAGE_VIEW_TOPIC, PAGE_VIEW2_TOPIC);

    TEST_HARNESS.produceRows(PAGE_VIEW_TOPIC, PAGE_VIEWS_PROVIDER, FormatFactory.KAFKA, FormatFactory.JSON);
    TEST_HARNESS.produceRows(TOMBSTONE_TOPIC, TOMBSTONE_PROVIDER, FormatFactory.KAFKA, FormatFactory.JSON);

    RestIntegrationTestUtil.createStream(REST_APP, PAGE_VIEWS_PROVIDER);
    RestIntegrationTestUtil.createStream(REST_APP, PAGE_VIEWS2_PROVIDER);
    RestIntegrationTestUtil.createTable(REST_APP, TOMBSTONE_PROVIDER);

    makeKsqlRequest("CREATE TABLE " + AGG_TABLE + " AS "
        + "SELECT USERID, COUNT(1) AS COUNT FROM " + PAGE_VIEW_STREAM + " GROUP BY USERID;"
    );

    makeKsqlRequest("CREATE STREAM " + PAGE_VIEW_CSAS + " AS "
        + "SELECT * FROM " + PAGE_VIEW2_STREAM + ";"
    );
  }

  @Before
  public void setUp() {
    verifyNoPushQueries();
  }

  @After
  public void tearDown() {
    verifyNoPushQueries();

    if (serviceContext != null) {
      serviceContext.close();
    }
  }

  @AfterClass
  public static void classTearDown() {
    REST_APP.getPersistentQueries().forEach(str -> makeKsqlRequest("TERMINATE " + str + ";"));
  }

  @Test
  public void shouldExecutePushQueryThatReturnsStreamOverWebSocketWithV1ContentType() {
    // When:
    final List<String> messages = makeWebSocketRequest(
        "SELECT * from " + PAGE_VIEW_STREAM + " EMIT CHANGES LIMIT " + LIMIT + ";",
        KsqlMediaType.KSQL_V1_JSON.mediaType(),
        KsqlMediaType.KSQL_V1_JSON.mediaType(),
        Optional.empty(),
        Optional.empty()
    );

    // Then:
    assertThat(messages, hasSize(HEADER + LIMIT + 1));
    assertValidJsonMessages(messages);
    assertThat(messages.get(0), is("["
        + "{\"name\":\"PAGEID\",\"schema\":{\"type\":\"STRING\",\"fields\":null,\"memberSchema\":null}},"
        + "{\"name\":\"USERID\",\"schema\":{\"type\":\"STRING\",\"fields\":null,\"memberSchema\":null}},"
        + "{\"name\":\"VIEWTIME\",\"schema\":{\"type\":\"BIGINT\",\"fields\":null,\"memberSchema\":null}}"
        + "]"));
    assertThat(messages.get(1), is(
        "{\"row\":{\"columns\":[\"PAGE_1\",\"USER_1\",1]}}"
    ));
    assertThat(messages.get(2), is(
        "{\"row\":{\"columns\":[\"PAGE_2\",\"USER_2\",2]}}"
    ));
  }

  @Test
  public void shouldExecutePushQueryThatReturnsTableOverWebSocketWithV1ContentType() {
    // When:
    final List<String> messages = makeWebSocketRequest(
        "SELECT VAL from " + TOMBSTONE_TABLE + " EMIT CHANGES LIMIT " + LIMIT + ";",
        KsqlMediaType.KSQL_V1_JSON.mediaType(),
        KsqlMediaType.KSQL_V1_JSON.mediaType(),
        Optional.empty(),
        Optional.empty()
    );

    // Then:
    assertThat(messages, hasSize(HEADER + LIMIT + 1));
    assertValidJsonMessages(messages);
    assertThat(messages.get(0), is("["
        + "{\"name\":\"VAL\",\"schema\":{\"type\":\"STRING\",\"fields\":null,\"memberSchema\":null}}"
        + "]"));
    assertThat(messages.get(1), is(
        "{\"row\":{\"columns\":[\"a\"]}}"
    ));
    assertThat(messages.get(2), is(
        "{\"row\":{\"columns\":[\"b\"]}}"
    ));
  }

  @Test
  public void shouldExecutePushQueryThatReturnsStreamOverWebSocketWithJsonContentType() {
    // When:
    final List<String> messages = makeWebSocketRequest(
        "SELECT * from " + PAGE_VIEW_STREAM + " EMIT CHANGES LIMIT " + LIMIT + ";",
        MediaType.APPLICATION_JSON,
        MediaType.APPLICATION_JSON,
        Optional.empty(),
        Optional.empty()
    );

    // Then:
    assertThat(messages, hasSize(HEADER + LIMIT + 1));
    assertValidJsonMessages(messages);
    assertThat(messages.get(0), is("["
        + "{\"name\":\"PAGEID\",\"schema\":{\"type\":\"STRING\",\"fields\":null,\"memberSchema\":null}},"
        + "{\"name\":\"USERID\",\"schema\":{\"type\":\"STRING\",\"fields\":null,\"memberSchema\":null}},"
        + "{\"name\":\"VIEWTIME\",\"schema\":{\"type\":\"BIGINT\",\"fields\":null,\"memberSchema\":null}}"
        + "]"));
    assertThat(messages.get(1), is(
        "{\"row\":{\"columns\":[\"PAGE_1\",\"USER_1\",1]}}"
    ));
    assertThat(messages.get(2), is(
        "{\"row\":{\"columns\":[\"PAGE_2\",\"USER_2\",2]}}"
    ));
  }

  @Test
  public void shouldExecutePullQueryThatReturnsStreamOverWebSocketWithJsonContentType() {
    // When:
    final List<String> messages = makeWebSocketRequest(
        "SELECT * from " + PAGE_VIEW_STREAM + ";",
        MediaType.APPLICATION_JSON,
        MediaType.APPLICATION_JSON,
        Optional.empty(),
        Optional.empty()
    );

    // Then:
    assertThat(messages, equalTo(
        ImmutableList.of(
            "[{\"name\":\"PAGEID\",\"schema\":{\"type\":\"STRING\",\"fields\":null,\"memberSchema\":null}}"
                + ",{\"name\":\"USERID\",\"schema\":{\"type\":\"STRING\",\"fields\":null,\"memberSchema\":null}}"
                + ",{\"name\":\"VIEWTIME\",\"schema\":{\"type\":\"BIGINT\",\"fields\":null,\"memberSchema\":null}}]",
            "{\"row\":{\"columns\":[\"PAGE_1\",\"USER_1\",1]}}",
            "{\"row\":{\"columns\":[\"PAGE_2\",\"USER_2\",2]}}",
            "{\"row\":{\"columns\":[\"PAGE_3\",\"USER_4\",3]}}",
            "{\"row\":{\"columns\":[\"PAGE_4\",\"USER_3\",4]}}",
            "{\"row\":{\"columns\":[\"PAGE_5\",\"USER_0\",5]}}",
            "{\"row\":{\"columns\":[\"PAGE_5\",\"USER_2\",6]}}",
            "{\"row\":{\"columns\":[\"PAGE_5\",\"USER_3\",7]}}",
            // This is a bit weird, but it's clearly what the code is meant to produce.
            // I'm unsure if it's ok to change this to make more sense, or if user code depends
            // on this completion message.
            "{\"error\":\"done\"}"
        )
    ));
  }

  @Test
  public void shouldExecutePushQueryThatReturnsTableOverWebSocketWithJsonContentType() {
    // When:
    final List<String> messages = makeWebSocketRequest(
        "SELECT VAL from " + TOMBSTONE_TABLE + " EMIT CHANGES LIMIT " + LIMIT + ";",
        MediaType.APPLICATION_JSON,
        MediaType.APPLICATION_JSON,
        Optional.empty(),
        Optional.empty()
    );

    // Then:
    assertThat(messages, hasSize(HEADER + LIMIT + 1));
    assertValidJsonMessages(messages);
    assertThat(messages.get(0), is("["
        + "{\"name\":\"VAL\",\"schema\":{\"type\":\"STRING\",\"fields\":null,\"memberSchema\":null}}"
        + "]"));
    assertThat(messages.get(1), is(
        "{\"row\":{\"columns\":[\"a\"]}}"
    ));
    assertThat(messages.get(2), is(
        "{\"row\":{\"columns\":[\"b\"]}}"
    ));
  }

  @Test
  public void shouldExecuteInfoRequest() {
    // When:
    final ServerInfo response = RestIntegrationTestUtil.makeInfoRequest(REST_APP);

    // Then:
    assertThat(response.getVersion(), is(notNullValue()));
  }

  @Test
  public void shouldExecuteStatusRequest() {

    // When:
    String commandId = "stream/`" + PAGE_VIEW_STREAM + "`/create";
    final CommandStatus response = RestIntegrationTestUtil.makeStatusRequest(REST_APP, commandId);

    // Then:
    assertThat(response.getStatus(), is(Status.SUCCESS));
  }

  @Test
  public void shouldExecuteStatusesRequest() {

    // When:
    final CommandStatuses response = RestIntegrationTestUtil.makeStatusesRequest(REST_APP);

    // Then:
    CommandId expected = new CommandId(Type.STREAM, "`" + PAGE_VIEW_STREAM + "`", Action.CREATE);
    assertThat(response.containsKey(expected), is(true));
  }

  @Test
  public void shouldExecuteServerMetadataRequest() {
    // When:
    final ServerMetadata response = RestIntegrationTestUtil.makeServerMetadataRequest(REST_APP);

    // Then:
    assertThat(response.getVersion(), is(notNullValue()));
    assertThat(response.getClusterId(), is(notNullValue()));
  }

  @Test
  public void shouldExecuteServerMetadataIdRequest() {
    // When:
    final ServerClusterId response = RestIntegrationTestUtil.makeServerMetadataIdRequest(REST_APP);

    // Then:
    assertThat(response, is(notNullValue()));
  }

  @Test
  public void shouldExecuteRootDocumentRequest() {

    HttpResponse<Buffer> resp = RestIntegrationTestUtil
        .rawRestRequest(REST_APP, HttpVersion.HTTP_1_1, HttpMethod.GET,
            "/", null);

    // Then
    assertThat(resp.statusCode(), is(HttpStatus.SC_TEMPORARY_REDIRECT));
    assertThat(resp.getHeader("location"), is("/info"));
  }

  @Test
  public void shouldRejectPushQueryWithUnsupportedMediaType() {
    // When:
    final int response = failingRestQueryRequest(
        "SELECT USERID, PAGEID, VIEWTIME from " + PAGE_VIEW_STREAM + " EMIT CHANGES LIMIT "
            + LIMIT + ";",
        "unsupported/type"
    );

    // Then:
    assertThat(response, is(HttpStatus.SC_NOT_ACCEPTABLE));
  }

  @Test
  public void shouldExecutePushQueryThatReturnsStreamOverRestV1() {
    // When:
    final String response = rawRestQueryRequest(
        "SELECT USERID, PAGEID, VIEWTIME from " + PAGE_VIEW_STREAM + " EMIT CHANGES LIMIT "
            + LIMIT + ";",
        KsqlMediaType.KSQL_V1_JSON.mediaType()
    );

    // Then:
    assertThat(parseRawRestQueryResponse(response), hasSize(HEADER + LIMIT + FOOTER));
    final List<String> messages = Arrays.stream(response.split(System.lineSeparator()))
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());
    assertThat(messages, hasSize(HEADER + LIMIT + FOOTER));
    assertThat(messages.get(0),
        startsWith("[{\"header\":{\"queryId\":\""));
    assertThat(messages.get(0),
        endsWith("\",\"schema\":\"`USERID` STRING, `PAGEID` STRING, `VIEWTIME` BIGINT\"}},"));
    assertThat(messages.get(1), is("{\"row\":{\"columns\":[\"USER_1\",\"PAGE_1\",1]}},"));
    assertThat(messages.get(2), is("{\"row\":{\"columns\":[\"USER_2\",\"PAGE_2\",2]}},"));
    assertThat(messages.get(3), is("{\"finalMessage\":\"Limit Reached\"}]"));
  }

  @Test
  public void shouldExecutePullQueryThatReturnsStreamOverRestV1AndIgnoreReset() {
    // When:
    final KsqlRequest request =
        new KsqlRequest(
            "SELECT USERID, PAGEID, VIEWTIME from " + PAGE_VIEW_STREAM + ";",
            ImmutableMap.of("auto.offset.reset", "latest"), // should get ignored
            ImmutableMap.of(),
            null
        );

    final String response = RestIntegrationTestUtil.rawRestRequest(REST_APP, HTTP_1_1, POST,
            "/query", request, KsqlMediaType.KSQL_V1_JSON.mediaType(),
            Optional.empty())
        .body()
        .toString();

    // Then:

    assertThat(
        response.replaceFirst("queryId\":\"transient_[^\"]*\"", "queryId\":\"XYZ\""),
        equalTo(
            "[{\"header\":{\"queryId\":\"XYZ\",\"schema\":\"`USERID` STRING, `PAGEID` STRING, `VIEWTIME` BIGINT\"}},\n"
                + "{\"row\":{\"columns\":[\"USER_1\",\"PAGE_1\",1]}},\n"
                + "{\"row\":{\"columns\":[\"USER_2\",\"PAGE_2\",2]}},\n"
                + "{\"row\":{\"columns\":[\"USER_4\",\"PAGE_3\",3]}},\n"
                + "{\"row\":{\"columns\":[\"USER_3\",\"PAGE_4\",4]}},\n"
                + "{\"row\":{\"columns\":[\"USER_0\",\"PAGE_5\",5]}},\n"
                + "{\"row\":{\"columns\":[\"USER_2\",\"PAGE_5\",6]}},\n"
                + "{\"row\":{\"columns\":[\"USER_3\",\"PAGE_5\",7]}},\n"
                + "\n"
                + "]\n"
    )
    );
  }

  @Test
  public void shouldExecutePushQueryThatReturnsTableOverRestV1() {
    // When:
    final String response = rawRestQueryRequest(
        "SELECT VAL from " + TOMBSTONE_TABLE + " EMIT CHANGES LIMIT " + LIMIT + ";",
        KsqlMediaType.KSQL_V1_JSON.mediaType()
    );

    // Then:
    assertThat(parseRawRestQueryResponse(response), hasSize(HEADER + LIMIT + FOOTER));
    final List<String> messages = Arrays.stream(response.split(System.lineSeparator()))
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());
    assertThat(messages, hasSize(HEADER + LIMIT + FOOTER));
    assertThat(messages.get(0),
        startsWith("[{\"header\":{\"queryId\":\""));
    assertThat(messages.get(0), endsWith("\",\"schema\":\"`VAL` STRING\"}},"));
    assertThat(messages.get(1), is("{\"row\":{\"columns\":[\"a\"]}},"));
    assertThat(messages.get(2), is("{\"row\":{\"columns\":[null],\"tombstone\":true}},"));
    assertThat(messages.get(3), is("{\"finalMessage\":\"Limit Reached\"}]"));
  }

  @Test
  public void shouldExecutePushQueryThatReturnsStreamOverRest() {
    // When:
    final String response = rawRestQueryRequest(
        "SELECT USERID, PAGEID, VIEWTIME from " + PAGE_VIEW_STREAM + " EMIT CHANGES LIMIT "
            + LIMIT + ";",
        MediaType.APPLICATION_JSON
    );

    // Then:
    assertThat(parseRawRestQueryResponse(response), hasSize(HEADER + LIMIT + FOOTER));
    final List<String> messages = Arrays.stream(response.split(System.lineSeparator()))
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());
    assertThat(messages, hasSize(HEADER + LIMIT + FOOTER));
    assertThat(messages.get(0),
        startsWith("[{\"header\":{\"queryId\":\""));
    assertThat(messages.get(0),endsWith("\",\"schema\":\"`USERID` STRING, `PAGEID` STRING, `VIEWTIME` BIGINT\"}},"));
    assertThat(messages.get(1), is("{\"row\":{\"columns\":[\"USER_1\",\"PAGE_1\",1]}},"));
    assertThat(messages.get(2), is("{\"row\":{\"columns\":[\"USER_2\",\"PAGE_2\",2]}},"));
    assertThat(messages.get(3), is("{\"finalMessage\":\"Limit Reached\"}]"));
  }

  @Test
  public void shouldExecutePushQueryThatReturnsTableOverRest() {
    // When:
    final String response = rawRestQueryRequest(
        "SELECT VAL from " + TOMBSTONE_TABLE + " EMIT CHANGES LIMIT " + LIMIT + ";",
        MediaType.APPLICATION_JSON
    );

    // Then:
    assertThat(parseRawRestQueryResponse(response), hasSize(HEADER + LIMIT + FOOTER));
    final List<String> messages = Arrays.stream(response.split(System.lineSeparator()))
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());
    assertThat(messages, hasSize(HEADER + LIMIT + FOOTER));
    assertThat(messages.get(0),
        startsWith("[{\"header\":{\"queryId\":\""));
    assertThat(messages.get(0),endsWith("\",\"schema\":\"`VAL` STRING\"}},"));
    assertThat(messages.get(1), is("{\"row\":{\"columns\":[\"a\"]}},"));
    assertThat(messages.get(2), is("{\"row\":{\"columns\":[null],\"tombstone\":true}},"));
    assertThat(messages.get(3), is("{\"finalMessage\":\"Limit Reached\"}]"));
  }

  @Test
  public void shouldExecuteScalablePushQueryOverHttp2QueryStream()
      throws InterruptedException, ExecutionException {
    QueryStreamArgs queryStreamArgs = new QueryStreamArgs(
        "SELECT USERID, PAGEID, VIEWTIME from " + PAGE_VIEW_CSAS + " EMIT CHANGES LIMIT "
            + LIMIT + ";",
        ImmutableMap.of("auto.offset.reset", "latest"),
        Collections.emptyMap(), Collections.emptyMap());

    List<String> messages = new ArrayList<>();
    Semaphore start = new Semaphore(0);
    VertxCompletableFuture<Void> future = RestIntegrationTestUtil.rawRestRequest(REST_APP,
        HTTP_2, POST,
        "/query-stream", queryStreamArgs, "application/vnd.ksqlapi.delimited.v1",
        buffer -> {
          if (buffer == null || buffer.length() == 0) {
            return;
          }

          String str = buffer.toString();
          if (str.startsWith("{") && str.contains("\"queryId\"")) {
            start.release();
          }
          String[] parts = str.split("\n");
          messages.addAll(Arrays.asList(parts));
        });

    // Wait to get the metadata so we know we've started.
    start.acquire();
    assertExpectedScalablePushQueries(1);

    // Write some new rows
    TEST_HARNESS.produceRows(PAGE_VIEW2_TOPIC, PAGE_VIEWS2_PROVIDER, FormatFactory.KAFKA,
        FormatFactory.JSON);

    future.get();
    assertThat(messages.size(), is(HEADER + LIMIT));
    assertThat(messages.get(0),
        startsWith("{\"queryId\":\"SCALABLE_PUSH_QUERY_"));
    assertThat(messages.get(0), endsWith(",\"columnNames\":[\"USERID\",\"PAGEID\",\"VIEWTIME\"],"
        + "\"columnTypes\":[\"STRING\",\"STRING\",\"BIGINT\"]}"));
    assertThat(messages.get(1), is("[\"USER_4\",\"PAGE_1\",10]"));
    assertThat(messages.get(2), is("[\"USER_0\",\"PAGE_5\",11]"));
  }

  @Test
  public void shouldExecuteScalablePushQueryOverHttp1Query()
    throws InterruptedException, ExecutionException {
    KsqlRequest ksqlRequest = new KsqlRequest(
        "SELECT USERID, PAGEID, VIEWTIME from " + PAGE_VIEW_CSAS + " EMIT CHANGES LIMIT "
            + LIMIT + ";",
        ImmutableMap.of("auto.offset.reset", "latest"),
        Collections.emptyMap(), Collections.emptyMap(), null);

    List<String> messages = new ArrayList<>();
    Semaphore start = new Semaphore(0);
    StringBuffer sb = new StringBuffer();
    VertxCompletableFuture<Void> future = RestIntegrationTestUtil.rawRestRequest(REST_APP,
        HTTP_1_1, POST,
        "/query", ksqlRequest, KsqlMediaType.KSQL_V1_JSON.mediaType(),
        buffer -> {
          if (buffer == null || buffer.length() == 0) {
            return;
          }

          String bufferStr = buffer.toString();
          sb.append(bufferStr);
          if (!bufferStr.endsWith("\n")) {
            return;
          }

          String line = sb.toString();
          sb.setLength(0);
          if (line.startsWith("[") && line.contains("\"header\"")) {
            start.release();
          }
          String[] parts = line.split("\n");
          messages.addAll(
              Arrays.stream(parts)
                  .filter(part -> !(part.equals(",") || part.equals("]")))
                  .collect(Collectors.toList()));
        });

    // Wait to get the metadata so we know we've started.
    start.acquire();
    assertExpectedScalablePushQueries(1);

    // Write some new rows
    TEST_HARNESS.produceRows(PAGE_VIEW2_TOPIC, PAGE_VIEWS2_PROVIDER, FormatFactory.KAFKA,
        FormatFactory.JSON);

    future.get();
    assertThat(messages.size(), is(HEADER + LIMIT + FOOTER));
    assertThat(messages.get(0), startsWith("[{\"header\":{\"queryId\":\"SCALABLE_PUSH_QUERY_"));
    assertThat(messages.get(0),
        endsWith("\",\"schema\":\"`USERID` STRING, `PAGEID` STRING, `VIEWTIME` BIGINT\"}},"));
    assertThat(messages.get(1), is("{\"row\":{\"columns\":[\"USER_4\",\"PAGE_1\",10]}},"));
    assertThat(messages.get(2), is("{\"row\":{\"columns\":[\"USER_0\",\"PAGE_5\",11]}},"));
    assertThat(messages.get(3), is("{\"finalMessage\":\"Limit Reached\"}]"));
  }

  @Test
  public void shouldExecuteScalablePushQueryOverWebSocket()
      throws InterruptedException, ExecutionException {
    List<String> messages = new ArrayList<>();
    Semaphore start = new Semaphore(0);
    CompletableFuture<Void> future = makeWebSocketRequest(
        "SELECT USERID, PAGEID, VIEWTIME from " + PAGE_VIEW_CSAS + " EMIT CHANGES LIMIT "
            + LIMIT + ";",
        KsqlMediaType.KSQL_V1_JSON.mediaType(),
        KsqlMediaType.KSQL_V1_JSON.mediaType(),
        ImmutableMap.of("auto.offset.reset", "latest"),
        str -> {
          if (str == null || str.length() == 0) {
            return;
          }

          if (str.contains("\"schema\":{\"type\"")) {
            start.release();
          }
          String[] parts = str.split("\n");
          messages.addAll(
              Arrays.stream(parts)
                  .filter(part -> !(part.equals(",") || part.equals("]")))
                  .collect(Collectors.toList()));
        });

    // Wait to get the metadata so we know we've started.
    start.acquire();
    assertExpectedScalablePushQueries(1);

    // Write some new rows
    TEST_HARNESS.produceRows(PAGE_VIEW2_TOPIC, PAGE_VIEWS2_PROVIDER, FormatFactory.KAFKA,
        FormatFactory.JSON);

    future.get();
    assertThat(messages.size(), is(HEADER + LIMIT + FOOTER));
    assertThat(messages.get(0), is("["
        + "{\"name\":\"USERID\",\"schema\":{\"type\":\"STRING\",\"fields\":null,\"memberSchema\":null}},"
        + "{\"name\":\"PAGEID\",\"schema\":{\"type\":\"STRING\",\"fields\":null,\"memberSchema\":null}},"
        + "{\"name\":\"VIEWTIME\",\"schema\":{\"type\":\"BIGINT\",\"fields\":null,\"memberSchema\":null}}"
        + "]"));
    assertThat(messages.get(1), is("{\"row\":{\"columns\":[\"USER_4\",\"PAGE_1\",10]}}"));
    assertThat(messages.get(2), is("{\"row\":{\"columns\":[\"USER_0\",\"PAGE_5\",11]}}"));
    assertThat(messages.get(3), is("{\"error\":\"done\"}"));
  }

  @Test
  public void shouldExecutePullQueryOverWebSocketWithV1ContentType() {
    // When:
    final Supplier<List<String>> call = () -> makeWebSocketRequest(
        "SELECT * from " + AGG_TABLE + " WHERE USERID='" + AN_AGG_KEY + "';",
        KsqlMediaType.KSQL_V1_JSON.mediaType(),
        KsqlMediaType.KSQL_V1_JSON.mediaType(),
        Optional.empty(),
        Optional.empty()
    );

    // Then:
    final List<String> messages = assertThatEventually(call, hasSize(HEADER + 2));
    assertValidJsonMessages(messages);
    assertThat(messages.get(0),
        is("["
            + "{\"name\":\"USERID\",\"schema\":{\"type\":\"STRING\",\"fields\":null,\"memberSchema\":null},\"type\":\"KEY\"},"
            + "{\"name\":\"COUNT\",\"schema\":{\"type\":\"BIGINT\",\"fields\":null,\"memberSchema\":null}}"
            + "]"));

    assertThat(messages.get(1), is("{\"row\":{\"columns\":[\"USER_1\",1]}}"));
  }

  @Test
  public void shouldExecutePullQueryOverWebSocketWithJsonContentType() {
    // When:
    final Supplier<List<String>> call = () -> makeWebSocketRequest(
        "SELECT COUNT, USERID from " + AGG_TABLE + " WHERE USERID='" + AN_AGG_KEY + "';",
        MediaType.APPLICATION_JSON,
        MediaType.APPLICATION_JSON,
        Optional.empty(),
        Optional.empty()
    );

    // Then:
    final List<String> messages = assertThatEventually(call, hasSize(HEADER + 2));
    assertValidJsonMessages(messages);
    assertThat(messages.get(0),
        is("["
            + "{\"name\":\"COUNT\",\"schema\":{\"type\":\"BIGINT\",\"fields\":null,\"memberSchema\":null}},"
            + "{\"name\":\"USERID\",\"schema\":{\"type\":\"STRING\",\"fields\":null,\"memberSchema\":null},\"type\":\"KEY\"}"
            + "]"));
    assertThat(messages.get(1),
        is("{\"row\":{\"columns\":[1,\"USER_1\"]}}"));
  }

  @Test
  public void shouldReturnCorrectSchemaForPullQueryWithOnlyKeyInSelect() {
    // When:
    final Supplier<List<String>> call = () -> makeWebSocketRequest(
        "SELECT USERID from " + AGG_TABLE + " WHERE USERID='" + AN_AGG_KEY + "';",
        MediaType.APPLICATION_JSON,
        MediaType.APPLICATION_JSON,
        Optional.empty(),
        Optional.empty()
    );

    // Then:
    final List<String> messages = assertThatEventually(call, hasSize(HEADER + 2));
    assertValidJsonMessages(messages);
    assertThat(messages.get(0),
        is("["
            + "{\"name\":\"USERID\",\"schema\":{\"type\":\"STRING\",\"fields\":null,\"memberSchema\":null},\"type\":\"KEY\"}"
            + "]"));
    assertThat(messages.get(1),
        is("{\"row\":{\"columns\":[\"USER_1\"]}}"));
  }

  @Test
  public void shouldReturnCorrectSchemaForPullQueryWithOnlyValueColumnInSelect() {
    // When:
    final Supplier<List<String>> call = () -> makeWebSocketRequest(
        "SELECT COUNT from " + AGG_TABLE + " WHERE USERID='" + AN_AGG_KEY + "';",
        MediaType.APPLICATION_JSON,
        MediaType.APPLICATION_JSON,
        Optional.empty(),
        Optional.empty()
    );

    // Then:
    final List<String> messages = assertThatEventually(call, hasSize(HEADER + 2));
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
    // Given:
    final Supplier<List<String>> call = () -> {
      final String response = rawRestQueryRequest(
          "SELECT COUNT, USERID from " + AGG_TABLE + " WHERE USERID='" + AN_AGG_KEY + "';",
          MediaType.APPLICATION_JSON);
      return Arrays.asList(response.split(System.lineSeparator()));
    };

    // When:
    final List<String> messages = assertThatEventually(call, hasSize(HEADER + 1));

    // Then:
    assertThat(messages, hasSize(HEADER + 1));
    assertThat(messages.get(0), startsWith("[{\"header\":{\"queryId\":\""));
    assertThat(messages.get(0),
        endsWith("\",\"schema\":\"`COUNT` BIGINT, `USERID` STRING KEY\"}},"));
    assertThat(messages.get(1), is("{\"row\":{\"columns\":[1,\"USER_1\"]}}]"));
  }

  @Test
  public void shouldFailToExecutePullQueryOverRestHttp2() {
    // Given
    final KsqlRequest request = new KsqlRequest(
        "SELECT COUNT, USERID from " + AGG_TABLE + " WHERE USERID='" + AN_AGG_KEY + "';",
        ImmutableMap.of(),
        Collections.emptyMap(),
        null
    );
    final Supplier<Integer> call = () -> rawRestRequest(
        HttpVersion.HTTP_2, HttpMethod.POST, "/query", request
    ).statusCode();

    // When:
    assertThatEventually(call, is(METHOD_NOT_ALLOWED.code()));
  }

  @Test
  public void shouldExecutePullQueryOverHttp2QueryStream() {
      QueryStreamArgs queryStreamArgs = new QueryStreamArgs(
          "SELECT COUNT, USERID from " + AGG_TABLE + " WHERE USERID='" + AN_AGG_KEY + "';",
          Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());

      QueryResponse[] queryResponse = new QueryResponse[1];
      assertThatEventually(() -> {
        try {
          HttpResponse<Buffer> resp = RestIntegrationTestUtil.rawRestRequest(REST_APP,
              HTTP_2, POST,
              "/query-stream", queryStreamArgs, "application/vnd.ksqlapi.delimited.v1",
              Optional.empty());
          queryResponse[0] = new QueryResponse(resp.body().toString());
          return queryResponse[0].rows.size();
        } catch (Throwable t) {
          return Integer.MAX_VALUE;
        }
      }, is(1));
      assertThat(queryResponse[0].rows.get(0).getList(), is(ImmutableList.of(1, "USER_1")));
  }

  @Test
  public void shouldExecutePullQuery_allTypes() {
    ImmutableList<String> formats = ImmutableList.of(
        "application/vnd.ksqlapi.delimited.v1",
        KsqlMediaType.KSQL_V1_JSON.mediaType());
    ImmutableList<HttpVersion> httpVersions = ImmutableList.of(HTTP_1_1, HTTP_2);
    ImmutableList<String> endpoints = ImmutableList.of("/query-stream");

    for (String format : formats) {
      for (HttpVersion version : httpVersions) {
        for (String endpoint : endpoints) {
          LOG.info("Trying pull query combination {} {} {}", format, version, endpoint);
          Object requestBody;
          if (endpoint.equals("/query-stream")) {
            requestBody = new QueryStreamArgs(
                "SELECT COUNT, USERID from " + AGG_TABLE + " WHERE USERID='" + AN_AGG_KEY + "';",
                Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
          } else {
            fail("Unknown endpoint " + endpoint);
            return;
          }

          // It would be nice to check the output the same way between all of the formats, but this
          // is somewhat hard since they have different data, so we don't try.
          if (format.equals("application/vnd.ksqlapi.delimited.v1")) {
            QueryResponse[] queryResponse = new QueryResponse[1];
            assertThatEventually(() -> {
              try {
                HttpResponse<Buffer> resp = RestIntegrationTestUtil.rawRestRequest(REST_APP,
                    version, POST,
                    endpoint, requestBody, "application/vnd.ksqlapi.delimited.v1",
                    Optional.empty());
                queryResponse[0] = new QueryResponse(resp.body().toString());
                return queryResponse[0].rows.size();
              } catch (Throwable t) {
                return Integer.MAX_VALUE;
              }
            }, is(1));
            assertThat(queryResponse[0].rows.get(0).getList(), is(ImmutableList.of(1, "USER_1")));
          } else if (format.equals(KsqlMediaType.KSQL_V1_JSON.mediaType())) {
            final Supplier<List<String>> call = () -> {
              HttpResponse<Buffer> resp = RestIntegrationTestUtil.rawRestRequest(REST_APP,
                  version, POST,
                  endpoint, requestBody, KsqlMediaType.KSQL_V1_JSON.mediaType(),
                  Optional.empty());
              final String response = resp.body().toString();
              return Arrays.asList(response.split(System.lineSeparator()));
            };

            // When:
            final List<String> messages = assertThatEventually(call, hasSize(HEADER + 1 + FOOTER));
            // Then:
            assertThat(messages, hasSize(HEADER + 1 + FOOTER));
            assertThat(messages.get(0), startsWith("[{\"header\":{\"queryId\":\""));
            assertThat(messages.get(0),
                endsWith("\",\"schema\":\"`COUNT` BIGINT, `USERID` STRING KEY\"}},"));
            assertThat(messages.get(1), is("{\"row\":{\"columns\":[1,\"USER_1\"]}},"));
            assertThat(messages.get(2), is("{\"finalMessage\":\"Pull query complete\"}]"));
          } else {
            fail("Unknown format " + format);
            return;
          }
        }
      }
    }
  }

  @Ignore
  @Test
  public void shouldRoundTripCVPullQueryOverWebSocketWithJsonContentType() {
    // Given:
    Map<String, Object> configOverrides =  ImmutableMap.of(
        KsqlConfig.KSQL_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR_ENABLED, true);
    Map<String, Object> requestProperties = ImmutableMap.of(
        KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR, "");

    // When:
    final Supplier<List<String>> call = () -> makeWebSocketRequest(
        "SELECT COUNT, USERID from " + AGG_TABLE + " WHERE USERID='" + AN_AGG_KEY + "';",
        MediaType.APPLICATION_JSON,
        MediaType.APPLICATION_JSON,
        Optional.of(configOverrides),
        Optional.of(requestProperties)
    );

    // Then:
    final List<String> messages = assertThatEventually(call, hasSize(HEADER + 3));
    assertValidJsonMessages(messages);
    assertThat(messages.get(2), is("{\"consistencyToken\":{\"consistencyToken\":"
                                       + "\"eyJ2ZXJzaW9uIjoyLCJvZmZzZXRWZWN0b3IiOnsiZHVtbXkiOnsiNS"
                                       + "I6NSwiNiI6NiwiNyI6N319fQ==\"}}"));
  }

  @Ignore
  @Test
  public void shouldRoundTripCVPullQueryOverWebSocketWithV1ContentType() {
    // Given:
    Map<String, Object> configOverrides =  ImmutableMap.of(
        KsqlConfig.KSQL_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR_ENABLED, true);
    Map<String, Object> requestProperties = ImmutableMap.of(
        KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR, "");

    // When:
    final Supplier<List<String>> call = () -> makeWebSocketRequest(
        "SELECT * from " + AGG_TABLE + " WHERE USERID='" + AN_AGG_KEY + "';",
        KsqlMediaType.KSQL_V1_JSON.mediaType(),
        KsqlMediaType.KSQL_V1_JSON.mediaType(),
        Optional.of(configOverrides),
        Optional.of(requestProperties)
    );

    // Then:
    final List<String> messages = assertThatEventually(call, hasSize(HEADER + 3));
    assertValidJsonMessages(messages);
    assertThat(messages.get(2), is("{\"consistencyToken\":{\"consistencyToken\":"
                                       + "\"eyJ2ZXJzaW9uIjoyLCJvZmZzZXRWZWN0b3IiOnsiZHVtbXkiOnsiNS"
                                       + "I6NSwiNiI6NiwiNyI6N319fQ==\"}}"));
  }

  @Ignore
  @Test
  public void shouldPrintTopicOverWebSocket() {
    // When:
    final List<String> messages = makeWebSocketRequest(
        "PRINT '" + PAGE_VIEW_TOPIC + "' FROM BEGINNING LIMIT " + LIMIT + ";",
        MediaType.APPLICATION_JSON,
        MediaType.APPLICATION_JSON,
        Optional.empty(),
        Optional.empty());

    // Then:
    assertThat(messages, hasSize(LIMIT + 1));
  }

  @Test
  public void shouldDeleteTopic() {
    // Given:
    makeKsqlRequest("CREATE STREAM X AS SELECT * FROM " + PAGE_VIEW_STREAM + ";");
    final String query = REST_APP.getPersistentQueries().stream()
        .filter(q -> q.startsWith("CSAS_X_"))
        .findFirst()
        .orElseThrow(IllegalStateException::new);
    makeKsqlRequest("TERMINATE " + query + ";");

    assertThat("Expected topic X to be created", topicExists("X"));

    // When:
    makeKsqlRequest("DROP STREAM X DELETE TOPIC;");

    // Then:
    assertThat("Expected topic X to be deleted", !topicExists("X"));
  }

  @Test
  public void shouldCreateStreamWithVariableSubstitution() {
    // Given:
    // When:
    makeKsqlRequestWithVariables(
        "CREATE STREAM ${name} AS SELECT * FROM " + PAGE_VIEW_STREAM + " WHERE USERID='${id}';",
        ImmutableMap.of("id", "USER_1", "name", "Y")
    );

    // Then:
    final List<String> query = ((Queries) makeKsqlRequest("SHOW QUERIES;").get(0))
        .getQueries().stream().map(RunningQuery::getQueryString)
        .filter(q -> q.contains("WHERE (PAGEVIEW_KSTREAM.USERID = 'USER_1')"))
        .collect(Collectors.toList());
    assertThat(query.size(), is(1));
  }

  @Test
  public void shouldFailToExecuteQueryUsingRestWithHttp2() {
    // Given:
    KsqlRequest ksqlRequest = new KsqlRequest("SELECT * from " + AGG_TABLE + " EMIT CHANGES;",
        Collections.emptyMap(), Collections.emptyMap(), null);

    // When:
    HttpResponse<Buffer> resp = RestIntegrationTestUtil.rawRestRequest(REST_APP,
        HttpVersion.HTTP_2, HttpMethod.POST, "/query", ksqlRequest);

    // Then:
    assertThat(resp.statusCode(), is(405));
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

  private static List<KsqlEntity> makeKsqlRequest(final String sql) {
    return RestIntegrationTestUtil.makeKsqlRequest(REST_APP, sql);
  }

  private static void makeKsqlRequestWithVariables(final String sql, final Map<String, Object> variables) {
    RestIntegrationTestUtil.makeKsqlRequestWithVariables(REST_APP, sql, variables);
  }

  private static String rawRestQueryRequest(final String sql, final String mediaType) {
    return RestIntegrationTestUtil.rawRestQueryRequest(REST_APP, sql, mediaType)
        .body()
        .toString();
  }

  private static int failingRestQueryRequest(final String sql, final String mediaType) {
    return RestIntegrationTestUtil.rawRestQueryRequest(REST_APP, sql, mediaType)
        .statusCode();
  }

  private static List<Map<String, Object>> parseRawRestQueryResponse(final String response) {
    try {
      return ApiJsonMapper.INSTANCE.get().readValue(
          response,
          new TypeReference<List<Map<String, Object>>>() {
          }
      );
    } catch (final Exception e) {
      throw new AssertionError("Invalid JSON received: " + response, e);
    }
  }

  private static HttpResponse<Buffer> rawRestRequest(
      final HttpVersion httpVersion,
      final HttpMethod method,
      final String uri,
      final Object requestBody) {
    return RestIntegrationTestUtil.rawRestRequest(REST_APP, httpVersion, method, uri, requestBody);
  }

  private static List<String> makeWebSocketRequest(
      final String sql,
      final String mediaType,
      final String contentType,
      final Optional<Map<String, Object>> overrides,
      final Optional<Map<String, Object>> requestProperties
  ) {
    return RestIntegrationTestUtil.makeWsRequest(
        REST_APP.getWsListener(),
        sql,
        Optional.of(mediaType),
        Optional.of(contentType),
        Optional.of(SUPER_USER),
        overrides,
        requestProperties
    );
  }

  private static CompletableFuture<Void> makeWebSocketRequest(
      final String sql,
      final String mediaType,
      final String contentType,
      final Map<String, Object> overrides,
      final Consumer<String> chunkConsumer
  ) {
    return RestIntegrationTestUtil.makeWsRequest(
        REST_APP.getWsListener(),
        sql,
        Optional.of(mediaType),
        Optional.of(contentType),
        Optional.of(SUPER_USER),
        overrides,
        chunkConsumer
    );
  }

  private static void assertValidJsonMessages(final Iterable<String> messages) {
    for (final String msg : messages) {
      try {
        ApiJsonMapper.INSTANCE.get().readValue(msg, Object.class);
      } catch (final Exception e) {
        throw new AssertionError("Invalid JSON message received: " + msg, e);
      }
    }
  }

  private static void verifyNoPushQueries() {
    assertThatEventually(REST_APP::getTransientQueries, hasSize(0));
    assertThatEventually(() -> {
      for (final PersistentQueryMetadata metadata : REST_APP.getEngine().getPersistentQueries()) {
        if (metadata.getScalablePushRegistry().get().numRegistered() != 0) {
          return false;
        }
      }
      return true;
    }, is(true));
  }

  private static void assertExpectedScalablePushQueries(
      final int expectedScalablePushQueries
  ) {
    assertThatEventually(() -> {
      for (final PersistentQueryMetadata metadata : REST_APP.getEngine().getPersistentQueries()) {
        if (!metadata.getSinkName().isPresent()
            || !metadata.getSinkName().get().text().equals(PAGE_VIEW_CSAS)) {
          continue;
        }
        if (metadata.getScalablePushRegistry().get().latestNumRegistered()
            < expectedScalablePushQueries
            || !metadata.getScalablePushRegistry().get().latestHasAssignment()) {
          return false;
        }
      }
      return true;
    }, is(true));
  }
}
