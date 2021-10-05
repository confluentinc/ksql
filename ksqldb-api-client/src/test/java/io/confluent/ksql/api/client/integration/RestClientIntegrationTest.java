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
package io.confluent.ksql.api.client.integration;

import static io.confluent.ksql.util.KsqlConfig.KSQL_DEFAULT_KEY_FORMAT_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_STREAMS_PREFIX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.reactive.BaseSubscriber;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.client.StreamPublisher;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.integration.RestIntegrationTestUtil;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.util.ConsistencyOffsetVector;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.StructuredTypesDataProvider;
import io.confluent.ksql.util.TestDataProvider;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.reactivestreams.Subscription;


/**
 * This integration test is displaced from the rest-client package
 * to make use of utilities that are not available there.
 */
@Category(IntegrationTest.class)
public class RestClientIntegrationTest {

  private static final StructuredTypesDataProvider TEST_DATA_PROVIDER = new StructuredTypesDataProvider();
  private static final String TEST_TOPIC = TEST_DATA_PROVIDER.topicName();
  private static final String TEST_STREAM = TEST_DATA_PROVIDER.sourceName();

  private static final Format KEY_FORMAT = FormatFactory.JSON;
  private static final Format VALUE_FORMAT = FormatFactory.JSON;

  private static final String AGG_TABLE = "AGG_TABLE";
  private static final String AN_AGG_KEY = "STRUCT(F1 := ARRAY['a'])";
  private static final PhysicalSchema AGG_SCHEMA = PhysicalSchema.from(
      LogicalSchema.builder()
          .keyColumn(ColumnName.of("K"), SqlTypes.struct()
              .field("F1", SqlTypes.array(SqlTypes.STRING))
              .build())
          .valueColumn(ColumnName.of("LONG"), SqlTypes.BIGINT)
          .build(),
      SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES),
      SerdeFeatures.of()
  );

  private static final TestDataProvider EMPTY_TEST_DATA_PROVIDER = new TestDataProvider(
      "EMPTY_STRUCTURED_TYPES", TEST_DATA_PROVIDER.schema(), ImmutableListMultimap.of());
  private static final String EMPTY_TEST_TOPIC = EMPTY_TEST_DATA_PROVIDER.topicName();

  private static final TestDataProvider EMPTY_TEST_DATA_PROVIDER_2 = new TestDataProvider(
      "EMPTY_STRUCTURED_TYPES_2", TEST_DATA_PROVIDER.schema(), ImmutableListMultimap.of());
  private static final String EMPTY_TEST_TOPIC_2 = EMPTY_TEST_DATA_PROVIDER_2.topicName();

  private static final String PUSH_QUERY = "SELECT * FROM " + TEST_STREAM + " EMIT CHANGES;";
  private static final String PULL_QUERY_ON_TABLE =
      "SELECT * from " + AGG_TABLE + " WHERE K=" + AN_AGG_KEY + ";";

  private final static String serializedCT = "rO0ABXNyACdpby5jb25mbHVlbnQua3NxbC51dGlsLkNvbnNpc3Rlb"
      + "mN5VG9rZW4BxDEeQk6w5QIAAkkAB3ZlcnNpb25MABZ0YWJsZVBhcnRpdGlvbnNPZmZzZXRzdAAPTGphdmEvdXRpbC9"
      + "NYXA7eHAAAAABc3IAEWphdmEudXRpbC5IYXNoTWFwBQfawcMWYNEDAAJGAApsb2FkRmFjdG9ySQAJdGhyZXNob2xke"
      + "HA/QAAAAAAAA3cIAAAABAAAAAJ0AAV2aWNreXNyABNqYXZhLnV0aWwuQXJyYXlMaXN0eIHSHZnHYZ0DAAFJAARzaXp"
      + "leHAAAAADdwQAAAADc3IADmphdmEubGFuZy5Mb25nO4vkkMyPI98CAAFKAAV2YWx1ZXhyABBqYXZhLmxhbmcuTnVtY"
      + "mVyhqyVHQuU4IsCAAB4cAAAAAAAAAABc3EAfgAIAAAAAAAAAAJzcQB+AAgAAAAAAAAAA3h0AAVWaWNreXNyADZjb20"
      + "uZ29vZ2xlLmNvbW1vbi5jb2xsZWN0LkltbXV0YWJsZUxpc3QkU2VyaWFsaXplZEZvcm0AAAAAAAAAAAIAAVsACGVsZ"
      + "W1lbnRzdAATW0xqYXZhL2xhbmcvT2JqZWN0O3hwdXIAE1tMamF2YS5sYW5nLk9iamVjdDuQzlifEHMpbAIAAHhwAAA"
      + "AA3EAfgAKcQB+AAtxAH4ADHg=";

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  // these properties are set together to allow us to verify that we can handle push queries
  // in the worker pool without blocking the event loop.
  private static final int EVENT_LOOP_POOL_SIZE = 1;
  private static final int NUM_CONCURRENT_REQUESTS_TO_TEST = 5;
  private static final int WORKER_POOL_SIZE = 10;

  private static final TestKsqlRestApp REST_APP = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
      .withProperty(KSQL_DEFAULT_KEY_FORMAT_CONFIG, "JSON")
      .withProperty("ksql.verticle.instances", EVENT_LOOP_POOL_SIZE)
      .withProperty("ksql.worker.pool.size", WORKER_POOL_SIZE)
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS)
      .around(REST_APP);

  @BeforeClass
  public static void setUpClass() throws Exception {
    TEST_HARNESS.ensureTopics(TEST_TOPIC, EMPTY_TEST_TOPIC, EMPTY_TEST_TOPIC_2);
    TEST_HARNESS.produceRows(TEST_TOPIC, TEST_DATA_PROVIDER, KEY_FORMAT, VALUE_FORMAT);
    RestIntegrationTestUtil.createStream(REST_APP, TEST_DATA_PROVIDER);
    RestIntegrationTestUtil.createStream(REST_APP, EMPTY_TEST_DATA_PROVIDER);
    RestIntegrationTestUtil.createStream(REST_APP, EMPTY_TEST_DATA_PROVIDER_2);

    makeKsqlRequest("CREATE TABLE " + AGG_TABLE + " AS "
        + "SELECT K, LATEST_BY_OFFSET(LONG) AS LONG FROM " + TEST_STREAM + " GROUP BY K;"
    );

    TEST_HARNESS.verifyAvailableUniqueRows(
        AGG_TABLE,
        4, // Only unique keys are counted
        KEY_FORMAT,
        VALUE_FORMAT,
        AGG_SCHEMA
    );

    final String testDir = Paths.get(TestUtils.tempDirectory().getAbsolutePath(), "client_integ_test").toString();
    final String connectFilePath = Paths.get(testDir, "connect.properties").toString();
    Files.createDirectories(Paths.get(testDir));

    writeConnectConfigs(connectFilePath, ImmutableMap.<String, String>builder()
        .put("bootstrap.servers", TEST_HARNESS.kafkaBootstrapServers())
        .put("group.id", UUID.randomUUID().toString())
        .put("key.converter", StringConverter.class.getName())
        .put("value.converter", JsonConverter.class.getName())
        .put("offset.storage.topic", "connect-offsets")
        .put("status.storage.topic", "connect-status")
        .put("config.storage.topic", "connect-config")
        .put("offset.storage.replication.factor", "1")
        .put("status.storage.replication.factor", "1")
        .put("config.storage.replication.factor", "1")
        .put("value.converter.schemas.enable", "false")
        .build()
    );

  }

  private static void writeConnectConfigs(final String path, final Map<String, String> configs) throws Exception {
    try (PrintWriter out = new PrintWriter(new OutputStreamWriter(
        new FileOutputStream(path, true), StandardCharsets.UTF_8))) {
      for (Map.Entry<String, String> entry : configs.entrySet()) {
        out.println(entry.getKey() + "=" + entry.getValue());
      }
    }
  }

  @AfterClass
  public static void classTearDown() {
    REST_APP.getPersistentQueries().forEach(str -> makeKsqlRequest("TERMINATE " + str + ";"));
  }

  private Vertx vertx;

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
  }

  @After
  public void tearDown() {
    if (vertx != null) {
      vertx.close();
    }
    REST_APP.getServiceContext().close();
  }

  @Test(timeout = 120000L)
  public void shouldStreamMultiplePushQueriesRest() {
    final List<RestResponse<StreamPublisher<StreamedRow>>> responses = new ArrayList<>(NUM_CONCURRENT_REQUESTS_TO_TEST);

    // We should be able to serve multiple pull queries at once, even
    // though we only have one event-loop thread, because we have enough
    // workers in the worker pool.
    for(long i = 0; i < NUM_CONCURRENT_REQUESTS_TO_TEST; i++) {
      responses.add(REST_APP.buildKsqlClient().makeQueryRequestStreamed(PUSH_QUERY,i));
    }

    assertThat(responses, everyItem(hasProperty("successful", is(true))));

    for (final RestResponse<StreamPublisher<StreamedRow>> response : responses) {
      response.getResponse().close();
    }
  }

  @Test(timeout = 120000L)
  public void shouldRoundTripConsistencyVectorWhenEnabled() throws Exception {
    final KsqlRestClient ksqlRestClient = REST_APP.buildKsqlClient();
    ksqlRestClient.setProperty(KsqlConfig.KSQL_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR_ENABLED, true);

    ksqlRestClient.getConsistencyOffsetVector().setVersion(1);
    ksqlRestClient.getConsistencyOffsetVector().setOffsetVector(
        ImmutableMap.of("Vicky", ImmutableMap.of(1, 1L, 2, 2L, 3, 3L)));

    final RestResponse<StreamPublisher<StreamedRow>> response =
        ksqlRestClient.makeQueryRequestStreamed(PULL_QUERY_ON_TABLE, 1L);

    final List<StreamedRow> rows = getElementsFromPublisher(4, response.getResponse());
    assertThat(rows, hasSize(3));
    assertThat(rows.get(2).getConsistencyToken().get(), not(Optional.empty()));
    final String serialized = rows.get(2).getConsistencyToken().get().getConsistencyToken();
    final ConsistencyOffsetVector cvResponse = new ConsistencyOffsetVector();
    cvResponse.deserialize(serialized);
    assertThat(cvResponse.getVersion(), is(2));
    assertThat(cvResponse.getOffsetVector().keySet(), hasSize(2));
    assertThat(cvResponse.getTopicOffsets("dummy").keySet(), hasSize(3));
    assertThat(cvResponse.getTopicOffsets("dummy").get(5), is(5L));
    assertThat(cvResponse.getTopicOffsets("dummy").get(6), is(6L));
    assertThat(cvResponse.getTopicOffsets("dummy").get(7), is(7L));
  }

  @Test(timeout = 120000L)
  public void shouldNotRoundTripConsistencyVectorWhenDisabled() throws Exception {
    final KsqlRestClient ksqlRestClient = REST_APP.buildKsqlClient();
    ksqlRestClient.getConsistencyOffsetVector().setVersion(1);
    ksqlRestClient.getConsistencyOffsetVector().setOffsetVector(
        ImmutableMap.of("Vicky", ImmutableMap.of(1, 1L, 2, 2L, 3, 3L)));

    final RestResponse<StreamPublisher<StreamedRow>> response =
        ksqlRestClient.makeQueryRequestStreamed(PULL_QUERY_ON_TABLE, 1L);

    final List<StreamedRow> rows = getElementsFromPublisher(4, response.getResponse());
    assertThat(rows, hasSize(2));
    assertThat(rows.get(0).getConsistencyToken(), is(Optional.empty()));
    assertThat(rows.get(1).getConsistencyToken(), is(Optional.empty()));
  }


  private static List<KsqlEntity> makeKsqlRequest(final String sql) {
    return RestIntegrationTestUtil.makeKsqlRequest(REST_APP, sql);
  }

  private <T> List<T> getElementsFromPublisher(int numElements, StreamPublisher<T> publisher)
      throws Exception {
    CompletableFuture<List<T>> future = new CompletableFuture<>();
    CollectSubscriber<T> subscriber = new CollectSubscriber<>(vertx.getOrCreateContext(),
                                                              future, numElements);
    publisher.subscribe(subscriber);
    return future.get();
  }

  private static class CollectSubscriber<T> extends BaseSubscriber<T> {

    private final CompletableFuture<List<T>> future;
    private final List<T> list = new ArrayList<>();
    private final int numRows;

    public CollectSubscriber(final Context context, CompletableFuture<List<T>> future,
                             final int numRows) {
      super(context);
      this.future = future;
      this.numRows = numRows;
    }

    @Override
    protected void afterSubscribe(final Subscription subscription) {
      makeRequest(1);
    }

    @Override
    protected void handleValue(final T value) {
      list.add(value);
      if (list.size() == numRows) {
        future.complete(new ArrayList<>(list));
      }
      makeRequest(1);
    }

    @Override
    protected void handleComplete() {
      future.complete(new ArrayList<>(list));
    }

    @Override
    protected void handleError(final Throwable t) {
      future.completeExceptionally(t);
    }
  }
}