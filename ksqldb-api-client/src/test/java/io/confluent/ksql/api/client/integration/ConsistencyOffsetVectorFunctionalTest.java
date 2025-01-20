///*
// * Copyright 2019 Confluent Inc.
// *
// * Licensed under the Confluent Community License (the "License"; you may not use
// * this file except in compliance with the License. You may obtain a copy of the
// * License at
// *
// * http://www.confluent.io/confluent-community-license
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations under the License.
// */
//
//package io.confluent.ksql.api.client.integration;
//
//import static io.confluent.ksql.api.client.util.ClientTestUtil.shouldReceiveRows;
//import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
//import static io.confluent.ksql.util.KsqlConfig.KSQL_DEFAULT_KEY_FORMAT_CONFIG;
//import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR_ENABLED;
//import static io.confluent.ksql.util.KsqlConfig.KSQL_STREAMS_PREFIX;
//import static org.hamcrest.MatcherAssert.assertThat;
//import static org.hamcrest.Matchers.hasSize;
//import static org.hamcrest.Matchers.is;
//import static org.hamcrest.Matchers.isEmptyString;
//import static org.hamcrest.Matchers.not;
//import static org.hamcrest.Matchers.notNullValue;
//import static org.hamcrest.Matchers.nullValue;
//
//import com.google.common.collect.ImmutableListMultimap;
//import com.google.common.collect.ImmutableMap;
//import io.confluent.ksql.api.client.BatchedQueryResult;
//import io.confluent.ksql.api.client.Client;
//import io.confluent.ksql.api.client.ClientOptions;
//import io.confluent.ksql.api.client.Row;
//import io.confluent.ksql.api.client.StreamedQueryResult;
//import io.confluent.ksql.api.client.impl.ClientImpl;
//import io.confluent.ksql.integration.IntegrationTestHarness;
//import io.confluent.ksql.integration.Retry;
//import io.confluent.ksql.name.ColumnName;
//import io.confluent.ksql.reactive.BaseSubscriber;
//import io.confluent.ksql.rest.client.KsqlRestClient;
//import io.confluent.ksql.rest.client.RestResponse;
//import io.confluent.ksql.rest.client.StreamPublisher;
//import io.confluent.ksql.rest.entity.KsqlEntity;
//import io.confluent.ksql.rest.entity.StreamedRow;
//import io.confluent.ksql.rest.integration.RestIntegrationTestUtil;
//import io.confluent.ksql.rest.server.TestKsqlRestApp;
//import io.confluent.ksql.schema.ksql.LogicalSchema;
//import io.confluent.ksql.schema.ksql.PhysicalSchema;
//import io.confluent.ksql.schema.ksql.types.SqlTypes;
//import io.confluent.ksql.serde.Format;
//import io.confluent.ksql.serde.FormatFactory;
//import io.confluent.ksql.serde.SerdeFeature;
//import io.confluent.ksql.serde.SerdeFeatures;
//import io.confluent.ksql.util.ConsistencyOffsetVector;
//import io.confluent.ksql.util.KsqlConfig;
//import io.confluent.ksql.util.StructuredTypesDataProvider;
//import io.confluent.ksql.util.TestDataProvider;
//import io.vertx.core.Context;
//import io.vertx.core.Vertx;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Optional;
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.TimeUnit;
//import kafka.zookeeper.ZooKeeperClientException;
//import org.apache.kafka.streams.StreamsConfig;
//import org.junit.After;
//import org.junit.AfterClass;
//import org.junit.Before;
//import org.junit.BeforeClass;
//import org.junit.ClassRule;
//import org.junit.Test;
//import org.junit.rules.RuleChain;
//import org.reactivestreams.Subscription;
//
//public class ConsistencyOffsetVectorFunctionalTest {
//
//  private static final StructuredTypesDataProvider TEST_DATA_PROVIDER = new StructuredTypesDataProvider();
//  private static final String TEST_TOPIC = TEST_DATA_PROVIDER.topicName();
//  private static final String TEST_STREAM = TEST_DATA_PROVIDER.sourceName();
//
//  private static final Format KEY_FORMAT = FormatFactory.JSON;
//  private static final Format VALUE_FORMAT = FormatFactory.JSON;
//
//  private static final String AGG_TABLE = "AGG_TABLE";
//  private static final String AN_AGG_KEY = "STRUCT(F1 := ARRAY['a'])";
//  private static final PhysicalSchema AGG_SCHEMA = PhysicalSchema.from(
//      LogicalSchema.builder()
//          .keyColumn(ColumnName.of("K"), SqlTypes.struct()
//              .field("F1", SqlTypes.array(SqlTypes.STRING))
//              .build())
//          .valueColumn(ColumnName.of("LONG"), SqlTypes.BIGINT)
//          .build(),
//      SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES),
//      SerdeFeatures.of()
//  );
//
//  private static final TestDataProvider EMPTY_TEST_DATA_PROVIDER = new TestDataProvider(
//      "EMPTY_STRUCTURED_TYPES", TEST_DATA_PROVIDER.schema(), ImmutableListMultimap.of());
//  private static final String EMPTY_TEST_TOPIC = EMPTY_TEST_DATA_PROVIDER.topicName();
//  private static final String PULL_QUERY_ON_TABLE =
//      "SELECT * from " + AGG_TABLE + " WHERE K=" + AN_AGG_KEY + ";";
//
//  private static final ConsistencyOffsetVector CONSISTENCY_OFFSET_VECTOR =
//      new ConsistencyOffsetVector(2, ImmutableMap.of("dummy",
//                                                     ImmutableMap.of(5, 5L, 6, 6L, 7, 7L)));
//  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();
//
//
//  private static final TestKsqlRestApp REST_APP = TestKsqlRestApp
//      .builder(TEST_HARNESS::kafkaBootstrapServers)
//      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
//      .withProperty(KSQL_DEFAULT_KEY_FORMAT_CONFIG, "JSON")
//      .withProperty(KSQL_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR_ENABLED, true)
//      .withProperty(KsqlConfig.KSQL_HEADERS_COLUMNS_ENABLED, true)
//      .build();
//
//  @ClassRule
//  public static final RuleChain CHAIN = RuleChain
//      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
//      .around(TEST_HARNESS)
//      .around(REST_APP);
//
//
//  @BeforeClass
//  public static void setUpClass() throws Exception {
//    TEST_HARNESS.ensureTopics(TEST_TOPIC, EMPTY_TEST_TOPIC);
//    TEST_HARNESS.produceRows(TEST_TOPIC, TEST_DATA_PROVIDER, KEY_FORMAT, VALUE_FORMAT);
//    RestIntegrationTestUtil.createStream(REST_APP, TEST_DATA_PROVIDER);
//    RestIntegrationTestUtil.createStream(REST_APP, EMPTY_TEST_DATA_PROVIDER);
//
//    makeKsqlRequest("CREATE TABLE " + AGG_TABLE + " AS "
//                        + "SELECT K, LATEST_BY_OFFSET(LONG) AS LONG FROM " + TEST_STREAM
//                        + " GROUP BY K;"
//    );
//
//    TEST_HARNESS.verifyAvailableUniqueRows(
//        AGG_TABLE,
//        4, // Only unique keys are counted
//        KEY_FORMAT,
//        VALUE_FORMAT,
//        AGG_SCHEMA
//    );
//  }
//
//
//  @AfterClass
//  public static void classTearDown() {
//    REST_APP.getPersistentQueries().forEach(str -> makeKsqlRequest("TERMINATE " + str + ";"));
//  }
//
//  private Vertx vertx;
//  private Client client;
//
//  @Before
//  public void setUp() {
//    vertx = Vertx.vertx();
//    client = createClient();
//  }
//
//  @After
//  public void tearDown() {
//    if (client != null) {
//      client.close();
//    }
//    if (vertx != null) {
//      vertx.close();
//    }
//    REST_APP.getServiceContext().close();
//  }
//
//  @Test
//  public void shouldRoundTripCVWhenPullQueryOnTableAsync() throws Exception {
//    // When
//    final StreamedQueryResult streamedQueryResult = client.streamQuery(
//        PULL_QUERY_ON_TABLE,  ImmutableMap.of(KSQL_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR_ENABLED, true)).get();
//
//    // Then
//    shouldReceiveRows(
//        streamedQueryResult,
//        1,
//        (v) -> {}, //do nothing
//        true
//    );
//
//    assertThatEventually(streamedQueryResult::isComplete, is(true));
//    assertThat(((ClientImpl)client).getSerializedConsistencyVector(), is(notNullValue()));
//    final String serializedCV = ((ClientImpl)client).getSerializedConsistencyVector();
//    verifyConsistencyVector(serializedCV);
//  }
//
//  @Test
//  public void shouldRoundTripCVWhenPullQueryOnTableSync() throws Exception {
//    // When
//    final StreamedQueryResult streamedQueryResult = client.streamQuery(
//        PULL_QUERY_ON_TABLE,  ImmutableMap.of(KSQL_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR_ENABLED, true)).get();
//    streamedQueryResult.poll();
//
//    // Then
//    assertThatEventually(streamedQueryResult::isComplete, is(true));
//    assertThatEventually(() -> ((ClientImpl)client).getSerializedConsistencyVector(),
//                         is(notNullValue()));
//    final String serializedCV = ((ClientImpl)client).getSerializedConsistencyVector();
//    verifyConsistencyVector(serializedCV);
//  }
//
//  @Test
//  public void shouldRoundTripCVWhenExecutePullQuery() throws Exception {
//    // When
//    final BatchedQueryResult batchedQueryResult = client.executeQuery(
//        PULL_QUERY_ON_TABLE, ImmutableMap.of(KSQL_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR_ENABLED, true));
//    final List<Row> rows = batchedQueryResult.get();
//
//    // Then
//    assertThat(rows, hasSize(1));
//    assertThat(batchedQueryResult.queryID().get(), is(notNullValue()));
//    assertThatEventually(() -> ((ClientImpl)client).getSerializedConsistencyVector(),
//                          is(notNullValue()));
//    final String serializedCV = ((ClientImpl)client).getSerializedConsistencyVector();
//    verifyConsistencyVector(serializedCV);
//  }
//
//  @Test(timeout = 120000L)
//  public void shouldRoundTripCVWhenPullQueryHttp1() throws Exception {
//    // Given
//    final KsqlRestClient ksqlRestClient = REST_APP.buildKsqlClient();
//    ksqlRestClient.setProperty(KSQL_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR_ENABLED, true);
//
//    // When
//    final RestResponse<StreamPublisher<StreamedRow>> response =
//        ksqlRestClient.makeQueryRequestStreamed(PULL_QUERY_ON_TABLE, 1L);
//    final List<StreamedRow> rows = getElementsFromPublisher(4, response.getResponse());
//
//    // Then
//    assertThat(rows, hasSize(3));
//    assertThat(rows.get(2).getConsistencyToken().get(), not(Optional.empty()));
//    final String serialized = rows.get(2).getConsistencyToken().get().getConsistencyToken();
//    verifyConsistencyVector(serialized);
//  }
//
//  @Test
//  public void shouldNotRoundTripCVWhenPullQueryOnTableAsync() throws Exception {
//    // When
//    final StreamedQueryResult streamedQueryResult = client.streamQuery(
//        PULL_QUERY_ON_TABLE,  ImmutableMap.of(KSQL_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR_ENABLED, false)).get();
//
//    // Then
//    shouldReceiveRows(
//        streamedQueryResult,
//        1  ,
//        (v) -> {}, //do nothing
//        true
//    );
//
//    assertThatEventually(streamedQueryResult::isComplete, is(true));
//    assertThat(((ClientImpl)client).getSerializedConsistencyVector(), is(isEmptyString()));
//  }
//
//  @Test
//  public void shouldNotRoundTripCVWhenPullQueryOnTableSync() throws Exception {
//    // When
//    final StreamedQueryResult streamedQueryResult = client.streamQuery(
//        PULL_QUERY_ON_TABLE,  ImmutableMap.of(KSQL_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR_ENABLED, false)).get();
//    streamedQueryResult.poll();
//
//    // Then
//    assertThatEventually(streamedQueryResult::isComplete, is(true));
//    assertThatEventually(() -> ((ClientImpl)client).getSerializedConsistencyVector(),
//                         is(isEmptyString()));
//  }
//
//  @Test
//  public void shouldNotRoundTripCVWhenExecutePullQuery() throws Exception {
//    // When
//    final BatchedQueryResult batchedQueryResult = client.executeQuery(
//        PULL_QUERY_ON_TABLE,  ImmutableMap.of(KSQL_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR_ENABLED, false));
//    batchedQueryResult.get();
//
//    // Then
//    assertThat(batchedQueryResult.queryID().get(), is(notNullValue()));
//    assertThat(((ClientImpl)client).getSerializedConsistencyVector(), is(isEmptyString()));
//  }
//
//  @Test(timeout = 120000L)
//  public void shouldNotRoundTripCVHttp1() throws Exception {
//    final KsqlRestClient ksqlRestClient = REST_APP.buildKsqlClient();
//    ksqlRestClient.setProperty(KSQL_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR_ENABLED, false);
//
//    final RestResponse<StreamPublisher<StreamedRow>> response =
//        ksqlRestClient.makeQueryRequestStreamed(PULL_QUERY_ON_TABLE, 1L);
//
//    final List<StreamedRow> rows = getElementsFromPublisher(4, response.getResponse());
//    assertThat(rows, hasSize(2));
//    assertThat(rows.get(0).getConsistencyToken(), is(Optional.empty()));
//    assertThat(rows.get(1).getConsistencyToken(), is(Optional.empty()));
//  }
//
//
//
//  private Client createClient() {
//    final ClientOptions clientOptions = ClientOptions.create()
//        .setHost("localhost")
//        .setPort(REST_APP.getListeners().get(0).getPort());
//    return Client.create(clientOptions, vertx);
//  }
//
//  private <T> List<T> getElementsFromPublisher(int numElements, StreamPublisher<T> publisher)
//      throws Exception {
//    CompletableFuture<List<T>> future = new CompletableFuture<>();
//    CollectSubscriber<T> subscriber = new CollectSubscriber<>(vertx.getOrCreateContext(),
//                                                              future, numElements);
//    publisher.subscribe(subscriber);
//    return future.get();
//  }
//
//  private static void verifyConsistencyVector(final String serializedCV) {
//    final ConsistencyOffsetVector cvResponse = ConsistencyOffsetVector.deserialize(serializedCV);
//    assertThat(cvResponse.equals(CONSISTENCY_OFFSET_VECTOR), is(true));
//  }
//
//  private static List<KsqlEntity> makeKsqlRequest(final String sql) {
//    return RestIntegrationTestUtil.makeKsqlRequest(REST_APP, sql);
//  }
//
//  private static class CollectSubscriber<T> extends BaseSubscriber<T> {
//
//    private final CompletableFuture<List<T>> future;
//    private final List<T> list = new ArrayList<>();
//    private final int numRows;
//
//    public CollectSubscriber(final Context context, CompletableFuture<List<T>> future,
//                             final int numRows) {
//      super(context);
//      this.future = future;
//      this.numRows = numRows;
//    }
//
//    @Override
//    protected void afterSubscribe(final Subscription subscription) {
//      makeRequest(1);
//    }
//
//    @Override
//    protected void handleValue(final T value) {
//      list.add(value);
//      if (list.size() == numRows) {
//        future.complete(new ArrayList<>(list));
//      }
//      makeRequest(1);
//    }
//
//    @Override
//    protected void handleComplete() {
//      future.complete(new ArrayList<>(list));
//    }
//
//    @Override
//    protected void handleError(final Throwable t) {
//      future.completeExceptionally(t);
//    }
//  }
//}