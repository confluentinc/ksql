/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
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

import static io.confluent.ksql.api.client.util.ClientTestUtil.shouldReceiveRows;
import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static io.confluent.ksql.util.KsqlConfig.KSQL_DEFAULT_KEY_FORMAT_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PUSH_V2_CONTINUATION_TOKENS_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PUSH_V2_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PUSH_V2_REGISTRY_INSTALLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_STREAMS_PREFIX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import io.confluent.ksql.api.client.BatchedQueryResult;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.client.StreamedQueryResult;
import io.confluent.ksql.api.client.impl.ClientImpl;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.reactive.BaseSubscriber;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.client.StreamPublisher;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.PushContinuationToken;
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
import io.confluent.ksql.util.ClientConfig.ConsistencyLevel;
import io.confluent.ksql.util.ConsistencyOffsetVector;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PushOffsetRange;
import io.confluent.ksql.util.PushOffsetVector;
import io.confluent.ksql.util.StructuredTypesDataProvider;
import io.confluent.ksql.util.TestDataProvider;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.reactivestreams.Subscription;

public class ConsistencyOffsetVectorFunctionalTest {

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
  private static final String PULL_QUERY_ON_TABLE =
      "SELECT * from " + AGG_TABLE + " WHERE K=" + AN_AGG_KEY + ";";
  private static final String PUSH_QUERY = "SELECT * FROM " + AGG_TABLE + " EMIT CHANGES;";

  private static final ConsistencyOffsetVector CONSISTENCY_OFFSET_VECTOR =
      ConsistencyOffsetVector.emptyVector().withComponent(TEST_TOPIC, 0, 5L);
  private static final PushContinuationToken CONTINUATION_TOKEN =
      new PushContinuationToken(
          new PushOffsetRange(
              Optional.of(new PushOffsetVector(ImmutableList.of(0L, 1L))),
              new PushOffsetVector(ImmutableList.of(0L, 3L))).serialize());
  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();


  private static final TestKsqlRestApp REST_APP = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
      .withProperty(KSQL_DEFAULT_KEY_FORMAT_CONFIG, "JSON")
      .withProperty(KSQL_QUERY_PULL_CONSISTENCY_OFFSET_VECTOR_ENABLED, true)
      .withProperty(KsqlConfig.KSQL_HEADERS_COLUMNS_ENABLED, true)
      .withProperty(KSQL_QUERY_PUSH_V2_ENABLED, true)
      .withProperty(KSQL_QUERY_PUSH_V2_REGISTRY_INSTALLED, true)
      .withProperty(KSQL_QUERY_PUSH_V2_CONTINUATION_TOKENS_ENABLED, true)
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS)
      .around(REST_APP);


  @BeforeClass
  public static void setUpClass() throws Exception {
    TEST_HARNESS.ensureTopics(TEST_TOPIC, EMPTY_TEST_TOPIC);
    TEST_HARNESS.produceRows(TEST_TOPIC, TEST_DATA_PROVIDER, KEY_FORMAT, VALUE_FORMAT);
    RestIntegrationTestUtil.createStream(REST_APP, TEST_DATA_PROVIDER);
    RestIntegrationTestUtil.createStream(REST_APP, EMPTY_TEST_DATA_PROVIDER);

    makeKsqlRequest("CREATE TABLE " + AGG_TABLE + " AS "
                        + "SELECT K, LATEST_BY_OFFSET(LONG) AS LONG FROM " + TEST_STREAM
                        + " GROUP BY K;"
    );

    TEST_HARNESS.verifyAvailableUniqueRows(
        AGG_TABLE,
        4, // Only unique keys are counted
        KEY_FORMAT,
        VALUE_FORMAT,
        AGG_SCHEMA
    );
  }


  @AfterClass
  public static void classTearDown() {
    REST_APP.getPersistentQueries().forEach(str -> makeKsqlRequest("TERMINATE " + str + ";"));
  }

  private Vertx vertx;
  private Client consistencClient;
  private Client eventualClient;

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
    consistencClient = createClient(true);
    eventualClient = createClient(false);
  }

  @After
  public void tearDown() {
    if (consistencClient != null) {
      consistencClient.close();
    }
    if (eventualClient != null) {
      eventualClient.close();
    }
    if (vertx != null) {
      vertx.close();
    }
    REST_APP.getServiceContext().close();
  }

  @Test
  public void shouldRoundTripCVWhenPullQueryOnTableAsync() throws Exception {
    // When
    final StreamedQueryResult streamedQueryResult = consistencClient.streamQuery(PULL_QUERY_ON_TABLE).get();

    // Then
    shouldReceiveRows(
        streamedQueryResult,
        1,
        (v) -> {}, //do nothing
        true
    );

    assertThatEventually(streamedQueryResult::isComplete, is(true));
    assertThat(((ClientImpl) consistencClient).getSerializedConsistencyVector(), is(notNullValue()));
    final String serializedCV = ((ClientImpl) consistencClient).getSerializedConsistencyVector();
    verifyConsistencyVector(serializedCV);
  }

  @Test
  public void shouldRetry() throws Exception {
    final Map<String, Object> properties = new HashMap<>();
    properties.put("ksql.query.push.v2.enabled", true);
    properties.put("auto.offset.reset", "latest");
    properties.put("ksql.query.push.v2.continuation.tokens.enabled", true);

    // When
    final StreamedQueryResult streamedQueryResult = eventualClient.streamQuery(PUSH_QUERY, properties).get();

    // Then
    streamedQueryResult.poll();

    assertThatEventually(streamedQueryResult::isComplete, is(true));
    assertThat(((ClientImpl) eventualClient).getContinuationToken(), is(notNullValue()));
    final String continuationToken = ((ClientImpl) eventualClient).getSerializedConsistencyVector();
//    verifyConsistencyVector(continuationToken);
  }

  @Test
  public void shouldRoundTripCVWhenPullQueryOnTableSync() throws Exception {
    // When
    final StreamedQueryResult streamedQueryResult = consistencClient.streamQuery(PULL_QUERY_ON_TABLE).get();
    streamedQueryResult.poll();

    // Then
    assertThatEventually(streamedQueryResult::isComplete, is(true));
    assertThatEventually(() -> ((ClientImpl) consistencClient).getSerializedConsistencyVector(),
                         is(notNullValue()));
    final String serializedCV = ((ClientImpl) consistencClient).getSerializedConsistencyVector();
    verifyConsistencyVector(serializedCV);
  }

  @Test
  public void shouldRoundTripCVWhenExecutePullQuery() throws Exception {
    // When
    final BatchedQueryResult batchedQueryResult = consistencClient.executeQuery(PULL_QUERY_ON_TABLE);
    final List<Row> rows = batchedQueryResult.get();

    // Then
    assertThat(rows, hasSize(1));
    assertThat(batchedQueryResult.queryID().get(), is(notNullValue()));
    assertThatEventually(() -> ((ClientImpl) consistencClient).getSerializedConsistencyVector(),
                          is(notNullValue()));
    final String serializedCV = ((ClientImpl) consistencClient).getSerializedConsistencyVector();
    verifyConsistencyVector(serializedCV);
  }

  @Test(timeout = 120000L)
  public void shouldRoundTripCVWhenPullQueryHttp1() throws Exception {
    // Given
    final KsqlRestClient ksqlRestClient = REST_APP.buildKsqlClient(
        Optional.empty(), ConsistencyLevel.MONOTONIC_SESSION);

    // When
    final RestResponse<StreamPublisher<StreamedRow>> response =
        ksqlRestClient.makeQueryRequestStreamed(PULL_QUERY_ON_TABLE, 1L, null, null);
    final List<StreamedRow> rows = getElementsFromPublisher(4, response.getResponse());

    // Then
    assertThat(rows, hasSize(3));
    assertThat(rows.get(2).getConsistencyToken().get(), not(Optional.empty()));
    final String serialized = rows.get(2).getConsistencyToken().get().getConsistencyToken();
    verifyConsistencyVector(serialized);
  }

  @Test
  public void shouldNotRoundTripCVWhenPullQueryOnTableAsync() throws Exception {
    // When
    final StreamedQueryResult streamedQueryResult = eventualClient.streamQuery(PULL_QUERY_ON_TABLE).get();

    // Then
    shouldReceiveRows(
        streamedQueryResult,
        1  ,
        (v) -> {}, //do nothing
        true
    );

    assertThatEventually(streamedQueryResult::isComplete, is(true));
    assertThat(((ClientImpl) eventualClient).getSerializedConsistencyVector(), is(isEmptyString()));
  }

  @Test
  public void shouldNotRoundTripCVWhenPullQueryOnTableSync() throws Exception {
    // When
    final StreamedQueryResult streamedQueryResult = eventualClient.streamQuery(PULL_QUERY_ON_TABLE).get();
    streamedQueryResult.poll();

    // Then
    assertThatEventually(streamedQueryResult::isComplete, is(true));
    assertThatEventually(() -> ((ClientImpl) eventualClient).getSerializedConsistencyVector(),
                         is(isEmptyString()));
  }

  @Test
  public void shouldNotRoundTripCVWhenExecutePullQuery() throws Exception {
    // When
    final BatchedQueryResult batchedQueryResult = eventualClient.executeQuery(PULL_QUERY_ON_TABLE);
    batchedQueryResult.get();

    // Then
    assertThat(batchedQueryResult.queryID().get(), is(notNullValue()));
    assertThat(((ClientImpl) eventualClient).getSerializedConsistencyVector(), is(isEmptyString()));
  }

  @Test(timeout = 120000L)
  public void shouldNotRoundTripCVHttp1() throws Exception {
    final KsqlRestClient ksqlRestClient = REST_APP.buildKsqlClient(
        Optional.empty(), ConsistencyLevel.EVENTUAL);

    final RestResponse<StreamPublisher<StreamedRow>> response =
        ksqlRestClient.makeQueryRequestStreamed(PULL_QUERY_ON_TABLE, 1L);

    final List<StreamedRow> rows = getElementsFromPublisher(4, response.getResponse());
    assertThat(rows, hasSize(2));
    assertThat(rows.get(0).getConsistencyToken(), is(Optional.empty()));
    assertThat(rows.get(1).getConsistencyToken(), is(Optional.empty()));
  }



  private Client createClient(final boolean withConsistency) {
    final ClientOptions clientOptions = ClientOptions.create()
        .setHost("localhost")
        .setPort(REST_APP.getListeners().get(0).getPort());
    if (withConsistency)
        clientOptions.setConsistencyLevel(ConsistencyLevel.MONOTONIC_SESSION);
    return Client.create(clientOptions, vertx);
  }

  private <T> List<T> getElementsFromPublisher(int numElements, StreamPublisher<T> publisher)
      throws Exception {
    CompletableFuture<List<T>> future = new CompletableFuture<>();
    CollectSubscriber<T> subscriber = new CollectSubscriber<>(vertx.getOrCreateContext(),
                                                              future, numElements);
    publisher.subscribe(subscriber);
    return future.get();
  }

  private static void verifyConsistencyVector(final String serializedCV) {
    final ConsistencyOffsetVector cvResponse = ConsistencyOffsetVector.deserialize(serializedCV);
    assertThat(cvResponse.equals(CONSISTENCY_OFFSET_VECTOR), is(true));
  }

  private static List<KsqlEntity> makeKsqlRequest(final String sql) {
    return RestIntegrationTestUtil.makeKsqlRequest(REST_APP, sql);
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