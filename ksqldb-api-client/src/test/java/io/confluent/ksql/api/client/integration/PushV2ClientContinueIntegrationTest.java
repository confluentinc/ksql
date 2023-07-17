/*
 * Copyright 2022 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *Ø
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.api.client.integration;

import static io.confluent.ksql.api.client.util.ClientTestUtil.shouldReceiveRows;
import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static io.confluent.ksql.util.KsqlConfig.KSQL_DEFAULT_KEY_FORMAT_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PUSH_V2_CONTINUATION_TOKENS_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PUSH_V2_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PUSH_V2_REGISTRY_INSTALLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_STREAMS_PREFIX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.ColumnType;
import io.confluent.ksql.api.client.KsqlArray;
import io.confluent.ksql.api.client.KsqlObject;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.client.StreamedQueryResult;
import io.confluent.ksql.api.client.impl.StreamedQueryResultImpl;
import io.confluent.ksql.api.client.util.ClientTestUtil;
import io.confluent.ksql.api.client.util.RowUtil;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.rest.ApiJsonMapper;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.integration.RestIntegrationTestUtil;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.schema.ksql.SqlTimeTypes;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.StructuredTypesDataProvider;
import io.confluent.ksql.util.StructuredTypesDataProvider.Batch;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.reactivestreams.Publisher;

@Category({IntegrationTest.class})
public class PushV2ClientContinueIntegrationTest {
  private static final StructuredTypesDataProvider TEST_DATA_PROVIDER = new StructuredTypesDataProvider();
  private static final StructuredTypesDataProvider TEST_MORE_DATA_PROVIDER =
      new StructuredTypesDataProvider(Batch.BATCH2);

  private static final String TEST_TOPIC = TEST_DATA_PROVIDER.topicName();
  private static final String TEST_STREAM = TEST_DATA_PROVIDER.sourceName();
  private static final int TEST_NUM_ROWS = TEST_DATA_PROVIDER.data().size();
  private static final int TEST_MORE_NUM_ROWS = TEST_MORE_DATA_PROVIDER.data().size();
  private static final List<String> TEST_COLUMN_NAMES =
      ImmutableList.of("K", "STR", "LONG", "DEC", "BYTES_", "ARRAY", "MAP", "STRUCT", "COMPLEX",
          "TIMESTAMP", "DATE", "TIME", "HEAD");
  private static final List<ColumnType> TEST_COLUMN_TYPES =
      RowUtil.columnTypesFromStrings(ImmutableList.of("STRUCT", "STRING", "BIGINT", "DECIMAL",
          "BYTES", "ARRAY", "MAP", "STRUCT", "STRUCT", "TIMESTAMP", "DATE", "TIME", "BYTES"));
  private static final List<KsqlArray> TEST_EXPECTED_ROWS =
      convertToClientRows(TEST_DATA_PROVIDER.data());
  private static final List<KsqlArray> TEST_MORE_EXPECTED_ROWS =
      convertToClientRows(TEST_MORE_DATA_PROVIDER.data());
  private static final Format KEY_FORMAT = FormatFactory.JSON;
  private static final Format VALUE_FORMAT = FormatFactory.JSON;
  private static final Supplier<Long> TS_SUPPLIER = () -> 0L;
  private static final Supplier<List<Header>> HEADERS_SUPPLIER = () -> ImmutableList.of(new RecordHeader("h0", new byte[] {23}));

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  // these properties are set together to allow us to verify that we can handle push queries
  // in the worker pool without blocking the event loop.
  private static final int EVENT_LOOP_POOL_SIZE = 1;
  private static final int WORKER_POOL_SIZE = 10;

  private static final TestKsqlRestApp REST_APP = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
      .withProperty(KSQL_DEFAULT_KEY_FORMAT_CONFIG, "JSON")
      .withProperty("ksql.verticle.instances", EVENT_LOOP_POOL_SIZE)
      .withProperty("ksql.worker.pool.size", WORKER_POOL_SIZE)
      .withProperty(KsqlConfig.KSQL_HEADERS_COLUMNS_ENABLED, true)
      .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:8088")
      .withProperty(KsqlConfig.KSQL_QUERY_PUSH_V2_NEW_LATEST_DELAY_MS, 0L)
      .withProperty(KSQL_QUERY_PUSH_V2_ENABLED, true)
      .withProperty(KSQL_QUERY_PUSH_V2_REGISTRY_INSTALLED, true)
      .withProperty(KSQL_QUERY_PUSH_V2_CONTINUATION_TOKENS_ENABLED, true)
      .withProperty(KSQL_STREAMS_PREFIX + "max.poll.interval.ms", 5000)
      .withProperty(KSQL_STREAMS_PREFIX + "session.timeout.ms", 10000)
      .withProperty(KSQL_STREAMS_PREFIX + "auto.offset.reset", "latest")
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS)
      .around(REST_APP);


  @BeforeClass
  public static void setUpClass() throws Exception {
    TEST_HARNESS.ensureTopics(TEST_TOPIC);
    RestIntegrationTestUtil.createStream(REST_APP, TEST_DATA_PROVIDER);
  }

  @AfterClass
  public static void classTearDown() {
    REST_APP.getPersistentQueries().forEach(str -> makeKsqlRequest("TERMINATE " + str + ";"));
  }

  private Vertx vertx;
  private Client client;

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
    client = createClient();
  }

  @After
  public void tearDown() {
    if (client != null) {
      client.close();
    }
    if (vertx != null) {
      vertx.close();
    }
    REST_APP.getServiceContext().close();
  }

  @Test
  public void shouldContinueStreamPushQueryV2AfterNetworkFault() throws Exception {
    //Given
    final String streamName = "SOURCE_SPQV2";
    makeKsqlRequest("CREATE STREAM " + streamName + " AS "
        + "SELECT * FROM " + TEST_STREAM + ";"
    );
    final String PUSH_QUERY_V2 =
        "SELECT * FROM SOURCE_SPQV2 EMIT CHANGES;";
    assertAllPersistentQueriesRunning();

    final StreamedQueryResult oldStreamedQueryResult = client.streamQuery(PUSH_QUERY_V2).get();

    assertThat(oldStreamedQueryResult.columnNames(), is(TEST_COLUMN_NAMES));
    assertThat(oldStreamedQueryResult.columnTypes(), is(TEST_COLUMN_TYPES));
    assertThat(oldStreamedQueryResult.queryID(), is(notNullValue()));
    assertExpectedScalablePushQueries(1);

    TEST_HARNESS.produceRows(TEST_TOPIC, TEST_DATA_PROVIDER, KEY_FORMAT, VALUE_FORMAT, TS_SUPPLIER, HEADERS_SUPPLIER);

    assertThatEventually(((StreamedQueryResultImpl) oldStreamedQueryResult)::hasContinuationToken, is(true));
    final String oldContinuationToken = ((StreamedQueryResultImpl) oldStreamedQueryResult).getContinuationToken().get();

    shouldReceiveStreamRows(oldStreamedQueryResult, false, TEST_NUM_ROWS, TEST_EXPECTED_ROWS );

    // Continuation token should get updated
    assertThatEventually(() -> oldContinuationToken.equals(((StreamedQueryResultImpl) oldStreamedQueryResult).getContinuationToken().get()), is(false));

    // Mimic a network fault by abruptly stopping and starting the server
    try {
      REST_APP.stop();
    } catch(Exception ignored) {
    }
    REST_APP.start();
    assertAllPersistentQueriesRunning();

    // When
    final StreamedQueryResult newStreamedQueryResult = oldStreamedQueryResult.continueFromLastContinuationToken().get();

    assertThat(oldStreamedQueryResult.columnNames(), is(TEST_COLUMN_NAMES));
    assertThat(oldStreamedQueryResult.columnTypes(), is(TEST_COLUMN_TYPES));
    assertThat(oldStreamedQueryResult.queryID(), is(notNullValue()));
    assertExpectedScalablePushQueries(1);
    
    TEST_HARNESS.produceRows(TEST_TOPIC, TEST_MORE_DATA_PROVIDER, KEY_FORMAT, VALUE_FORMAT, TS_SUPPLIER, HEADERS_SUPPLIER);

    // Then
    shouldReceiveStreamRows(newStreamedQueryResult, false, TEST_MORE_NUM_ROWS, TEST_MORE_EXPECTED_ROWS);
  }

  private Client createClient() {
    final ClientOptions clientOptions = ClientOptions.create()
        .setHost("localhost")
        .setPort(REST_APP.getListeners().get(0).getPort());
    return Client.create(clientOptions, vertx);
  }

  private static List<KsqlEntity> makeKsqlRequest(final String sql) {
    return RestIntegrationTestUtil.makeKsqlRequest(REST_APP, sql);
  }

  private static void shouldReceiveStreamRows(
      final Publisher<Row> publisher,
      final boolean subscriberCompleted,
      final int numRows,
      List<KsqlArray> expectedRows
  ) {
    shouldReceiveRows(
        publisher,
        numRows,
        rows -> verifyStreamRows(rows, numRows, expectedRows),
        subscriberCompleted
    );
  }

  private static void verifyStreamRows(final List<Row> rows, final int numRows,
      List<KsqlArray> expectedRows) {
    List<Row> orderedRows = rows.stream()
        .sorted(ClientTestUtil::compareRowByOrderedLong)
        .collect(Collectors.toList());
    for (int i = 0; i < Math.min(numRows, rows.size()); i++) {
      verifyStreamRowWithIndex(orderedRows.get(i), i, expectedRows);
    }
    if (rows.size() < numRows) {
      fail("Expected " + numRows + " but only got " + rows.size());
    } else if (rows.size() > numRows) {
      final List<Row> extra = rows.subList(numRows, rows.size());
      fail("Expected " + numRows + " but got " + rows.size() + ". The extra rows were: " + extra);
    }

    // not strictly necessary after the other checks, but just to specify the invariant
    assertThat(rows, hasSize(numRows));

  }

  private static void verifyStreamRowWithIndex(final Row row, final int index,
      List<KsqlArray> expectedRows) {
    List<KsqlArray> orderedRows = expectedRows.stream()
        .sorted(ClientTestUtil::compareKsqlArrayByOrderedLong)
        .collect(Collectors.toList());
    final KsqlArray expectedRow = orderedRows.get(index);

    // verify metadata
    assertThat(row.values(), equalTo(expectedRow));
    assertThat(row.columnNames(), equalTo(TEST_COLUMN_NAMES));
    assertThat(row.columnTypes(), equalTo(TEST_COLUMN_TYPES));

    // verify type-based getters
    assertThat(row.getKsqlObject("K"), is(expectedRow.getKsqlObject(0)));
    assertThat(row.getString("STR"), is(expectedRow.getString(1)));
    assertThat(row.getLong("LONG"), is(expectedRow.getLong(2)));
    assertThat(row.getDecimal("DEC"), is(expectedRow.getDecimal(3)));
    assertThat(row.getBytes("BYTES_"), is(expectedRow.getBytes(4)));
    assertThat(row.getKsqlArray("ARRAY"), is(expectedRow.getKsqlArray(5)));
    assertThat(row.getKsqlObject("MAP"), is(expectedRow.getKsqlObject(6)));
    assertThat(row.getKsqlObject("STRUCT"), is(expectedRow.getKsqlObject(7)));
    assertThat(row.getKsqlObject("COMPLEX"), is(expectedRow.getKsqlObject(8)));

    // verify index-based getters are 1-indexed
    assertThat(row.getKsqlObject(1), is(row.getKsqlObject("K")));
    assertThat(row.getString(2), is(row.getString("STR")));
    assertThat(row.getLong(3), is(row.getLong("LONG")));
    assertThat(row.getDecimal(4), is(row.getDecimal("DEC")));
    assertThat(row.getBytes(5), is(row.getBytes("BYTES_")));
    assertThat(row.getKsqlArray(6), is(row.getKsqlArray("ARRAY")));
    assertThat(row.getKsqlObject(7), is(row.getKsqlObject("MAP")));
    assertThat(row.getKsqlObject(8), is(row.getKsqlObject("STRUCT")));
    assertThat(row.getKsqlObject(9), is(row.getKsqlObject("COMPLEX")));

    // verify isNull() evaluation
    assertThat(row.isNull("STR"), is(false));

    // verify exception on invalid cast
    assertThrows(ClassCastException.class, () -> row.getInteger("STR"));

    // verify KsqlArray methods
    final KsqlArray values = row.values();
    assertThat(values.size(), is(TEST_COLUMN_NAMES.size()));
    assertThat(values.isEmpty(), is(false));
    assertThat(values.getKsqlObject(0), is(row.getKsqlObject("K")));
    assertThat(values.getString(1), is(row.getString("STR")));
    assertThat(values.getLong(2), is(row.getLong("LONG")));
    assertThat(values.getDecimal(3), is(row.getDecimal("DEC")));
    assertThat(values.getBytes(4), is(row.getBytes("BYTES_")));
    assertThat(values.getKsqlArray(5), is(row.getKsqlArray("ARRAY")));
    assertThat(values.getKsqlObject(6), is(row.getKsqlObject("MAP")));
    assertThat(values.getKsqlObject(7), is(row.getKsqlObject("STRUCT")));
    assertThat(values.getKsqlObject(8), is(row.getKsqlObject("COMPLEX")));
    assertThat(values.toJsonString(), is((new JsonArray(values.getList())).toString()));
    assertThat(values.toString(), is(values.toJsonString()));

    // verify KsqlObject methods
    final KsqlObject obj = row.asObject();
    assertThat(obj.size(), is(TEST_COLUMN_NAMES.size()));
    assertThat(obj.isEmpty(), is(false));
    assertThat(obj.fieldNames(), contains(TEST_COLUMN_NAMES.toArray()));
    assertThat(obj.getKsqlObject("K"), is(row.getKsqlObject("K")));
    assertThat(obj.getString("STR"), is(row.getString("STR")));
    assertThat(obj.getLong("LONG"), is(row.getLong("LONG")));
    assertThat(obj.getDecimal("DEC"), is(row.getDecimal("DEC")));
    assertThat(obj.getBytes("BYTES_"), is(row.getBytes("BYTES_")));
    assertThat(obj.getKsqlArray("ARRAY"), is(row.getKsqlArray("ARRAY")));
    assertThat(obj.getKsqlObject("MAP"), is(row.getKsqlObject("MAP")));
    assertThat(obj.getKsqlObject("STRUCT"), is(row.getKsqlObject("STRUCT")));
    assertThat(obj.getKsqlObject("COMPLEX"), is(row.getKsqlObject("COMPLEX")));
    assertThat(obj.containsKey("DEC"), is(true));
    assertThat(obj.containsKey("notafield"), is(false));
    assertThat(obj.toJsonString(), is((new JsonObject(obj.getMap())).toString()));
    assertThat(obj.toString(), is(obj.toJsonString()));
  }

  private static List<KsqlArray> convertToClientRows(
      final Multimap<GenericKey, GenericRow> data
  ) {
    final List<KsqlArray> expectedRows = new ArrayList<>();
    for (final Map.Entry<GenericKey, GenericRow> entry : data.entries()) {
      final KsqlArray expectedRow = new KsqlArray();
      addObjectToKsqlArray(expectedRow, entry.getKey().get(0));

      for (final Object value : entry.getValue().values()) {
        addObjectToKsqlArray(expectedRow, value);
      }

      // Add header column
      addObjectToKsqlArray(expectedRow, new byte[] {23});

      expectedRows.add(expectedRow);
    }
    return expectedRows;
  }

  private static void addObjectToKsqlArray(final KsqlArray array, final Object value) {
    if (value instanceof Struct) {
      array.add(StructuredTypesDataProvider.structToMap((Struct) value));
    } else if (value instanceof BigDecimal) {
      // Can't use expectedRow.add((BigDecimal) value) directly since client serializes BigDecimal as string,
      // whereas this method builds up the expected result (unrelated to serialization)
      array.addAll(new KsqlArray(Collections.singletonList(value)));
    } else if (value instanceof Timestamp) {
      array.add(SqlTimeTypes.formatTimestamp((Timestamp) value));
    } else if (value instanceof Date) {
      array.add(SqlTimeTypes.formatDate((Date) value));
    } else if (value instanceof Time) {
      array.add(SqlTimeTypes.formatTime((Time) value));
    } else if (value instanceof byte[]) {
      array.add(serializeVertX3CompatibleByte((byte[]) value));
    } else {
      array.add(value);
    }
  }

  /**
   * VertX 4 changed to serialize using Base64 without padding but our server
   * still encodes the data with padding - this uses the same serializer that
   * the server uses to serialize the data into a string
   *
   * @see <a href=https://github.com/eclipse-vertx/vert.x/pull/3197>vertx#3197</a>
   */
  private static String serializeVertX3CompatibleByte(final byte[] bytes) {
    try {
      // writeValueAsString by default adds quotes to both sides to make it valid
      // JSON
      final String escaped = ApiJsonMapper.INSTANCE.get().writeValueAsString(bytes);
      return escaped.substring(1, escaped.length() - 1);
    } catch (JsonProcessingException e) {
      throw new KsqlException(e);
    }
  }

  private void assertExpectedScalablePushQueries(
      final int expectedScalablePushQueries
  ) {
    assertThatEventually(() -> {
      for (final PersistentQueryMetadata metadata : REST_APP.getEngine().getPersistentQueries()) {
        if (metadata.getScalablePushRegistry().get().latestNumRegistered()
            < expectedScalablePushQueries
            || !metadata.getScalablePushRegistry().get().latestHasAssignment()) {
          return false;
        }
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      return true;
    }, is(true));
  }

  private void assertAllPersistentQueriesRunning() {
    assertThatEventually("persistent queries check", () -> {
      for (final PersistentQueryMetadata metadata : REST_APP.getEngine().getPersistentQueries()) {
        if (metadata.getState() != State.RUNNING) {
          return false;
        }
      }
      return true;
    },
    is(true),
    60000,
    TimeUnit.MILLISECONDS);
  }
}
