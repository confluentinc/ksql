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

package io.confluent.ksql.api.integration;

import static io.confluent.ksql.rest.Errors.ERROR_CODE_BAD_REQUEST;
import static io.confluent.ksql.rest.Errors.ERROR_CODE_BAD_STATEMENT;
import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster.VALID_USER2;
import static io.confluent.ksql.util.KsqlConfig.KSQL_DEFAULT_KEY_FORMAT_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_STREAMS_PREFIX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.api.utils.InsertsResponse;
import io.confluent.ksql.api.utils.PrintResponse;
import io.confluent.ksql.api.utils.QueryResponse;
import io.confluent.ksql.api.utils.ReceiveStream;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.rest.integration.RestIntegrationTestUtil;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.test.util.secure.ClientTrustStore;
import io.confluent.ksql.test.util.secure.Credentials;
import io.confluent.ksql.test.util.secure.SecureKafkaHelper;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.StructuredTypesDataProvider;
import io.confluent.ksql.util.VertxCompletableFuture;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

@Category({IntegrationTest.class})
public class ApiIntegrationTest {

  private static final StructuredTypesDataProvider TEST_DATA_PROVIDER =
      new StructuredTypesDataProvider();
  private static final String TEST_TOPIC = TEST_DATA_PROVIDER.topicName();
  private static final String TEST_STREAM = TEST_DATA_PROVIDER.sourceName();

  private static final String AGG_TABLE = "AGG_TABLE";
  private static final Credentials NORMAL_USER = VALID_USER2;
  private static final String AN_AGG_KEY = "STRUCT(F1 := ARRAY['a'])";

  private static final JsonObject COMPLEX_FIELD_VALUE = new JsonObject()
      .put("DECIMAL", 1.1) // JsonObject does not accept BigDecimal
      .put("STRUCT", new JsonObject().put("F1", "foo").put("F2", 3))
      .put("ARRAY_ARRAY", new JsonArray().add(new JsonArray().add("bar")))
      .put("ARRAY_STRUCT", new JsonArray().add(new JsonObject().put("F1", "x")))
      .put("ARRAY_MAP", new JsonArray().add(new JsonObject().put("k", 10)))
      .put("MAP_ARRAY", new JsonObject().put("k", new JsonArray().add("e1").add("e2")))
      .put("MAP_MAP", new JsonObject().put("k1", new JsonObject().put("k2", 5)))
      .put("MAP_STRUCT", new JsonObject().put("k", new JsonObject().put("F1", "baz")));

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.builder()
      .withKafkaCluster(
          EmbeddedSingleNodeKafkaCluster.newBuilder()
              .withoutPlainListeners()
              .withSaslSslListeners()
      ).build();

  private static final TestKsqlRestApp REST_APP = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty("security.protocol", "SASL_SSL")
      .withProperty("sasl.mechanism", "PLAIN")
      .withProperty("sasl.jaas.config", SecureKafkaHelper.buildJaasConfig(NORMAL_USER))
      .withProperties(ClientTrustStore.trustStoreProps())
      .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
      .withProperty(KSQL_DEFAULT_KEY_FORMAT_CONFIG, "JSON")
      .withProperty(KsqlConfig.KSQL_HEADERS_COLUMNS_ENABLED, true)
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS)
      .around(REST_APP);

  @BeforeClass
  public static void setUpClass() {
    TEST_HARNESS.ensureTopics(TEST_TOPIC);

    TEST_HARNESS.produceRows(TEST_TOPIC, TEST_DATA_PROVIDER, FormatFactory.JSON,
        FormatFactory.JSON);

    RestIntegrationTestUtil.createStream(REST_APP, TEST_DATA_PROVIDER);

    makeKsqlRequest("CREATE TABLE " + AGG_TABLE + " AS "
        + "SELECT K, LATEST_BY_OFFSET(LONG) AS LONG FROM " + TEST_STREAM + " GROUP BY K;"
    );
  }

  @AfterClass
  public static void classTearDown() {
    REST_APP.getPersistentQueries().forEach(str -> makeKsqlRequest("TERMINATE " + str + ";"));
  }

  private Vertx vertx;
  private WebClient client;

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
    client = createClient();
  }

  @After
  public void tearDown() {
    // Both client and vertx are currently being closed in the
    // shouldExecutePrintWithEmptyResponseIfConnectionClosed test
    // to simulate a connection closed by the client.
    if (client != null) {
      client.close();
    }
    if (vertx != null) {
      vertx.close();
    }
    REST_APP.getServiceContext().close();
  }

  @Test
  public void shouldExecutePushQueryWithLimit() {

    // Given:
    final String sql = "SELECT * from " + TEST_STREAM + " EMIT CHANGES LIMIT " + 2 + ";";

    // When:
    final QueryResponse response = executeQuery(sql);

    // Then:
    assertThat(response.rows, hasSize(2));
    assertThat(response.responseObject.getJsonArray("columnNames"), is(
        new JsonArray().add("K").add("STR").add("LONG").add("DEC").add("BYTES_").add("ARRAY")
            .add("MAP").add("STRUCT").add("COMPLEX").add("TIMESTAMP").add("DATE").add("TIME")
            .add("HEAD")));
    assertThat(response.responseObject.getJsonArray("columnTypes"), is(
        new JsonArray().add("STRUCT<`F1` ARRAY<STRING>>").add("STRING").add("BIGINT")
            .add("DECIMAL(4, 2)").add("BYTES").add("ARRAY<STRING>").add("MAP<STRING, STRING>")
            .add("STRUCT<`F1` INTEGER>")
            .add("STRUCT<`DECIMAL` DECIMAL(2, 1), `STRUCT` STRUCT<`F1` STRING, `F2` INTEGER>, "
                + "`ARRAY_ARRAY` ARRAY<ARRAY<STRING>>, `ARRAY_STRUCT` ARRAY<STRUCT<`F1` STRING>>, "
                + "`ARRAY_MAP` ARRAY<MAP<STRING, INTEGER>>,"
                + " `MAP_ARRAY` MAP<STRING, ARRAY<STRING>>, "
                + "`MAP_MAP` MAP<STRING, MAP<STRING, INTEGER>>,"
                + " `MAP_STRUCT` MAP<STRING, STRUCT<`F1` STRING>>>")
            .add("TIMESTAMP").add("DATE").add("TIME").add("BYTES")));
    assertThat(response.responseObject.getString("queryId"), is(notNullValue()));
  }

  @Test
  public void shouldExecutePushQueryWithVariableSubstitution() {

    // Given:
    final String sql = "SELECT DEC AS ${name} from " + TEST_STREAM + " EMIT CHANGES LIMIT 2;";

    // When:
    final QueryResponse response = executeQueryWithVariables(sql, new JsonObject().put("name", "COL"));

    // Then:
    assertThat(response.rows, hasSize(2));
    assertThat(response.responseObject.getJsonArray("columnNames"), is(new JsonArray().add("COL")));
    assertThat(response.responseObject.getJsonArray("columnTypes"),
        is(new JsonArray().add("DECIMAL(4, 2)")));
    assertThat(response.responseObject.getString("queryId"), is(notNullValue()));
  }

  @Test
  public void shouldFailPushQueryWithInvalidSql() {

    // Given:
    final String sql = "SLECTT * from " + TEST_STREAM + " EMIT CHANGES;";

    // Then:
    shouldFailToExecuteQuery(sql, "line 1:1: Syntax Error\n"
        + "Unknown statement 'SLECTT'\n"
        + "Did you mean 'SELECT'?");
  }

  @Test
  public void shouldFailPushQueryWithMoreThanOneStatement() {

    // Given:
    final String sql = "SELECT * from " + TEST_STREAM + " EMIT CHANGES;" +
        "SELECT * from " + TEST_STREAM + " EMIT CHANGES;";

    // Then:
    shouldFailToExecuteQuery(sql, "Expected exactly one KSQL statement; found 2 instead");
  }

  @Test
  public void shouldFailPushWithNonQuery() {

    // Given:
    final String sql =
        "CREATE STREAM SOME_STREAM AS SELECT * from " + TEST_STREAM + " EMIT CHANGES;";

    // Then:
    shouldFailToExecuteQuery(sql, "Neither a query nor a print statement");
  }

  @Test
  public void shouldExecutePushQueryNoLimit() throws Exception {

    KsqlEngine engine = (KsqlEngine) REST_APP.getEngine();
    // One persistent query for the agg table
    assertThatEventually(engine::numberOfLiveQueries, is(1));

    // Given:
    final String sql = "SELECT * from " + TEST_STREAM + " EMIT CHANGES;";

    // Create a write stream to capture the incomplete response
    ReceiveStream writeStream = new ReceiveStream(vertx);

    // Make the request to stream a query
    JsonObject properties = new JsonObject();
    JsonObject requestBody = new JsonObject()
        .put("sql", sql).put("properties", properties);
    VertxCompletableFuture<HttpResponse<Void>> responseFuture = new VertxCompletableFuture<>();
    client.post("/query-stream")
        .as(BodyCodec.pipe(writeStream))
        .sendJsonObject(requestBody, responseFuture);

    assertThatEventually(engine::numberOfLiveQueries, is(2));

    // Wait for all rows in the response to arrive
    assertThatEventually(() -> {
      try {
        Buffer buff = writeStream.getBody();
        QueryResponse queryResponse = new QueryResponse(buff.toString());
        return queryResponse.rows.size();
      } catch (Throwable t) {
        return -1;
      }
    }, greaterThanOrEqualTo(6));

    // The response shouldn't have ended yet
    assertThat(writeStream.isEnded(), is(false));

    QueryResponse queryResponse = new QueryResponse(writeStream.getBody().toString());
    String queryId = queryResponse.responseObject.getString("queryId");

    // Now send another request to close the query
    JsonObject closeQueryRequestBody = new JsonObject().put("queryId", queryId);
    HttpResponse<Buffer> closeQueryResponse = sendRequest(client, "/close-query",
        closeQueryRequestBody.toBuffer());
    assertThat(closeQueryResponse.statusCode(), is(200));

    // The response should now be ended
    assertThatEventually(writeStream::isEnded, is(true));
    HttpResponse<Void> response = responseFuture.get();
    assertThat(response.statusCode(), is(200));

    // Make sure it's cleaned up on the server
    assertThatEventually(engine::numberOfLiveQueries, is(1));
  }

  @Test
  public void shouldExecutePullQuery() {

    // Given:
    String sql = "SELECT * from " + AGG_TABLE + " WHERE K=" + AN_AGG_KEY + ";";

    // When:
    // Maybe need to retry as populating agg table is async
    AtomicReference<QueryResponse> atomicReference = new AtomicReference<>();
    assertThatEventually(() -> {
      QueryResponse queryResponse = executeQuery(sql);
      atomicReference.set(queryResponse);
      return queryResponse.rows;
    }, hasSize(1));

    QueryResponse response = atomicReference.get();

    // Then:
    JsonArray expectedColumnNames = new JsonArray().add("K").add("LONG");
    JsonArray expectedColumnTypes = new JsonArray().add("STRUCT<`F1` ARRAY<STRING>>").add("BIGINT");
    assertThat(response.rows, hasSize(1));
    assertThat(response.responseObject.getJsonArray("columnNames"), is(expectedColumnNames));
    assertThat(response.responseObject.getJsonArray("columnTypes"), is(expectedColumnTypes));
    assertThat(response.responseObject.getString("queryId"), startsWith("query_"));
    assertThat(response.rows.get(0).getJsonObject(0).getJsonArray("F1").getString(0),
        is("a")); // rowkey
    assertThat(response.rows.get(0).getLong(1), is(1L)); // latest_by_offset(long)
  }

  @Test
  public void shouldExecutePullQueryWithVariableSubstitution() {

    // Given:
    String sql = "SELECT * from ${AGG_TABLE} WHERE K=" + AN_AGG_KEY + ";";
    final JsonObject variables = new JsonObject().put("AGG_TABLE", AGG_TABLE);

    // When:
    // Maybe need to retry as populating agg table is async
    AtomicReference<QueryResponse> atomicReference = new AtomicReference<>();
    assertThatEventually(() -> {
      QueryResponse queryResponse = executeQueryWithVariables(sql, variables);
      atomicReference.set(queryResponse);
      return queryResponse.rows;
    }, hasSize(1));

    QueryResponse response = atomicReference.get();

    // Then:
    JsonArray expectedColumnNames = new JsonArray().add("K").add("LONG");
    JsonArray expectedColumnTypes = new JsonArray().add("STRUCT<`F1` ARRAY<STRING>>").add("BIGINT");
    assertThat(response.rows, hasSize(1));
    assertThat(response.responseObject.getJsonArray("columnNames"), is(expectedColumnNames));
    assertThat(response.responseObject.getJsonArray("columnTypes"), is(expectedColumnTypes));
    assertThat(response.responseObject.getString("queryId"), startsWith("query_"));
    assertThat(response.rows.get(0).getJsonObject(0).getJsonArray("F1").getString(0),
        is("a")); // rowkey
    assertThat(response.rows.get(0).getLong(1), is(1L)); // latest_by_offset(long)
  }

  @Test
  public void shouldFailPullQueryWithInvalidSql() {

    // Given:
    String sql = "SLLLECET * from " + AGG_TABLE + " WHERE STR='" + AN_AGG_KEY + "';";

    // Then:
    shouldFailToExecuteQuery(sql, "line 1:1: Syntax Error\n"
        + "Unknown statement 'SLLLECET'\n"
        + "Did you mean 'SELECT'?");
  }

  @Test
  public void shouldFailPullQueryWithMoreThanOneStatement() {

    // Given:
    String sql = "SELECT * from " + AGG_TABLE + " WHERE K=" + AN_AGG_KEY + ";" +
        "SELECT * from " + AGG_TABLE + " WHERE K=" + AN_AGG_KEY + ";";

    // Then:
    shouldFailToExecuteQuery(sql, "Expected exactly one KSQL statement; found 2 instead");
  }

  @Test
  public void shouldExecuteInserts() {

    // Given:
    JsonObject properties = new JsonObject();
    JsonObject requestBody = new JsonObject()
        .put("target", TEST_STREAM).put("properties", properties);
    Buffer bodyBuffer = requestBody.toBuffer();
    bodyBuffer.appendString("\n");

    int numRows = 10;

    for (int i = 0; i < numRows; i++) {
      JsonObject row = new JsonObject()
          .put("K", new JsonObject().put("F1", new JsonArray().add("my_key_" + i)))
          .put("STR", "Value_" + i)
          .put("LONG", 1000 + i)
          .put("DEC", i + 0.11) // JsonObject does not accept BigDecimal
          .put("ARRAY", new JsonArray().add("a_" + i).add("b_" + i))
          .put("MAP", new JsonObject().put("k1", "v1_" + i).put("k2", "v2_" + i))
          .put("STRUCT", new JsonObject().put("F1", i))
          .put("COMPLEX", COMPLEX_FIELD_VALUE);
      bodyBuffer.appendBuffer(row.toBuffer()).appendString("\n");
    }

    // When:
    HttpResponse<Buffer> response = sendRequest("/inserts-stream", bodyBuffer);

    // Then:
    assertThat(response.statusCode(), is(200));

    InsertsResponse insertsResponse = new InsertsResponse(response.bodyAsString());
    assertThat(insertsResponse.acks, hasSize(numRows));
    assertThat(insertsResponse.error, is(nullValue()));

    Set<Long> sequences = new HashSet<>();
    for (JsonObject ack : insertsResponse.acks) {
      sequences.add(ack.getLong("seq"));
    }
    assertThat(sequences, hasSize(numRows));
    for (long l = 0; l < numRows; l++) {
      assertThat(sequences.contains(l), is(true));
    }
  }

  @Test
  public void shouldFailToInsertWithMissingKey() {

    // Given:
    JsonObject row = new JsonObject()
        .put("STR", "HELLO")
        .put("LONG", 1000)
        .put("DEC", 12.21) // JsonObject does not accept BigDecimal
        .put("ARRAY", new JsonArray().add("a").add("b"))
        .put("MAP", new JsonObject().put("k1", "v1").put("k2", "v2"))
        .put("STRUCT", new JsonObject().put("F1", 3))
        .put("COMPLEX", COMPLEX_FIELD_VALUE);

    // Then:
    shouldFailToInsert(row, ERROR_CODE_BAD_REQUEST,
        "Key field must be specified: K");
  }

  @Test
  public void shouldFailToInsertWithNonMatchingKeyType() {

    // Given:
    JsonObject row = new JsonObject()
        .put("K", "bad type")
        .put("STR", "HELLO")
        .put("LONG", 1000)
        .put("DEC", 12.21) // JsonObject does not accept BigDecimal
        .put("ARRAY", new JsonArray().add("a").add("b"))
        .put("MAP", new JsonObject().put("k1", "v1").put("k2", "v2"))
        .put("STRUCT", new JsonObject().put("F1", 3))
        .put("COMPLEX", COMPLEX_FIELD_VALUE);

    // Then:
    shouldFailToInsert(row, ERROR_CODE_BAD_REQUEST,
        "Can't coerce a field of type class java.lang.String into type STRUCT<`F1` ARRAY<STRING>>");
  }

  @Test
  public void shouldFailToInsertWithNonMatchingValueType() {

    // Given:
    JsonObject row = new JsonObject()
        .put("K", new JsonObject().put("F1", new JsonArray().add("my_key")))
        .put("STR", "HELLO")
        .put("LONG", "not a number")
        .put("DEC", 12.21) // JsonObject does not accept BigDecimal
        .put("ARRAY", new JsonArray().add("a").add("b"))
        .put("MAP", new JsonObject().put("k1", "v1").put("k2", "v2"))
        .put("STRUCT", new JsonObject().put("F1", 3))
        .put("COMPLEX", COMPLEX_FIELD_VALUE);

    // Then:
    shouldFailToInsert(row, ERROR_CODE_BAD_REQUEST,
        "Can't coerce a field of type class java.lang.String into type BIGINT");
  }

  @Test
  public void shouldInsertWithMissingValueField() {

    // Given:
    JsonObject row = new JsonObject()
        .put("K", new JsonObject().put("F1", new JsonArray().add("my_key")))
        .put("STR", "HELLO")
        .put("DEC", 12.21) // JsonObject does not accept BigDecimal
        .put("BYTES_", new byte[]{0, 1, 2})
        .put("ARRAY", new JsonArray().add("a").add("b"))
        .put("MAP", new JsonObject().put("k1", "v1").put("k2", "v2"))
        .put("STRUCT", new JsonObject().put("F1", 3))
        .put("COMPLEX", COMPLEX_FIELD_VALUE);

    // Then:
    shouldInsert(row);
  }

  @Test
  public void shouldInsertWithCaseInsensitivity() {

    // Given: lowercase fields names and stream name
    String target = TEST_STREAM.toLowerCase();
    JsonObject row = new JsonObject()
        .put("k", new JsonObject().put("f1", new JsonArray().add("my_key")))
        .put("str", "HELLO")
        .put("dec", 12.21) // JsonObject does not accept BigDecimal
        .put("bytes_", new byte[]{0, 1, 2})
        .put("array", new JsonArray().add("a").add("b"))
        .put("map", new JsonObject().put("k1", "v1").put("k2", "v2"))
        .put("struct", new JsonObject().put("f1", 3))
        .put("COMPLEX", COMPLEX_FIELD_VALUE);

    // Then:
    shouldInsert(target, row);
  }

  @Test
  public void shouldTreatInsertTargetAsCaseSensitiveIfQuotedWithBackticks() {
    // Given:
    String target = "`" + TEST_STREAM.toLowerCase() + "`";
    JsonObject row = new JsonObject()
        .put("K", new JsonObject().put("F1", new JsonArray().add("my_key")))
        .put("STR", "HELLO")
        .put("LONG", 1000L)
        .put("DEC", 12.21) // JsonObject does not accept BigDecimal
        .put("BYTES_", new byte[]{0, 1, 2})
        .put("ARRAY", new JsonArray().add("a").add("b"))
        .put("MAP", new JsonObject().put("k1", "v1").put("k2", "v2"))
        .put("STRUCT", new JsonObject().put("F1", 3))
        .put("COMPLEX", COMPLEX_FIELD_VALUE);

    // Then: request fails because stream name is invalid
    shouldRejectInsertRequest(target, row,
        "Cannot insert values into an unknown stream/table: " + target
            + "\nDid you mean STRUCTURED_TYPES_KSTREAM? Hint: try removing double quotes from the source name.");
  }

  @Test
  public void shouldTreatInsertTargetAsCaseSensitiveIfQuotedWithDoubleQuotes() {
    // Given:
    String target = "\"" + TEST_STREAM.toLowerCase() + "\"";
    JsonObject row = new JsonObject()
        .put("K", new JsonObject().put("F1", new JsonArray().add("my_key")))
        .put("STR", "HELLO")
        .put("LONG", 1000L)
        .put("DEC", 12.21) // JsonObject does not accept BigDecimal
        .put("BYTES_", new byte[]{0, 1, 2})
        .put("ARRAY", new JsonArray().add("a").add("b"))
        .put("MAP", new JsonObject().put("k1", "v1").put("k2", "v2"))
        .put("STRUCT", new JsonObject().put("F1", 3))
        .put("COMPLEX", COMPLEX_FIELD_VALUE);

    // Then: request fails because stream name is invalid
    shouldRejectInsertRequest(target, row,
        "Cannot insert values into an unknown stream/table: `" + TEST_STREAM.toLowerCase() + "`"
            + "\nDid you mean STRUCTURED_TYPES_KSTREAM? Hint: try removing double quotes from the source name.");
  }

  @Test
  public void shouldTreatInsertColumnNamesAsCaseSensitiveIfQuotedWithBackticks() {
    // Given:
    JsonObject row = new JsonObject()
        .put("`k`", new JsonObject().put("F1", new JsonArray().add("my_key")))
        .put("STR", "HELLO")
        .put("LONG", 1000L)
        .put("DEC", 12.21) // JsonObject does not accept BigDecimal
        .put("BYTES_", new byte[]{0, 1, 2})
        .put("ARRAY", new JsonArray().add("a").add("b"))
        .put("MAP", new JsonObject().put("k1", "v1").put("k2", "v2"))
        .put("STRUCT", new JsonObject().put("F1", 3))
        .put("COMPLEX", COMPLEX_FIELD_VALUE);

    // Then: request fails because column name is incorrect
    shouldFailToInsert(row, ERROR_CODE_BAD_REQUEST, "Key field must be specified: K");
  }

  @Test
  public void shouldTreatInsertColumnNamesAsCaseSensitiveIfQuotedWithDoubleQuotes() {
    // Given:
    JsonObject row = new JsonObject()
        .put("\"k\"", new JsonObject().put("F1", new JsonArray().add("my_key")))
        .put("STR", "HELLO")
        .put("LONG", 1000L)
        .put("DEC", 12.21) // JsonObject does not accept BigDecimal
        .put("BYTES_", new byte[]{0, 1, 2})
        .put("ARRAY", new JsonArray().add("a").add("b"))
        .put("MAP", new JsonObject().put("k1", "v1").put("k2", "v2"))
        .put("STRUCT", new JsonObject().put("F1", 3))
        .put("COMPLEX", COMPLEX_FIELD_VALUE);

    // Then: request fails because column name is incorrect
    shouldFailToInsert(row, ERROR_CODE_BAD_REQUEST, "Key field must be specified: K");
  }

  @Test
  public void shouldExecutePushQueryFromLatestOffset() {

    KsqlEngine engine = (KsqlEngine) REST_APP.getEngine();
    // One persistent query for the agg table
    assertThatEventually(engine::numberOfLiveQueries, is(1));

    // Given:
    String sql = "SELECT * from " + TEST_STREAM + " EMIT CHANGES LIMIT 1;";

    // Create a write stream to capture the incomplete response
    ReceiveStream writeStream = new ReceiveStream(vertx);

    // Make the request to stream a query
    JsonObject queryProperties = new JsonObject().put("auto.offset.reset", "latest");
    JsonObject queryRequestBody = new JsonObject()
        .put("sql", sql).put("properties", queryProperties);
    VertxCompletableFuture<HttpResponse<Void>> responseFuture = new VertxCompletableFuture<>();
    client.post("/query-stream")
        .as(BodyCodec.pipe(writeStream))
        .sendJsonObject(queryRequestBody, responseFuture);

    assertThatEventually(engine::numberOfLiveQueries, is(2));

    // New row to insert
    JsonObject row = new JsonObject()
        .put("K", new JsonObject().put("F1",
            new JsonArray().add("my_key_shouldExecutePushQueryFromLatestOffset")))
        .put("STR", "Value_shouldExecutePushQueryFromLatestOffset")
        .put("LONG", 2000L)
        .put("DEC", 12.34) // JsonObject does not accept BigDecimal
        .put("BYTES_", new byte[]{0, 1, 2})
        .put("ARRAY", new JsonArray().add("a_shouldExecutePushQueryFromLatestOffset"))
        .put("MAP", new JsonObject().put("k1", "v1_shouldExecutePushQueryFromLatestOffset"))
        .put("STRUCT", new JsonObject().put("F1", 3))
        .put("COMPLEX", COMPLEX_FIELD_VALUE);

    // Insert a new row and wait for it to arrive
    assertThatEventually(() -> {
      try {
        shouldInsert(
            row); // Attempt the insert multiple times, in case the query hasn't started yet
        Buffer buff = writeStream.getBody();
        QueryResponse queryResponse = new QueryResponse(buff.toString());
        return queryResponse.rows.size();
      } catch (Throwable t) {
        return Integer.MAX_VALUE;
      }
    }, is(1));

    // Verify that the received row is the expected one
    Buffer buff = writeStream.getBody();
    QueryResponse queryResponse = new QueryResponse(buff.toString());
    assertThat(queryResponse.rows.get(0).getJsonObject(0), is(new JsonObject().put("F1",
        new JsonArray().add("my_key_shouldExecutePushQueryFromLatestOffset"))));
    assertThat(queryResponse.rows.get(0).getString(1),
        is("Value_shouldExecutePushQueryFromLatestOffset"));
    assertThat(queryResponse.rows.get(0).getLong(2), is(2000L));
    assertThat(queryResponse.rows.get(0).getDouble(3), is(12.34));
    assertThat(queryResponse.rows.get(0).getBinary(4), is(new byte[]{0, 1, 2}));
    assertThat(queryResponse.rows.get(0).getJsonArray(5),
        is(new JsonArray().add("a_shouldExecutePushQueryFromLatestOffset")));
    assertThat(queryResponse.rows.get(0).getJsonObject(6),
        is(new JsonObject().put("k1", "v1_shouldExecutePushQueryFromLatestOffset")));
    assertThat(queryResponse.rows.get(0).getJsonObject(7), is(new JsonObject().put("F1", 3)));
    assertThat(queryResponse.rows.get(0).getJsonObject(8), is(COMPLEX_FIELD_VALUE));

    // Check that query is cleaned up on the server
    assertThatEventually(engine::numberOfLiveQueries, is(1));
  }

  private void shouldFailToExecuteQuery(final String sql, final String message) {
    // When:
    QueryResponse response = executeQuery(sql);

    // Then:
    assertThat(response.rows, hasSize(0));
    assertThat(response.responseObject.getInteger("error_code"),
        is(ERROR_CODE_BAD_STATEMENT));
    assertThat(response.responseObject.getString("message"),
        startsWith(message));
  }

  private void shouldFailToExecutePrint(final String sql, final String message) {
    // When:
    PrintResponse printResponse = executePrintTopic(sql);
    
    // Then:
    assertThat(printResponse.rows, hasSize(0));
    assertThat(printResponse.error.getInteger("error_code"), is(ERROR_CODE_BAD_STATEMENT));
    assertThat(printResponse.error.getString("message"), startsWith(message));
  }

  @Test
  public void shouldExecutePrint() {
    // Given:
    String sql = "PRINT " + TEST_TOPIC + " FROM BEGINNING LIMIT 1;";

    // Create a write stream to capture the incomplete response
    ReceiveStream writeStream = new ReceiveStream(vertx);

    // Make the request to stream a print
    JsonObject printRequestBody = new JsonObject().put("sql", sql);
    VertxCompletableFuture<HttpResponse<Void>> responseFuture = new VertxCompletableFuture<>();

    // When:
    client.post("/query-stream").as(BodyCodec.pipe(writeStream))
        .sendJsonObject(printRequestBody, responseFuture);

    // Then:
    AtomicReference<PrintResponse> atomicReference = new AtomicReference<>();
    assertThatEventually(() -> {
      try {
        Buffer buff = writeStream.getBody();
        PrintResponse printResponse = new PrintResponse(buff.toString());
        atomicReference.set(printResponse);
        return printResponse.rows.size();
      } catch (Throwable t) {
        return -1;
      }
    }, is(1));

    PrintResponse printResponse = atomicReference.get();

    assertThatEventually(() -> {
      try {
        return responseFuture.isDone();
      } catch (Throwable t) {
        return false;
      }
    }, is(true));
    assertThat(writeStream.isEnded(), is(true));
    assertThat(printResponse.rows.get(0), containsString(
        "key: {\"F1\":[\"a\"]}, value: {\"STR\":\"FOO\",\"LONG\":1,\"DEC\":1.11,\"BYTES_\":\"AQ==\",\"ARRAY\":[\"a\"],\"MAP\":{\"k1\":\"v1\"},\"STRUCT\":{\"F1\":2},\"COMPLEX\":{\"DECIMAL\":0.0,\"STRUCT\":{\"F1\":\"v0\",\"F2\":0},\"ARRAY_ARRAY\":[[\"foo\"]],\"ARRAY_STRUCT\":[{\"F1\":\"v0\"}],\"ARRAY_MAP\":[{\"k1\":0}],\"MAP_ARRAY\":{\"k\":[\"v0\"]},\"MAP_MAP\":{\"k\":{\"k\":0}},\"MAP_STRUCT\":{\"k\":{\"F1\":\"v0\"}}},\"TIMESTAMP\":1,\"DATE\":1,\"TIME\":0}, partition: 0"));
  }

  @Test
  public void shouldTryToPrintAndSendDoneResponseIfConnectionClosed() {
    // Given:
    KsqlEngine engine = (KsqlEngine) REST_APP.getEngine();
    String sql = "PRINT " + TEST_TOPIC + ";";
    ReceiveStream writeStream = new ReceiveStream(vertx);
    JsonObject printRequestBody = new JsonObject().put("sql", sql);
    VertxCompletableFuture<HttpResponse<Void>> responseFuture = new VertxCompletableFuture<>();
    AtomicReference<VertxCompletableFuture<HttpResponse<Void>>> responseFutureRef = new AtomicReference<>(
        responseFuture);
    AtomicReference<Boolean> doneAtomicReference = new AtomicReference<>(
        false);

    // When:
    client.post("/query-stream").as(BodyCodec.pipe(writeStream))
        .sendJsonObject(printRequestBody, responseFuture);
    vertx.setTimer(1000, timerId -> {
      // Only the persistent query for the agg table running in the background should be left
      assertThatEventually(engine::numberOfLiveQueries, is(1));
      // We close the client and vertx to simulate a connection closed from the client.
      // (Closing the only the client doesn't seem to be enough to simulate this for some reason)
      // This could be removed if if we can somehow do this in a different
      // or if the test becomes flaky.
      client.close();
      vertx.close().onComplete(v -> {
        VertxCompletableFuture<HttpResponse<Void>> response = responseFutureRef.get();
        if (response.isDone()) {
          doneAtomicReference.set(true);
        } else {
          throw new AssertionError("Response is not done");
        }
      });
    });

    // Then:
    assertThatEventually(() -> {
      try {
        return doneAtomicReference.get();
      } catch (Throwable t) {
        return false;
      }
    }, is(true));
  }

  @Test
  public void shouldExecutePrintQueryNoLimit() {
    // Given:
    String sql = "PRINT " + TEST_TOPIC + " FROM BEGINNING;";

    // Create a write stream to capture the incomplete response
    ReceiveStream writeStream = new ReceiveStream(vertx);

    // Make the request to stream a print
    JsonObject printRequestBody = new JsonObject().put("sql", sql);
    VertxCompletableFuture<HttpResponse<Void>> responseFuture = new VertxCompletableFuture<>();

    // When:
    client.post("/query-stream").timeout(Long.MAX_VALUE).as(BodyCodec.pipe(writeStream))
        .sendJsonObject(printRequestBody, responseFuture);

    // Then:
    AtomicReference<PrintResponse> atomicReference = new AtomicReference<>();
    assertThatEventually(() -> {
      try {
        Buffer buff = writeStream.getBody();
        PrintResponse printResponse = new PrintResponse(buff.toString());
        atomicReference.set(printResponse);
        return printResponse.rows.size();
      } catch (Throwable t) {
        return -1;
      }
    }, greaterThanOrEqualTo(6));

    PrintResponse printResponse = atomicReference.get();
    assertThat(writeStream.isEnded(), is(false));
    assertThat(printResponse.error, is(nullValue()));
    assertThat(printResponse.rows.get(5), containsString(
        "key: {\"F1\":[\"d\"]}, value: {\"STR\":\"BUZZ\",\"LONG\":6,\"DEC\":10.1,\"BYTES_\":\"Bg==\",\"ARRAY\":[\"f\",\"g\"],\"MAP\":{},\"STRUCT\":{\"F1\":null},\"COMPLEX\":{\"DECIMAL\":5.0,\"STRUCT\":{\"F1\":\"v5\",\"F2\":5},\"ARRAY_ARRAY\":[[\"foo\"]],\"ARRAY_STRUCT\":[{\"F1\":\"v5\"}],\"ARRAY_MAP\":[{\"k1\":5}],\"MAP_ARRAY\":{\"k\":[\"v5\"]},\"MAP_MAP\":{\"k\":{\"k\":5}},\"MAP_STRUCT\":{\"k\":{\"F1\":\"v5\"}}},\"TIMESTAMP\":12,\"DATE\":12,\"TIME\":12}, partition: 0"));
  }

  @Test
  public void shouldExecutePrintQueryFromBeginningWithLimit() {
    // Given:
    String sql = "PRINT " + TEST_TOPIC + " FROM BEGINNING LIMIT 3;";

    // When:
    AtomicReference<PrintResponse> atomicReference = new AtomicReference<>();
    assertThatEventually(() -> {
      PrintResponse printResponse = executePrintTopic(sql);
      atomicReference.set(printResponse);
      return printResponse.rows.size();
    }, is(3));

    PrintResponse printResponse = atomicReference.get();

    // Then:
    assertThat(printResponse.rows.get(0), containsString("rowtime:"));
    assertThat(printResponse.rows.get(0), containsString(
        "key: {\"F1\":[\"a\"]}, value: {\"STR\":\"FOO\",\"LONG\":1,\"DEC\":1.11,\"BYTES_\":\"AQ==\",\"ARRAY\":[\"a\"],\"MAP\":{\"k1\":\"v1\"},\"STRUCT\":{\"F1\":2},\"COMPLEX\":{\"DECIMAL\":0.0,\"STRUCT\":{\"F1\":\"v0\",\"F2\":0},\"ARRAY_ARRAY\":[[\"foo\"]],\"ARRAY_STRUCT\":[{\"F1\":\"v0\"}],\"ARRAY_MAP\":[{\"k1\":0}],\"MAP_ARRAY\":{\"k\":[\"v0\"]},\"MAP_MAP\":{\"k\":{\"k\":0}},\"MAP_STRUCT\":{\"k\":{\"F1\":\"v0\"}}},\"TIMESTAMP\":1,\"DATE\":1,\"TIME\":0}, partition: 0"));
    assertThat(printResponse.rows.get(1), containsString("rowtime:"));
    assertThat(printResponse.rows.get(1), containsString(
        "key: {\"F1\":[\"b\"]}, value: {\"STR\":\"BAR\",\"LONG\":2,\"DEC\":2.22,\"BYTES_\":\"Ag==\",\"ARRAY\":[],\"MAP\":{},\"STRUCT\":{\"F1\":3},\"COMPLEX\":{\"DECIMAL\":1.0,\"STRUCT\":{\"F1\":\"v1\",\"F2\":1},\"ARRAY_ARRAY\":[[\"foo\"]],\"ARRAY_STRUCT\":[{\"F1\":\"v1\"}],\"ARRAY_MAP\":[{\"k1\":1}],\"MAP_ARRAY\":{\"k\":[\"v1\"]},\"MAP_MAP\":{\"k\":{\"k\":1}},\"MAP_STRUCT\":{\"k\":{\"F1\":\"v1\"}}},\"TIMESTAMP\":2,\"DATE\":2,\"TIME\":1}, partition: 0"));
    assertThat(printResponse.rows.get(2), containsString("rowtime:"));
    assertThat(printResponse.rows.get(2), containsString(
        "key: {\"F1\":[\"c\"]}, value: {\"STR\":\"BAZ\",\"LONG\":3,\"DEC\":30.33,\"BYTES_\":\"Aw==\",\"ARRAY\":[\"b\"],\"MAP\":{},\"STRUCT\":{\"F1\":null},\"COMPLEX\":{\"DECIMAL\":2.0,\"STRUCT\":{\"F1\":\"v2\",\"F2\":2},\"ARRAY_ARRAY\":[[\"foo\"]],\"ARRAY_STRUCT\":[{\"F1\":\"v2\"}],\"ARRAY_MAP\":[{\"k1\":2}],\"MAP_ARRAY\":{\"k\":[\"v2\"]},\"MAP_MAP\":{\"k\":{\"k\":2}},\"MAP_STRUCT\":{\"k\":{\"F1\":\"v2\"}}},\"TIMESTAMP\":3,\"DATE\":3,\"TIME\":2}, partition: 0"));
  }

  @Test
  public void shouldExecutePrintQueryFromEarliestOffsetWithLimit() {
    // Given:
    String sql = "PRINT " + TEST_TOPIC + " LIMIT 3;";

    // When:
    AtomicReference<PrintResponse> atomicReference = new AtomicReference<>();
    assertThatEventually(() -> {
      JsonObject printProperties = new JsonObject().put("auto.offset.reset", "earliest");
      PrintResponse printResponse = executePrintTopicWithProperties(sql, printProperties);
      atomicReference.set(printResponse);
      return printResponse.rows.size();
    }, is(3));

    PrintResponse printResponse = atomicReference.get();

    // Then:
    assertThat(printResponse.rows.get(0), containsString("rowtime:"));
    assertThat(printResponse.rows.get(0), containsString(
        "key: {\"F1\":[\"a\"]}, value: {\"STR\":\"FOO\",\"LONG\":1,\"DEC\":1.11,\"BYTES_\":\"AQ==\",\"ARRAY\":[\"a\"],\"MAP\":{\"k1\":\"v1\"},\"STRUCT\":{\"F1\":2},\"COMPLEX\":{\"DECIMAL\":0.0,\"STRUCT\":{\"F1\":\"v0\",\"F2\":0},\"ARRAY_ARRAY\":[[\"foo\"]],\"ARRAY_STRUCT\":[{\"F1\":\"v0\"}],\"ARRAY_MAP\":[{\"k1\":0}],\"MAP_ARRAY\":{\"k\":[\"v0\"]},\"MAP_MAP\":{\"k\":{\"k\":0}},\"MAP_STRUCT\":{\"k\":{\"F1\":\"v0\"}}},\"TIMESTAMP\":1,\"DATE\":1,\"TIME\":0}, partition: 0"));
    assertThat(printResponse.rows.get(1), containsString("rowtime:"));
    assertThat(printResponse.rows.get(1), containsString(
        "key: {\"F1\":[\"b\"]}, value: {\"STR\":\"BAR\",\"LONG\":2,\"DEC\":2.22,\"BYTES_\":\"Ag==\",\"ARRAY\":[],\"MAP\":{},\"STRUCT\":{\"F1\":3},\"COMPLEX\":{\"DECIMAL\":1.0,\"STRUCT\":{\"F1\":\"v1\",\"F2\":1},\"ARRAY_ARRAY\":[[\"foo\"]],\"ARRAY_STRUCT\":[{\"F1\":\"v1\"}],\"ARRAY_MAP\":[{\"k1\":1}],\"MAP_ARRAY\":{\"k\":[\"v1\"]},\"MAP_MAP\":{\"k\":{\"k\":1}},\"MAP_STRUCT\":{\"k\":{\"F1\":\"v1\"}}},\"TIMESTAMP\":2,\"DATE\":2,\"TIME\":1}, partition: 0"));
    assertThat(printResponse.rows.get(2), containsString("rowtime:"));
    assertThat(printResponse.rows.get(2), containsString(
        "key: {\"F1\":[\"c\"]}, value: {\"STR\":\"BAZ\",\"LONG\":3,\"DEC\":30.33,\"BYTES_\":\"Aw==\",\"ARRAY\":[\"b\"],\"MAP\":{},\"STRUCT\":{\"F1\":null},\"COMPLEX\":{\"DECIMAL\":2.0,\"STRUCT\":{\"F1\":\"v2\",\"F2\":2},\"ARRAY_ARRAY\":[[\"foo\"]],\"ARRAY_STRUCT\":[{\"F1\":\"v2\"}],\"ARRAY_MAP\":[{\"k1\":2}],\"MAP_ARRAY\":{\"k\":[\"v2\"]},\"MAP_MAP\":{\"k\":{\"k\":2}},\"MAP_STRUCT\":{\"k\":{\"F1\":\"v2\"}}},\"TIMESTAMP\":3,\"DATE\":3,\"TIME\":2}, partition: 0"));
  }

  @Test
  public void shouldFailToPrintNonExistingTopic() {
    // Given:
    String sql = "PRINT `NON_EXISTING_TOPIC`;";

    // Then:
    shouldFailToExecutePrint(sql, "Topic does not exist: NON_EXISTING_TOPIC");
  }

  @Test
  public void shouldFailToPrintWithInvalidSql() {
    // Given:
    String sql = "PRONT `NON_EXISTING_TOPIC`;";

    // Then:
    shouldFailToExecutePrint(sql,
        "line 1:1: Syntax Error\nUnknown statement 'PRONT'\nDid you mean 'PRINT'?\nStatement: PRONT `NON_EXISTING_TOPIC`;");
  }

  // Print topic currently only supports delimited format. Sendung an "accept" header with
  // "application/json" or any other format will result in a 406 (Not Acceptable) response.
  @Test
  public void shouldFailToPrintInJsonFormat() {
    // Given:
    String sql = "PRINT " + TEST_TOPIC + " FROM BEGINNING LIMIT 1;";

    // Create a write stream to capture the incomplete response
    ReceiveStream writeStream = new ReceiveStream(vertx);

    // Make the request to stream a print
    JsonObject printRequestBody = new JsonObject().put("sql", sql);
    VertxCompletableFuture<HttpResponse<Void>> responseFuture = new VertxCompletableFuture<>();

    // When:
    client.post("/query-stream").as(BodyCodec.pipe(writeStream))
        .putHeader("Accept", "application/json")
        .sendJsonObject(printRequestBody, responseFuture);

    try {
      HttpResponse<Void> response = responseFuture.get();

      assertThat(response.statusCode(), is(406));
      assertThat(responseFuture.get().statusMessage(), is("Not Acceptable"));
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  private QueryResponse executeQuery(final String sql) {
    return executeQueryWithVariables(sql, new JsonObject());
  }

  private QueryResponse executeQueryWithVariables(final String sql, final JsonObject variables) {
    JsonObject properties = new JsonObject();
    JsonObject requestBody = new JsonObject()
        .put("sql", sql).put("properties", properties).put("sessionVariables", variables);
    HttpResponse<Buffer> response = sendRequest("/query-stream", requestBody.toBuffer());
    return new QueryResponse(response.bodyAsString());
  }

  private PrintResponse executePrintTopic(final String sql) {
    JsonObject properties = new JsonObject();
    return executePrintTopicWithProperties(sql, properties);
  }

  private PrintResponse executePrintTopicWithProperties(final String sql,
      final JsonObject properties) {
    JsonObject requestBody = new JsonObject().put("sql", sql).put("properties", properties);
    HttpResponse<Buffer> response = sendRequest("/query-stream", requestBody.toBuffer());
    return new PrintResponse(response.bodyAsString());
  }

  private void shouldFailToInsert(final JsonObject row, final int errorCode,
      final String message) {
    final HttpResponse<Buffer> response = makeInsertsRequest(TEST_STREAM, row);

    assertThat(response.statusCode(), is(200));

    InsertsResponse insertsResponse = new InsertsResponse(response.bodyAsString());
    assertThat(insertsResponse.acks, hasSize(0));
    assertThat(insertsResponse.error, is(notNullValue()));
    assertThat(insertsResponse.error.getInteger("error_code"), is(errorCode));
    assertThat(insertsResponse.error.getString("message"),
        startsWith(message));
  }

  private void shouldInsert(final JsonObject row) {
    shouldInsert(TEST_STREAM, row);
  }

  private void shouldInsert(final String target, final JsonObject row) {
    HttpResponse<Buffer> response = makeInsertsRequest(target, row);

    assertThat(response.statusCode(), is(200));

    InsertsResponse insertsResponse = new InsertsResponse(response.bodyAsString());
    assertThat(insertsResponse.acks, hasSize(1));
    assertThat(insertsResponse.error, is(nullValue()));
  }

  private void shouldRejectInsertRequest(final String target, final JsonObject row,
      final String message) {
    HttpResponse<Buffer> response = makeInsertsRequest(target, row);

    assertThat(response.statusCode(), is(400));
    assertThat(response.statusMessage(), is("Bad Request"));

    QueryResponse queryResponse = new QueryResponse(response.bodyAsString());
    assertThat(queryResponse.responseObject.getInteger("error_code"),
        is(ERROR_CODE_BAD_STATEMENT));
    assertThat(queryResponse.responseObject.getString("message"), is(message));
  }

  private HttpResponse<Buffer> makeInsertsRequest(final String target, final JsonObject row) {
    JsonObject properties = new JsonObject();
    JsonObject requestBody = new JsonObject()
        .put("target", target).put("properties", properties);
    Buffer bodyBuffer = requestBody.toBuffer();
    bodyBuffer.appendString("\n");

    bodyBuffer.appendBuffer(row.toBuffer()).appendString("\n");

    return sendRequest("/inserts-stream", bodyBuffer);
  }

  private WebClient createClient() {
    WebClientOptions options = new WebClientOptions().
        setProtocolVersion(HttpVersion.HTTP_2).setHttp2ClearTextUpgrade(false)
        .setDefaultHost("localhost").setDefaultPort(REST_APP.getListeners().get(0).getPort());
    return WebClient.create(vertx, options);
  }

  private HttpResponse<Buffer> sendRequest(final String uri, final Buffer requestBody) {
    return sendRequest(client, uri, requestBody);
  }

  private HttpResponse<Buffer> sendRequest(final WebClient client, final String uri,
      final Buffer requestBody) {
    VertxCompletableFuture<HttpResponse<Buffer>> requestFuture = new VertxCompletableFuture<>();
    client
        .post(uri)
        .sendBuffer(requestBody, requestFuture);
    try {
      return requestFuture.get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void makeKsqlRequest(final String sql) {
    RestIntegrationTestUtil.makeKsqlRequest(REST_APP, sql);
  }


}
