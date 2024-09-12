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

import static io.confluent.ksql.api.client.util.ClientTestUtil.subscribeAndWait;
import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static io.confluent.ksql.util.KsqlConfig.KSQL_DEFAULT_KEY_FORMAT_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_STREAMS_PREFIX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.api.client.AcksPublisher;
import io.confluent.ksql.api.client.BatchedQueryResult;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.ExecuteStatementResult;
import io.confluent.ksql.api.client.InsertAck;
import io.confluent.ksql.api.client.InsertsPublisher;
import io.confluent.ksql.api.client.KsqlArray;
import io.confluent.ksql.api.client.KsqlObject;
import io.confluent.ksql.api.client.QueryInfo;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.client.StreamedQueryResult;
import io.confluent.ksql.api.client.exception.KsqlClientException;
import io.confluent.ksql.api.client.util.ClientTestUtil.TestSubscriber;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.rest.entity.ConnectorList;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.integration.RestIntegrationTestUtil;
import io.confluent.ksql.rest.server.ConnectExecutable;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.StructuredTypesDataProvider;
import io.confluent.ksql.util.TestDataProvider;
import io.vertx.core.Vertx;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
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

@Category({IntegrationTest.class})
public class ClientMutationIntegrationTest {

  private static final StructuredTypesDataProvider TEST_DATA_PROVIDER =
      new StructuredTypesDataProvider("STRUCTURED_TYPES_MUTABLE");
  private static final String TEST_TOPIC = TEST_DATA_PROVIDER.topicName();
  private static final String TEST_STREAM = TEST_DATA_PROVIDER.sourceName();

  private static final TestDataProvider EMPTY_TEST_DATA_PROVIDER = new TestDataProvider(
      "EMPTY_STRUCTURED_TYPES_MUTABLE",
      TEST_DATA_PROVIDER.schema(),
      ImmutableListMultimap.of()
  );
  private static final String EMPTY_TEST_TOPIC = EMPTY_TEST_DATA_PROVIDER.topicName();
  private static final String EMPTY_TEST_STREAM = EMPTY_TEST_DATA_PROVIDER.sourceName();

  private static final TestDataProvider EMPTY_TEST_DATA_PROVIDER_2 = new TestDataProvider(
      "EMPTY_STRUCTURED_TYPES_2_MUTABLE",
      TEST_DATA_PROVIDER.schema(),
      ImmutableListMultimap.of()
  );
  private static final String EMPTY_TEST_TOPIC_2 = EMPTY_TEST_DATA_PROVIDER_2.topicName();
  private static final String EMPTY_TEST_STREAM_2 = EMPTY_TEST_DATA_PROVIDER_2.sourceName();

  private static final Format KEY_FORMAT = FormatFactory.JSON;
  private static final Format VALUE_FORMAT = FormatFactory.JSON;

  private static final String TEST_TABLE = "TEST_TABLE";
  private static final String NON_EXIST_TABLE = "NON_EXIST_TABLE";
  private static final String AGG_TABLE = "AGG_TABLE";
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

  private static final KsqlObject COMPLEX_FIELD_VALUE = new KsqlObject()
      .put("DECIMAL", new BigDecimal("1.1"))
      .put("STRUCT", new KsqlObject().put("F1", "foo").put("F2", 3))
      .put("ARRAY_ARRAY", new KsqlArray().add(new KsqlArray().add("bar")))
      .put("ARRAY_STRUCT", new KsqlArray().add(new KsqlObject().put("F1", "x")))
      .put("ARRAY_MAP", new KsqlArray().add(new KsqlObject().put("k", 10)))
      .put("MAP_ARRAY", new KsqlObject().put("k", new KsqlArray().add("e1").add("e2")))
      .put("MAP_MAP", new KsqlObject().put("k1", new KsqlObject().put("k2", 5)))
      .put("MAP_STRUCT", new KsqlObject().put("k", new KsqlObject().put("F1", "baz")));
  private static final KsqlObject EXPECTED_COMPLEX_FIELD_VALUE = COMPLEX_FIELD_VALUE.copy()
      .put("DECIMAL", 1.1d); // Expect raw decimal value, whereas put(BigDecimal) serializes as string to avoid loss of precision

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
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS)
      .around(REST_APP);

  private static ConnectExecutable CONNECT;

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
    makeKsqlRequest(
        "CREATE TABLE " + TEST_TABLE + " (K STRING PRIMARY KEY, LONG BIGINT) WITH (KAFKA_TOPIC='"
            + TEST_TABLE + "', VALUE_FORMAT='json', PARTITIONS=1);");

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

    CONNECT = ConnectExecutable.of(connectFilePath);
    CONNECT.startAsync();
  }

  private static void writeConnectConfigs(final String path, final Map<String, String> configs) throws Exception {
    try (final PrintWriter out = new PrintWriter(new OutputStreamWriter(
        new FileOutputStream(path, true), StandardCharsets.UTF_8))) {
      for (final Map.Entry<String, String> entry : configs.entrySet()) {
        out.println(entry.getKey() + "=" + entry.getValue());
      }
    }
  }

  @AfterClass
  public static void classTearDown() {
    cleanupConnectors();
    CONNECT.shutdown();
    REST_APP.getPersistentQueries().forEach(str -> makeKsqlRequest("TERMINATE " + str + ";"));
    REST_APP.stop();
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
  public void shouldStreamQueryWithProperties() throws Exception {
    // Given
    final Map<String, Object> properties = new HashMap<>();
    properties.put("auto.offset.reset", "latest");
    final String sql = "SELECT * FROM " + TEST_STREAM + " EMIT CHANGES LIMIT 1;";

    final KsqlObject insertRow = new KsqlObject()
        .put("K", new KsqlObject().put("F1", new KsqlArray().add("my_key_shouldStreamQueryWithProperties")))
        .put("STR", "Value_shouldStreamQueryWithProperties")
        .put("LONG", 2000L)
        .put("DEC", new BigDecimal("12.34"))
        .put("BYTES_", new byte[]{0, 1, 2})
        .put("ARRAY", new KsqlArray().add("v1_shouldStreamQueryWithProperties").add("v2_shouldStreamQueryWithProperties"))
        .put("MAP", new KsqlObject().put("test_name", "shouldStreamQueryWithProperties"))
        .put("STRUCT", new KsqlObject().put("F1", 4))
        .put("COMPLEX", COMPLEX_FIELD_VALUE)
        .put("TIMESTAMP", "1970-01-01T00:00:00.001")
        .put("DATE", "1970-01-01")
        .put("TIME", "00:00:00");

    // When
    final StreamedQueryResult queryResult = client.streamQuery(sql, properties).get();

    // Then: a newly inserted row arrives
    final Row row = assertThatEventually(() -> {
      // Potentially try inserting multiple times, in case the query wasn't started by the first time
      try {
        client.insertInto(TEST_STREAM, insertRow).get();
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
      return queryResult.poll(Duration.ofMillis(10));
    }, is(notNullValue()));

    assertThat(row.getKsqlObject("K"), is(new KsqlObject().put("F1", new KsqlArray().add("my_key_shouldStreamQueryWithProperties"))));
    assertThat(row.getString("STR"), is("Value_shouldStreamQueryWithProperties"));
    assertThat(row.getLong("LONG"), is(2000L));
    assertThat(row.getDecimal("DEC"), is(new BigDecimal("12.34")));
    assertThat(row.getBytes("BYTES_"), is(new byte[]{0, 1, 2}));
    assertThat(row.getKsqlArray("ARRAY"), is(new KsqlArray().add("v1_shouldStreamQueryWithProperties").add("v2_shouldStreamQueryWithProperties")));
    assertThat(row.getKsqlObject("MAP"), is(new KsqlObject().put("test_name", "shouldStreamQueryWithProperties")));
    assertThat(row.getKsqlObject("STRUCT"), is(new KsqlObject().put("F1", 4)));
    assertThat(row.getKsqlObject("COMPLEX"), is(EXPECTED_COMPLEX_FIELD_VALUE));
    assertThat(row.getString("TIMESTAMP"), is("1970-01-01T00:00:00.001"));
    assertThat(row.getString("DATE"), is("1970-01-01"));
    assertThat(row.getString("TIME"), is("00:00"));
  }

  @Test
  public void shouldExecuteQueryWithProperties() {
    // Given
    final Map<String, Object> properties = new HashMap<>();
    properties.put("auto.offset.reset", "latest");
    final String sql = "SELECT * FROM " + TEST_STREAM + " EMIT CHANGES LIMIT 1;";

    final KsqlObject insertRow = new KsqlObject()
        .put("K", new KsqlObject().put("F1", new KsqlArray().add("my_key_shouldExecuteQueryWithProperties")))
        .put("STR", "Value_shouldExecuteQueryWithProperties")
        .put("LONG", 2000L)
        .put("DEC", new BigDecimal("12.34"))
        .put("BYTES_", new byte[]{0, 1, 2})
        .put("ARRAY", new KsqlArray().add("v1_shouldExecuteQueryWithProperties").add("v2_shouldExecuteQueryWithProperties"))
        .put("MAP", new KsqlObject().put("test_name", "shouldExecuteQueryWithProperties"))
        .put("STRUCT", new KsqlObject().put("F1", 4))
        .put("COMPLEX", COMPLEX_FIELD_VALUE)
        .put("TIMESTAMP", "1970-01-01T00:00:00.001")
        .put("DATE", "1970-01-01")
        .put("TIME", "00:00:00");

    // When
    final BatchedQueryResult queryResult = client.executeQuery(sql, properties);

    // Then: a newly inserted row arrives

    // Wait for row to arrive
    final AtomicReference<Row> rowRef = new AtomicReference<>();
    new Thread(() -> {
      try {
        final List<Row> rows = queryResult.get();
        assertThat(rows, hasSize(1));
        rowRef.set(rows.get(0));
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }).start();

    // Insert a new row
    final Row row = assertThatEventually(() -> {
      // Potentially try inserting multiple times, in case the query wasn't started by the first time
      try {
        client.insertInto(TEST_STREAM, insertRow).get();
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
      return rowRef.get();
    }, is(notNullValue()));

    // Verify received row
    assertThat(row.getKsqlObject("K"), is(new KsqlObject().put("F1", new KsqlArray().add("my_key_shouldExecuteQueryWithProperties"))));
    assertThat(row.getString("STR"), is("Value_shouldExecuteQueryWithProperties"));
    assertThat(row.getLong("LONG"), is(2000L));
    assertThat(row.getDecimal("DEC"), is(new BigDecimal("12.34")));
    assertThat(row.getBytes("BYTES_"), is(new byte[]{0, 1, 2}));
    assertThat(row.getKsqlArray("ARRAY"), is(new KsqlArray().add("v1_shouldExecuteQueryWithProperties").add("v2_shouldExecuteQueryWithProperties")));
    assertThat(row.getKsqlObject("MAP"), is(new KsqlObject().put("test_name", "shouldExecuteQueryWithProperties")));
    assertThat(row.getKsqlObject("STRUCT"), is(new KsqlObject().put("F1", 4)));
    assertThat(row.getKsqlObject("COMPLEX"), is(EXPECTED_COMPLEX_FIELD_VALUE));
    assertThat(row.getString("TIMESTAMP"), is("1970-01-01T00:00:00.001"));
    assertThat(row.getString("DATE"), is("1970-01-01"));
    assertThat(row.getString("TIME"), is("00:00"));
  }

  @Test
  public void shouldInsertInto() throws Exception {
    // Given
    final KsqlObject insertRow = new KsqlObject()
        .put("k", new KsqlObject().put("F1", new KsqlArray().add("my_key"))) // Column names are case-insensitive
        .put("str", "HELLO") // Column names are case-insensitive
        .put("`LONG`", 100L) // Backticks may be used to preserve case-sensitivity
        .put("\"DEC\"", new BigDecimal("13.31")) // Double quotes may also be used to preserve case-sensitivity
        .put("BYTES_", new byte[]{0, 1, 2})
        .put("ARRAY", new KsqlArray().add("v1").add("v2"))
        .put("MAP", new KsqlObject().put("some_key", "a_value").put("another_key", ""))
        .put("STRUCT", new KsqlObject().put("f1", 12)) // Nested field names are case-insensitive
        .put("COMPLEX", COMPLEX_FIELD_VALUE)
        .put("TIMESTAMP", "1970-01-01T00:00:00.001")
        .put("DATE", "1970-01-01")
        .put("TIME", "00:00:01");

    // When
    client.insertInto(EMPTY_TEST_STREAM.toLowerCase(), insertRow).get(); // Stream name is case-insensitive

    // Then: should receive new row
    final String query = "SELECT * FROM " + EMPTY_TEST_STREAM + " EMIT CHANGES LIMIT 1;";
    final List<Row> rows = client.executeQuery(query).get();

    // Verify inserted row is as expected
    assertThat(rows, hasSize(1));
    assertThat(rows.get(0).getKsqlObject("K"), is(new KsqlObject().put("F1", new KsqlArray().add("my_key"))));
    assertThat(rows.get(0).getString("STR"), is("HELLO"));
    assertThat(rows.get(0).getLong("LONG"), is(100L));
    assertThat(rows.get(0).getDecimal("DEC"), is(new BigDecimal("13.31")));
    assertThat(rows.get(0).getBytes("BYTES_"), is(new byte[]{0, 1, 2}));
    assertThat(rows.get(0).getKsqlArray("ARRAY"), is(new KsqlArray().add("v1").add("v2")));
    assertThat(rows.get(0).getKsqlObject("MAP"), is(new KsqlObject().put("some_key", "a_value").put("another_key", "")));
    assertThat(rows.get(0).getKsqlObject("STRUCT"), is(new KsqlObject().put("F1", 12)));
    assertThat(rows.get(0).getKsqlObject("COMPLEX"), is(EXPECTED_COMPLEX_FIELD_VALUE));
    assertThat(rows.get(0).getString("TIMESTAMP"), is("1970-01-01T00:00:00.001"));
    assertThat(rows.get(0).getString("DATE"), is("1970-01-01"));
    assertThat(rows.get(0).getString("TIME"), is("00:00:01"));
  }

  @Test
  public void shouldHandleErrorResponseFromInsertInto() {
    // Given
    final KsqlObject insertRow = new KsqlObject()
        .put("K", new KsqlObject().put("F1", new KsqlArray().add("my_key")))
        .put("LONG", 11L);

    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> client.insertInto(NON_EXIST_TABLE, insertRow).get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString("Received 400 response from server"));
    assertThat(e.getCause().getMessage(), containsString("Cannot insert values into an unknown stream/table"));
  }

  @Test
  public void shouldInsertIntoTable() throws Exception {
    // Given
    final Map<String, Object> properties = new HashMap<>();
    properties.put("auto.offset.reset", "earliest");

    final KsqlObject insertRow = new KsqlObject()
        .put("K", "my_key")
        .put("LONG", 11L);

    // When
    final String query = "SELECT * from " + TEST_TABLE + " WHERE K='my_key' EMIT CHANGES LIMIT 1;";
    StreamedQueryResult queryResult = client.streamQuery(query, properties).get();

    final Row row = assertThatEventually(() -> {
      // Potentially try inserting multiple times, in case the query wasn't started by the first time
      try {
        client.insertInto(TEST_TABLE, insertRow).get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return queryResult.poll(Duration.ofMillis(10));
    }, is(notNullValue()));

    // Then: a newly inserted row arrives
    assertThat(row.getString("K"), is("my_key"));
    assertThat(row.getLong("LONG"), is(11L));
  }

  @Test
  public void shouldStreamInserts() throws Exception {
    // Given
    final InsertsPublisher insertsPublisher = new InsertsPublisher();
    final int numRows = 5;

    // When
    final AcksPublisher acksPublisher = client.streamInserts(EMPTY_TEST_STREAM_2, insertsPublisher).get();

    final TestSubscriber<InsertAck> acksSubscriber = subscribeAndWait(acksPublisher);
    assertThat(acksSubscriber.getValues(), hasSize(0));
    acksSubscriber.getSub().request(numRows);

    for (int i = 0; i < numRows; i++) {
      insertsPublisher.accept(new KsqlObject()
          .put("K", new KsqlObject().put("F1", new KsqlArray().add("my_key_" + i)))
          .put("STR", "TEST_" + i)
          .put("LONG", i)
          .put("DEC", new BigDecimal("13.31"))
          .put("BYTES_", new byte[]{0, 1, 2})
          .put("ARRAY", new KsqlArray().add("v_" + i))
          .put("MAP", new KsqlObject().put("k_" + i, "v_" + i))
          .put("COMPLEX", COMPLEX_FIELD_VALUE)
          .put("TIMESTAMP", "1970-01-01T00:00:00.001")
          .put("DATE", "1970-01-01")
          .put("TIME", "00:00"));
    }

    // Then
    assertThatEventually(acksSubscriber::getValues, hasSize(numRows));
    for (int i = 0; i < numRows; i++) {
      assertThat(acksSubscriber.getValues().get(i).seqNum(), is(Long.valueOf(i)));
    }
    assertThat(acksSubscriber.getError(), is(nullValue()));
    assertThat(acksSubscriber.isCompleted(), is(false));

    assertThat(acksPublisher.isComplete(), is(false));
    assertThat(acksPublisher.isFailed(), is(false));

    // Then: should receive new rows
    final String query = "SELECT * FROM " + EMPTY_TEST_STREAM_2 + " EMIT CHANGES LIMIT " + numRows + ";";
    final List<Row> rows = client.executeQuery(query).get();

    // Verify inserted rows are as expected
    assertThat(rows, hasSize(numRows));
    for (int i = 0; i < numRows; i++) {
      assertThat(rows.get(i).getKsqlObject("K"), is(new KsqlObject().put("F1", new KsqlArray().add("my_key_" + i))));
      assertThat(rows.get(i).getString("STR"), is("TEST_" + i));
      assertThat(rows.get(i).getLong("LONG"), is(Long.valueOf(i)));
      assertThat(rows.get(i).getDecimal("DEC"), is(new BigDecimal("13.31")));
      assertThat(rows.get(i).getBytes("BYTES_"), is(new byte[]{0, 1, 2}));
      assertThat(rows.get(i).getKsqlArray("ARRAY"), is(new KsqlArray().add("v_" + i)));
      assertThat(rows.get(i).getKsqlObject("MAP"), is(new KsqlObject().put("k_" + i, "v_" + i)));
      assertThat(rows.get(i).getKsqlObject("COMPLEX"), is(EXPECTED_COMPLEX_FIELD_VALUE));
      assertThat(rows.get(i).getString("TIMESTAMP"), is("1970-01-01T00:00:00.001"));
      assertThat(rows.get(i).getString("DATE"), is("1970-01-01"));
      assertThat(rows.get(i).getString("TIME"), is("00:00"));
    }

    // When: end connection
    insertsPublisher.complete();

    // Then
    assertThatEventually(acksSubscriber::isCompleted, is(true));
    assertThat(acksSubscriber.getError(), is(nullValue()));

    assertThat(acksPublisher.isComplete(), is(true));
    assertThat(acksPublisher.isFailed(), is(false));
  }

  @Test
  public void shouldHandleErrorResponseFromStreamInserts() {
    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> client.streamInserts(NON_EXIST_TABLE, new InsertsPublisher()).get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString("Received 400 response from server"));
    assertThat(e.getCause().getMessage(),
        containsString("Cannot insert values into an unknown stream/table"));
  }

  @Test
  public void shouldExecuteDdlDmlStatements() throws Exception {
    // Given
    final String streamName = TEST_STREAM + "_COPY";
    final String csas = "create stream " + streamName + " as select * from " + TEST_STREAM + " emit changes;";

    final int numInitialStreams = 3;
    final int numInitialQueries = 1;
    verifyNumStreams(numInitialStreams);
    verifyNumQueries(numInitialQueries);

    // When: create stream, start persistent query
    final ExecuteStatementResult csasResult = client.executeStatement(csas).get();

    // Then
    verifyNumStreams(numInitialStreams + 1);
    verifyNumQueries(numInitialQueries + 1);
    assertThat(csasResult.queryId(), is(Optional.of(findQueryIdForSink(streamName))));

    // When: terminate persistent query
    final String queryId = csasResult.queryId().get();
    final ExecuteStatementResult terminateResult =
        client.executeStatement("terminate " + queryId + ";").get();

    // Then
    verifyNumQueries(numInitialQueries);
    assertThat(terminateResult.queryId(), is(Optional.empty()));

    // When: drop stream
    final ExecuteStatementResult dropStreamResult =
        client.executeStatement("drop stream " + streamName + ";").get();

    // Then
    verifyNumStreams(numInitialStreams);
    assertThat(dropStreamResult.queryId(), is(Optional.empty()));
  }

  private void verifyNumStreams(final int numStreams) {
    assertThatEventually(() -> {
      try {
        return client.listStreams().get().size();
      } catch (final Exception e) {
        return -1;
      }
    }, is(numStreams));
  }

  private void verifyNumQueries(final int numQueries) {
    assertThatEventually(() -> {
      try {
        return client.listQueries().get().size();
      } catch (final Exception e) {
        return -1;
      }
    }, is(numQueries));
  }

  private String findQueryIdForSink(final String sinkName) throws Exception {
    final List<String> queryIds = client.listQueries().get().stream()
        .filter(q -> q.getSink().equals(Optional.of(sinkName)))
        .map(QueryInfo::getId)
        .collect(Collectors.toList());
    assertThat(queryIds, hasSize(1));
    return queryIds.get(0);
  }

  private Client createClient() {
    final ClientOptions clientOptions = ClientOptions.create()
        .setHost("localhost")
        .setPort(REST_APP.getListeners().get(0).getPort());
    return Client.create(clientOptions, vertx);
  }

  private static void cleanupConnectors() {
    ((ConnectorList) makeKsqlRequest("SHOW CONNECTORS;").get(0)).getConnectors()
        .forEach(c -> makeKsqlRequest("DROP CONNECTOR " + c.getName() + ";"));
    assertThatEventually(
        () -> ((ConnectorList) makeKsqlRequest("SHOW CONNECTORS;")
            .get(0)).getConnectors().size(),
        is(0)
    );
  }

  private static List<KsqlEntity> makeKsqlRequest(final String sql) {
    return RestIntegrationTestUtil.makeKsqlRequest(REST_APP, sql);
  }

}