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

package io.confluent.ksql.materialization.ks;

import static io.confluent.ksql.serde.Format.JSON;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.Range;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.streams.materialization.Materialization;
import io.confluent.ksql.execution.streams.materialization.MaterializedTable;
import io.confluent.ksql.execution.streams.materialization.MaterializedWindowedTable;
import io.confluent.ksql.execution.streams.materialization.Row;
import io.confluent.ksql.execution.streams.materialization.Window;
import io.confluent.ksql.execution.streams.materialization.WindowedRow;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.integration.TestKsqlContext;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.test.util.KsqlIdentifierTestUtil;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.UserDataProvider;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;

@SuppressWarnings("OptionalGetWithoutIsPresent")
@Category({IntegrationTest.class})
public class KsMaterializationFunctionalTest {

  private static final String USERS_TOPIC = "users_topic";
  private static final String USER_TABLE = "users_table";
  private static final String USER_STREAM = "users_stream";

  private static final Format VALUE_FORMAT = JSON;
  private static final UserDataProvider USER_DATA_PROVIDER = new UserDataProvider();

  private static final Duration WINDOW_SIZE = Duration.ofSeconds(5);

  private static final Deserializer<String> STRING_DESERIALIZER = new StringDeserializer();

  private static final Deserializer<Windowed<String>> TIME_WINDOWED_DESERIALIZER =
      WindowedSerdes
          .timeWindowedSerdeFrom(String.class, WINDOW_SIZE.toMillis())
          .deserializer();

  private static final Deserializer<Windowed<String>> SESSION_WINDOWED_DESERIALIZER =
      WindowedSerdes
          .sessionWindowedSerdeFrom(String.class)
          .deserializer();

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  @ClassRule
  public static final RuleChain CLUSTER_WITH_RETRY = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS);

  @Rule
  public final TestKsqlContext ksqlContext = TEST_HARNESS.ksqlContextBuilder()
      .withAdditionalConfig(StreamsConfig.APPLICATION_SERVER_CONFIG, "https://localhost:34")
      .build();

  @Rule
  public final Timeout timeout = Timeout.seconds(120);

  private final List<QueryMetadata> toClose = new ArrayList<>();

  private String output;
  private final QueryId queryId = new QueryId("static");

  @BeforeClass
  public static void classSetUp() {
    TEST_HARNESS.ensureTopics(USERS_TOPIC);

    TEST_HARNESS.produceRows(
        USERS_TOPIC,
        USER_DATA_PROVIDER,
        VALUE_FORMAT
    );
  }

  @Before
  public void setUp() {
    output = KsqlIdentifierTestUtil.uniqueIdentifierName();

    toClose.clear();

    initializeKsql(ksqlContext);
  }

  @After
  public void after() {
    toClose.forEach(QueryMetadata::close);
  }

  @Test
  public void shouldReturnEmptyIfNotMaterializedTable() {
    // Given:
    PersistentQueryMetadata query = executeQuery(
        "CREATE TABLE " + output + " AS"
            + " SELECT * FROM " + USER_TABLE + ";"
    );

    // When:
    final Optional<Materialization> result = query.getMaterialization();

    // Then:
    assertThat(result, is(Optional.empty()));
  }

  @Test
  public void shouldReturnEmptyIfNotMaterializedStream() {
    // Given:
    PersistentQueryMetadata query = executeQuery(
        "CREATE STREAM " + output + " AS"
            + " SELECT * FROM " + USER_STREAM + ";"
    );

    // When:
    final Optional<Materialization> result = query.getMaterialization();

    // Then:
    assertThat(result, is(Optional.empty()));
  }

  @Test
  public void shouldReturnEmptyIfAppServerNotConfigured() {
    // Given:
    try (TestKsqlContext ksqlNoAppServer = TEST_HARNESS.ksqlContextBuilder().build()) {
      initializeKsql(ksqlNoAppServer);

      PersistentQueryMetadata query = executeQuery(
          ksqlNoAppServer,
          "CREATE TABLE " + output + " AS"
              + " SELECT COUNT(*) AS COUNT FROM " + USER_TABLE
              + " GROUP BY USERID;"
      );

      // When:
      final Optional<Materialization> result = query.getMaterialization();

      // Then:
      assertThat(result, is(Optional.empty()));
    }
  }

  @Test
  public void shouldQueryMaterializedTableForAggregatedTable() {
    // Given:
    PersistentQueryMetadata query = executeQuery(
        "CREATE TABLE " + output + " AS"
            + " SELECT COUNT(*) FROM " + USER_TABLE
            + " GROUP BY USERID;"
    );

    final LogicalSchema schema = schema("KSQL_COL_0", SqlTypes.BIGINT);

    final Map<String, GenericRow> rows = waitForTableRows(STRING_DESERIALIZER, schema);

    // When:
    final Materialization materialization = query.getMaterialization().get();

    // Then:
    assertThat(materialization.windowType(), is(Optional.empty()));

    final MaterializedTable table = materialization.nonWindowed();

    rows.forEach((rowKey, value) -> {
      final Struct key = asKeyStruct(rowKey, query.getPhysicalSchema());
      assertThat(
          "expected key",
          table.get(key),
          is(Optional.of(Row.of(schema.withoutMetaColumns(), key, value)))
      );
    });

    final Struct key = asKeyStruct("Won't find me", query.getPhysicalSchema());
    assertThat("unknown key", table.get(key), is(Optional.empty()));
  }

  @Test
  public void shouldQueryMaterializedTableForAggregatedStream() {
    // Given:
    PersistentQueryMetadata query = executeQuery(
        "CREATE TABLE " + output + " AS"
            + " SELECT COUNT(*) AS COUNT FROM " + USER_STREAM
            + " GROUP BY USERID;"
    );

    final LogicalSchema schema = schema("COUNT", SqlTypes.BIGINT);

    final Map<String, GenericRow> rows = waitForTableRows(STRING_DESERIALIZER, schema);

    // When:
    final Materialization materialization = query.getMaterialization().get();

    // Then:
    assertThat(materialization.windowType(), is(Optional.empty()));

    final MaterializedTable table = materialization.nonWindowed();

    rows.forEach((rowKey, value) -> {
      final Struct key = asKeyStruct(rowKey, query.getPhysicalSchema());
      assertThat(
          "expected key",
          table.get(key),
          is(Optional.of(Row.of(schema.withoutMetaColumns(), key, value)))
      );
    });

    final Struct key = asKeyStruct("Won't find me", query.getPhysicalSchema());
    assertThat("unknown key", table.get(key), is(Optional.empty()));
  }

  @Test
  public void shouldQueryMaterializedTableForTumblingWindowed() {
    // Given:
    PersistentQueryMetadata query = executeQuery(
        "CREATE TABLE " + output + " AS"
            + " SELECT COUNT(*) AS COUNT FROM " + USER_STREAM
            + " WINDOW TUMBLING (SIZE " + WINDOW_SIZE.getSeconds() + " SECONDS)"
            + " GROUP BY USERID;"
    );

    final LogicalSchema schema = schema("COUNT", SqlTypes.BIGINT);

    final Map<Windowed<String>, GenericRow> rows =
        waitForTableRows(TIME_WINDOWED_DESERIALIZER, schema);

    // When:
    final Materialization materialization = query.getMaterialization().get();

    // Then:
    assertThat(materialization.windowType(), is(Optional.of(WindowType.TUMBLING)));

    final MaterializedWindowedTable table = materialization.windowed();

    rows.forEach((k, v) -> {
      final Window w = Window.of(k.window().startTime(), Optional.empty());
      final Struct key = asKeyStruct(k.key(), query.getPhysicalSchema());

      assertThat(
          "at exact window start",
          table.get(key, Range.singleton(w.start())),
          contains(WindowedRow.of(schema.withoutMetaColumns(), key, w, v))
      );

      assertThat(
          "range including window start",
          table.get(key, Range.closed(w.start().minusMillis(1), w.start().plusMillis(1))),
          contains(WindowedRow.of(schema.withoutMetaColumns(), key, w, v))
      );

      assertThat(
          "past start",
          table.get(key, Range.closed(w.start().plusMillis(1), w.start().plusMillis(1))),
          is(empty())
      );
    });
  }

  @Test
  public void shouldQueryMaterializedTableForHoppingWindowed() {
    // Given:
    PersistentQueryMetadata query = executeQuery(
        "CREATE TABLE " + output + " AS"
            + " SELECT COUNT(*) AS COUNT FROM " + USER_STREAM
            + " WINDOW HOPPING (SIZE " + WINDOW_SIZE.getSeconds() + " SECONDS,"
            + " ADVANCE BY " + WINDOW_SIZE.getSeconds() + " SECONDS)"
            + " GROUP BY USERID;"
    );

    final LogicalSchema schema = schema("COUNT", SqlTypes.BIGINT);

    final Map<Windowed<String>, GenericRow> rows =
        waitForTableRows(TIME_WINDOWED_DESERIALIZER, schema);

    // When:
    final Materialization materialization = query.getMaterialization().get();

    // Then:
    assertThat(materialization.windowType(), is(Optional.of(WindowType.HOPPING)));

    final MaterializedWindowedTable table = materialization.windowed();

    rows.forEach((k, v) -> {
      final Window w = Window.of(k.window().startTime(), Optional.empty());
      final Struct key = asKeyStruct(k.key(), query.getPhysicalSchema());

      assertThat(
          "at exact window start",
          table.get(key, Range.singleton(w.start())),
          contains(WindowedRow.of(schema.withoutMetaColumns(), key, w, v))
      );

      assertThat(
          "range including window start",
          table.get(key, Range.closed(w.start().minusMillis(1), w.start().plusMillis(1))),
          contains(WindowedRow.of(schema.withoutMetaColumns(), key, w, v))
      );

      assertThat(
          "past start",
          table.get(key, Range.closed(w.start().plusMillis(1), w.start().plusMillis(1))),
          is(empty())
      );
    });
  }

  @Test
  public void shouldQueryMaterializedTableForSessionWindowed() {
    // Given:
    PersistentQueryMetadata query = executeQuery(
        "CREATE TABLE " + output + " AS"
            + " SELECT COUNT(*) AS COUNT FROM " + USER_STREAM
            + " WINDOW SESSION (" + WINDOW_SIZE.getSeconds() + " SECONDS)"
            + " GROUP BY USERID;"
    );

    final LogicalSchema schema = schema("COUNT", SqlTypes.BIGINT);

    final Map<Windowed<String>, GenericRow> rows =
        waitForTableRows(SESSION_WINDOWED_DESERIALIZER, schema);

    // When:
    final Materialization materialization = query.getMaterialization().get();

    // Then:
    assertThat(materialization.windowType(), is(Optional.of(WindowType.SESSION)));

    final MaterializedWindowedTable table = materialization.windowed();

    rows.forEach((k, v) -> {
      final Window w = Window.of(k.window().startTime(), Optional.of(k.window().endTime()));
      final Struct key = asKeyStruct(k.key(), query.getPhysicalSchema());

      assertThat(
          "at exact window start",
          table.get(key, Range.singleton(w.start())),
          contains(WindowedRow.of(schema.withoutMetaColumns(), key, w, v))
      );

      assertThat(
          "range including window start",
          table.get(key, Range.closed(w.start().minusMillis(1), w.start().plusMillis(1))),
          contains(WindowedRow.of(schema.withoutMetaColumns(), key, w, v))
      );

      assertThat(
          "past start",
          table.get(key, Range.closed(w.start().plusMillis(1), w.start().plusMillis(1))),
          is(empty())
      );
    });
  }

  @Test
  public void shouldQueryMaterializedTableWithKeyFieldsInProjection() {
    // Given:
    PersistentQueryMetadata query = executeQuery(
        "CREATE TABLE " + output + " AS"
            + " SELECT USERID, COUNT(*), USERID AS USERID_2 FROM " + USER_TABLE
            + " GROUP BY USERID;"
    );

    final LogicalSchema schema = schema(
        "USERID", SqlTypes.STRING,
        "KSQL_COL_1", SqlTypes.BIGINT,
        "USERID_2", SqlTypes.STRING
    );

    final Map<String, GenericRow> rows = waitForTableRows(STRING_DESERIALIZER, schema);


    // When:
    final Materialization materialization = query.getMaterialization().get();

    // Then:
    assertThat(materialization.windowType(), is(Optional.empty()));

    final MaterializedTable table = materialization.nonWindowed();

    rows.forEach((rowKey, value) -> {
      final Struct key = asKeyStruct(rowKey, query.getPhysicalSchema());
      assertThat(table.get(key), is(Optional.of(Row.of(schema.withoutMetaColumns(), key, value))));
    });
  }

  @Test
  public void shouldQueryMaterializedTableWitMultipleAggregationColumns() {
    // Given:
    PersistentQueryMetadata query = executeQuery(
        "CREATE TABLE " + output + " AS"
            + " SELECT COUNT(1) AS COUNT, SUM(REGISTERTIME) AS SUM FROM " + USER_TABLE
            + " GROUP BY USERID;"
    );

    final LogicalSchema schema = schema(
        "COUNT", SqlTypes.BIGINT,
        "SUM", SqlTypes.BIGINT
    );

    final Map<String, GenericRow> rows = waitForTableRows(STRING_DESERIALIZER, schema);

    // When:
    final Materialization materialization = query.getMaterialization().get();

    // Then:
    assertThat(materialization.windowType(), is(Optional.empty()));

    final MaterializedTable table = materialization.nonWindowed();

    rows.forEach((rowKey, value) -> {
      final Struct key = asKeyStruct(rowKey, query.getPhysicalSchema());
      assertThat(table.get(key), is(Optional.of(Row.of(schema.withoutMetaColumns(), key, value))));
    });
  }

  @Test
  public void shouldIgnoreHavingClause() {
    // Note: HAVING clause are handled centrally by KsqlMaterialization

    // Given:
    PersistentQueryMetadata query = executeQuery(
        "CREATE TABLE " + output + " AS"
            + " SELECT COUNT(*) AS COUNT FROM " + USER_TABLE
            + " GROUP BY USERID"
            + " HAVING SUM(REGISTERTIME) > 2;"
    );

    final LogicalSchema schema = schema("COUNT", SqlTypes.BIGINT);

    final Map<String, GenericRow> rows = waitForTableRows(STRING_DESERIALIZER, schema);

    // When:
    final Materialization materialization = query.getMaterialization().get();

    // Then:
    final MaterializedTable table = materialization.nonWindowed();

    rows.forEach((rowKey, value) -> {
      final Struct key = asKeyStruct(rowKey, query.getPhysicalSchema());

      final Optional<Row> row = Optional.ofNullable(value)
          .map(v -> Row.of(schema.withoutMetaColumns(), key, v));

      assertThat(table.get(key), is(row));
    });
  }

  private <T> Map<T, GenericRow> waitForTableRows(
      final Deserializer<T> keyDeserializer,
      final LogicalSchema aggregateSchema
  ) {
    return TEST_HARNESS.verifyAvailableUniqueRows(
        output.toUpperCase(),
        USER_DATA_PROVIDER.data().size(),
        VALUE_FORMAT,
        PhysicalSchema.from(aggregateSchema, SerdeOption.none()),
        keyDeserializer
    );
  }

  private PersistentQueryMetadata executeQuery(final String statement) {
    return executeQuery(ksqlContext, statement);
  }

  private PersistentQueryMetadata executeQuery(
      final TestKsqlContext ksqlContext,
      final String statement
  ) {
    final List<QueryMetadata> queries = ksqlContext.sql(statement);

    assertThat(queries, hasSize(1));
    assertThat(queries.get(0), instanceOf(PersistentQueryMetadata.class));

    final PersistentQueryMetadata query = (PersistentQueryMetadata) queries.get(0);

    toClose.add(query);

    return query;
  }

  private static Struct asKeyStruct(final String rowKey, final PhysicalSchema physicalSchema) {
    final Struct key = new Struct(physicalSchema.keySchema().ksqlSchema());
    key.put(SchemaUtil.ROWKEY_NAME.name(), rowKey);
    return key;
  }

  @SuppressWarnings("SameParameterValue")
  private static LogicalSchema schema(
      final String columnName0,
      final SqlType columnType0
  ) {
    return LogicalSchema.builder()
        .valueColumn(ColumnName.of(columnName0), columnType0)
        .build();
  }

  @SuppressWarnings("SameParameterValue")
  private static LogicalSchema schema(
      final String columnName0, final SqlType columnType0,
      final String columnName1, final SqlType columnType1
  ) {
    return LogicalSchema.builder()
        .valueColumn(ColumnName.of(columnName0), columnType0)
        .valueColumn(ColumnName.of(columnName1), columnType1)
        .build();
  }

  @SuppressWarnings("SameParameterValue")
  private static LogicalSchema schema(
      final String columnName0, final SqlType columnType0,
      final String columnName1, final SqlType columnType1,
      final String columnName2, final SqlType columnType2
  ) {
    return LogicalSchema.builder()
        .valueColumn(ColumnName.of(columnName0), columnType0)
        .valueColumn(ColumnName.of(columnName1), columnType1)
        .valueColumn(ColumnName.of(columnName2), columnType2)
        .build();
  }

  private static void initializeKsql(final TestKsqlContext ksqlContext) {
    ksqlContext.ensureStarted();

    ksqlContext.sql("CREATE TABLE " + USER_TABLE + " "
        + USER_DATA_PROVIDER.ksqlSchemaString()
        + " WITH ("
        + "    kafka_topic='" + USERS_TOPIC + "', "
        + "    value_format='" + VALUE_FORMAT + "', "
        + "    key = '" + USER_DATA_PROVIDER.key() + "'"
        + ");"
    );

    ksqlContext.sql("CREATE STREAM " + USER_STREAM + " "
        + USER_DATA_PROVIDER.ksqlSchemaString()
        + " WITH ("
        + "    kafka_topic='" + USERS_TOPIC + "', "
        + "    value_format='" + VALUE_FORMAT + "', "
        + "    key = '" + USER_DATA_PROVIDER.key() + "'"
        + ");"
    );
  }
}

