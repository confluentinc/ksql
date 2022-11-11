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

import static io.confluent.ksql.GenericKey.genericKey;
import static io.confluent.ksql.serde.FormatFactory.JSON;
import static io.confluent.ksql.serde.FormatFactory.KAFKA;
import static io.confluent.ksql.test.util.AssertEventually.RetryOnException;
import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import com.google.common.collect.Range;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.Window;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.streams.materialization.Materialization;
import io.confluent.ksql.execution.streams.materialization.MaterializedTable;
import io.confluent.ksql.execution.streams.materialization.MaterializedWindowedTable;
import io.confluent.ksql.execution.streams.materialization.Row;
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
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.test.util.KsqlIdentifierTestUtil;
import io.confluent.ksql.util.PageViewDataProvider;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.UserDataProvider;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.commons.compress.utils.Lists;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
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

  private static final String PAGE_VIEWS_TOPIC = "page_views_topic";
  private static final String PAGE_VIEWS_STREAM = "page_views_stream";

  private static final Format KEY_FORMAT = KAFKA;
  private static final Format VALUE_FORMAT = JSON;
  private static final UserDataProvider USER_DATA_PROVIDER = new UserDataProvider();
  private static final PageViewDataProvider PAGE_VIEW_DATA_PROVIDER = new PageViewDataProvider();
  private static final int PARTITION = 0;

  private static final Duration WINDOW_SIZE = Duration.ofSeconds(5);
  private static final Duration WINDOW_SEGMENT_DURATION = Duration.ofSeconds(60);
  private static final int NUM_WINDOWS = 4;
  private static final List<Instant> WINDOW_START_INSTANTS = LongStream.range(1, NUM_WINDOWS + 1)
      // records have to be apart by at-least a segment for retention to enforced
      .mapToObj(i -> Instant.ofEpochMilli(i * WINDOW_SEGMENT_DURATION.toMillis()))
      .collect(Collectors.toList());

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
  private final QueryContext.Stacker contextStacker = new QueryContext.Stacker();

  @BeforeClass
  public static void classSetUp() {
    TEST_HARNESS.ensureTopics(USERS_TOPIC, PAGE_VIEWS_TOPIC);

    TEST_HARNESS.produceRows(
        USERS_TOPIC,
        USER_DATA_PROVIDER,
        KEY_FORMAT,
        VALUE_FORMAT
    );

    for (final Instant windowTime : WINDOW_START_INSTANTS) {
      TEST_HARNESS.produceRows(
          PAGE_VIEWS_TOPIC,
          PAGE_VIEW_DATA_PROVIDER,
          KEY_FORMAT,
          VALUE_FORMAT,
          windowTime::toEpochMilli
      );
    }
  }

  @Before
  public void setUp() {
    output = KsqlIdentifierTestUtil.uniqueIdentifierName();

    toClose.clear();

    initializeKsql(ksqlContext);
  }

  @After
  public void after() {
    try {
      toClose.forEach(q -> q.close());
    } catch (Exception e) {

    }
  }

  @Test
  public void shouldReturnEmptyIfNotMaterializedStream() {
    // Given:
    final PersistentQueryMetadata query = executeQuery(
        "CREATE STREAM " + output + " AS"
            + " SELECT * FROM " + USER_STREAM + ";"
    );

    // When:
    final Optional<Materialization> result = query.getMaterialization(queryId, contextStacker);

    // Then:
    assertThat(result, is(Optional.empty()));
  }

  @Test
  public void shouldReturnEmptyIfAppServerNotConfigured() {
    // Given:
    try (TestKsqlContext ksqlNoAppServer = TEST_HARNESS.ksqlContextBuilder().build()) {
      initializeKsql(ksqlNoAppServer);

      final PersistentQueryMetadata query = executeQuery(
          ksqlNoAppServer,
          "CREATE TABLE " + output + " AS"
              + " SELECT USERID, COUNT(*) AS COUNT FROM " + USER_TABLE
              + " GROUP BY USERID;"
      );

      // When:
      final Optional<Materialization> result = query.getMaterialization(queryId, contextStacker);

      // Then:
      assertThat(result, is(Optional.empty()));
    }
  }

  @Test
  public void shouldQueryMaterializedTableForAggregatedTable() {
    // Given:
    final PersistentQueryMetadata query = executeQuery(
        "CREATE TABLE " + output + " AS"
            + " SELECT USERID, COUNT(*) FROM " + USER_TABLE
            + " GROUP BY USERID;"
    );

    final LogicalSchema schema = schema("KSQL_COL_0", SqlTypes.BIGINT);

    final Map<String, GenericRow> rows = waitForUniqueUserRows(STRING_DESERIALIZER, schema);

    // When:
    final Materialization materialization = query.getMaterialization(queryId, contextStacker).get();

    // Then:
    assertThat(materialization.windowType(), is(Optional.empty()));

    final MaterializedTable table = materialization.nonWindowed();

    rows.forEach((rowKey, value) -> {
      final GenericKey key = genericKey(rowKey);

      final Iterator<Row> rowIterator =
          withRetry(() -> table.get(key, PARTITION));

      
      assertThat(rowIterator.hasNext(), is(true));
      final Row row = rowIterator.next();
      assertThat(row.schema(), is(schema));
      assertThat(row.key(), is(key));
      assertThat(row.value(), is(value));
    });

    final GenericKey key = genericKey("Won't find me");
    assertThat("unknown key", withRetry(() -> table.get(key, PARTITION).hasNext()),
               is(false));
  }

  @Test
  public void shouldQueryMaterializedTableForAggregatedStream() {
    // Given:
    final PersistentQueryMetadata query = executeQuery(
        "CREATE TABLE " + output + " AS"
            + " SELECT USERID, COUNT(*) AS COUNT FROM " + USER_STREAM
            + " GROUP BY USERID;"
    );

    final LogicalSchema schema = schema("COUNT", SqlTypes.BIGINT);

    final Map<String, GenericRow> rows = waitForUniqueUserRows(STRING_DESERIALIZER, schema);

    // When:
    final Materialization materialization = query.getMaterialization(queryId, contextStacker).get();

    // Then:
    assertThat(materialization.windowType(), is(Optional.empty()));

    final MaterializedTable table = materialization.nonWindowed();

    rows.forEach((rowKey, value) -> {
      final GenericKey key = genericKey(rowKey);
      final Iterator<Row> rowIterator =
          withRetry(() -> table.get(key, PARTITION));

      
      assertThat(rowIterator.hasNext(), is(true));
      final Row row = rowIterator.next();
      assertThat(row.schema(), is(schema));
      assertThat(row.key(), is(key));
      assertThat(row.value(), is(value));

    });

    final GenericKey key = genericKey("Won't find me");
    assertThat("unknown key", withRetry(() -> table.get(key, PARTITION)).hasNext(),
               is(false));
  }

  @Test
  public void shouldQueryMaterializedTableForTumblingWindowed() {
    // Given:
    final PersistentQueryMetadata query = executeQuery(
        "CREATE TABLE " + output + " AS"
            + " SELECT USERID, COUNT(*) AS COUNT FROM " + USER_STREAM
            + " WINDOW TUMBLING (SIZE " + WINDOW_SIZE.getSeconds() + " SECONDS)"
            + " GROUP BY USERID;"
    );

    final LogicalSchema schema = schema("COUNT", SqlTypes.BIGINT);

    final Map<Windowed<String>, GenericRow> rows =
        waitForUniqueUserRows(TIME_WINDOWED_DESERIALIZER, schema);

    // When:
    final Materialization materialization = query.getMaterialization(queryId, contextStacker).get();

    // Then:
    assertThat(materialization.windowType(), is(Optional.of(WindowType.TUMBLING)));

    final MaterializedWindowedTable table = materialization.windowed();

    rows.forEach((k, v) -> {
      final Window w = Window.of(k.window().startTime(), k.window().endTime());
      final GenericKey key = genericKey(k.key());

      final List<WindowedRow> resultAtWindowStart =
          withRetry(() -> Lists.newArrayList(
              table.get(key, PARTITION, Range.singleton(w.start()), Range.all())));

      assertThat("at exact window start", resultAtWindowStart, hasSize(1));
      assertThat(resultAtWindowStart.get(0).schema(), is(schema));
      assertThat(resultAtWindowStart.get(0).window(), is(Optional.of(w)));
      assertThat(resultAtWindowStart.get(0).key(), is(key));
      assertThat(resultAtWindowStart.get(0).value(), is(v));

      final List<WindowedRow> resultAtWindowEnd =
          withRetry(() -> Lists.newArrayList(
              table.get(key, PARTITION, Range.all(), Range.singleton(w.end()))));
      assertThat("at exact window end", resultAtWindowEnd, hasSize(1));

      final List<WindowedRow> resultFromRange = withRetry(() -> withRetry(() -> Lists.newArrayList(table
          .get(key, PARTITION, Range.closed(w.start().minusMillis(1), w.start().plusMillis(1)),
              Range.all()))));

      assertThat("range including window start", resultFromRange, is(resultAtWindowStart));

      final List<WindowedRow> resultPast = withRetry(() -> Lists.newArrayList(table
          .get(key, PARTITION, Range.closed(w.start().plusMillis(1), w.start().plusMillis(1)),
              Range.all())));
      assertThat("past start", resultPast, is(empty())
      );
    });
  }

  @Test
  public void shouldQueryMaterializedTableForHoppingWindowed() {
    // Given:
    final PersistentQueryMetadata query = executeQuery(
        "CREATE TABLE " + output + " AS"
            + " SELECT USERID, COUNT(*) AS COUNT FROM " + USER_STREAM
            + " WINDOW HOPPING (SIZE " + WINDOW_SIZE.getSeconds() + " SECONDS,"
            + " ADVANCE BY " + WINDOW_SIZE.getSeconds() + " SECONDS)"
            + " GROUP BY USERID;"
    );

    final LogicalSchema schema = schema("COUNT", SqlTypes.BIGINT);

    final Map<Windowed<String>, GenericRow> rows =
        waitForUniqueUserRows(TIME_WINDOWED_DESERIALIZER, schema);

    // When:
    final Materialization materialization = query.getMaterialization(queryId, contextStacker).get();

    // Then:
    assertThat(materialization.windowType(), is(Optional.of(WindowType.HOPPING)));

    final MaterializedWindowedTable table = materialization.windowed();

    rows.forEach((k, v) -> {
      final Window w = Window.of(k.window().startTime(), k.window().endTime());
      final GenericKey key = genericKey(k.key());

      final List<WindowedRow> resultAtWindowStart =
          withRetry(() -> Lists.newArrayList(
              table.get(key, PARTITION, Range.singleton(w.start()), Range.all())));

      assertThat("at exact window start", resultAtWindowStart, hasSize(1));
      assertThat(resultAtWindowStart.get(0).schema(), is(schema));
      assertThat(resultAtWindowStart.get(0).window(), is(Optional.of(w)));
      assertThat(resultAtWindowStart.get(0).key(), is(key));
      assertThat(resultAtWindowStart.get(0).value(), is(v));

      final List<WindowedRow> resultAtWindowEnd =
          withRetry(() -> Lists.newArrayList(
              table.get(key, PARTITION, Range.all(), Range.singleton(w.end()))));
      assertThat("at exact window end", resultAtWindowEnd, hasSize(1));

      final List<WindowedRow> resultFromRange = withRetry(() -> Lists.newArrayList(
          table.get(key, PARTITION, Range.closed(w.start().minusMillis(1), w.start().plusMillis(1)),
              Range.all())));

      assertThat("range including window start", resultFromRange, is(resultAtWindowStart));

      final List<WindowedRow> resultPast = withRetry(() -> Lists.newArrayList(table
          .get(key, PARTITION, Range.closed(w.start().plusMillis(1), w.start().plusMillis(1)),
              Range.all())));

      assertThat("past start", resultPast, is(empty()));
    });
  }

  @Test
  public void shouldQueryMaterializedTableForSessionWindowed() {
    // Given:
    final PersistentQueryMetadata query = executeQuery(
        "CREATE TABLE " + output + " AS"
            + " SELECT USERID, COUNT(*) AS COUNT FROM " + USER_STREAM
            + " WINDOW SESSION (" + WINDOW_SIZE.getSeconds() + " SECONDS)"
            + " GROUP BY USERID;"
    );

    final LogicalSchema schema = schema("COUNT", SqlTypes.BIGINT);

    final Map<Windowed<String>, GenericRow> rows =
        waitForUniqueUserRows(SESSION_WINDOWED_DESERIALIZER, schema);

    // When:
    final Materialization materialization = query.getMaterialization(queryId, contextStacker).get();

    // Then:
    assertThat(materialization.windowType(), is(Optional.of(WindowType.SESSION)));

    final MaterializedWindowedTable table = materialization.windowed();

    rows.forEach((k, v) -> {
      final Window w = Window.of(k.window().startTime(), k.window().endTime());
      final GenericKey key = genericKey(k.key());

      final List<WindowedRow> resultAtWindowStart =
          withRetry(() -> Lists.newArrayList(table.get(key, PARTITION, Range.singleton(w.start())
              , Range.all())));

      assertThat("at exact window start", resultAtWindowStart, hasSize(1));
      assertThat(resultAtWindowStart.get(0).schema(), is(schema));
      assertThat(resultAtWindowStart.get(0).window(), is(Optional.of(w)));
      assertThat(resultAtWindowStart.get(0).key(), is(key));
      assertThat(resultAtWindowStart.get(0).value(), is(v));

      final List<WindowedRow> resultAtWindowEnd =
          withRetry(() -> Lists.newArrayList(table.get(
              key, PARTITION, Range.all(), Range.singleton(w.end()))));
      assertThat("at exact window end", resultAtWindowEnd, hasSize(1));

      final List<WindowedRow> resultFromRange = withRetry(() -> Lists.newArrayList(table
          .get(key, PARTITION, Range.closed(w.start().minusMillis(1), w.start().plusMillis(1)),
              Range.all())));
      assertThat("range including window start", resultFromRange, is(resultAtWindowStart));

      final List<WindowedRow> resultPast = withRetry(() -> Lists.newArrayList(table
          .get(key, PARTITION, Range.closed(w.start().plusMillis(1), w.start().plusMillis(1)),
              Range.all())));
      assertThat("past start", resultPast, is(empty()));
    });
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailQueryWithRetentionSmallerThanGracePeriod() {
    // Given:
    executeQuery("CREATE TABLE " + output + " AS"
            + " SELECT PAGEID, COUNT(*) AS COUNT FROM " + PAGE_VIEWS_STREAM
            + " WINDOW TUMBLING (SIZE " + WINDOW_SEGMENT_DURATION.getSeconds() + " SECONDS,"
            + " RETENTION " + (WINDOW_SEGMENT_DURATION.getSeconds() * 2) + " SECONDS)"
            + " GROUP BY PAGEID;"
    );
  }

  @Test
  public void shouldQueryTumblingWindowMaterializedTableWithRetention() {
    // Given:
    final PersistentQueryMetadata query = executeQuery(
        "CREATE TABLE " + output + " AS"
            + " SELECT PAGEID, COUNT(*) AS COUNT FROM " + PAGE_VIEWS_STREAM
            + " WINDOW TUMBLING (SIZE " + WINDOW_SEGMENT_DURATION.getSeconds() + " SECONDS,"
            + " RETENTION " + (WINDOW_SEGMENT_DURATION.getSeconds() * 2) + " SECONDS,"
            + " GRACE PERIOD 0 SECONDS)"
            + " GROUP BY PAGEID;"
    );

    final List<ConsumerRecord<Windowed<String>, GenericRow>> rows =
        waitForPageViewRows(TIME_WINDOWED_DESERIALIZER, query.getPhysicalSchema());

    // When:
    final Materialization materialization = query.getMaterialization(queryId, contextStacker).get();

    // Then:
    assertThat(materialization.windowType(), is(Optional.of(WindowType.TUMBLING)));
    final MaterializedWindowedTable table = materialization.windowed();
    final Set<Optional<Window>> expectedWindows = Stream.of(
        Window.of(WINDOW_START_INSTANTS.get(2), WINDOW_START_INSTANTS.get(2).plusSeconds(WINDOW_SEGMENT_DURATION.getSeconds())),
        Window.of(WINDOW_START_INSTANTS.get(3), WINDOW_START_INSTANTS.get(3).plusSeconds(WINDOW_SEGMENT_DURATION.getSeconds()))
    ).map(Optional::of).collect(Collectors.toSet());
    verifyRetainedWindows(rows, table, expectedWindows);
  }

  @Test
  public void shouldQueryHoppingWindowMaterializedTableWithRetention() {
    // Given:
    final PersistentQueryMetadata query = executeQuery(
        "CREATE TABLE " + output + " AS"
            + " SELECT PAGEID, COUNT(*) AS COUNT FROM " + PAGE_VIEWS_STREAM
            + " WINDOW HOPPING (SIZE " + WINDOW_SEGMENT_DURATION.getSeconds() + " SECONDS,"
            + " ADVANCE BY " + WINDOW_SEGMENT_DURATION.getSeconds() + " SECONDS, "
            + " RETENTION " + WINDOW_SEGMENT_DURATION.getSeconds() + " SECONDS,"
            + " GRACE PERIOD 0 SECONDS"
            + ") GROUP BY PAGEID;"
    );

    final List<ConsumerRecord<Windowed<String>, GenericRow>> rows =
        waitForPageViewRows(TIME_WINDOWED_DESERIALIZER, query.getPhysicalSchema());

    // When:
    final Materialization materialization = query.getMaterialization(queryId, contextStacker).get();

    // Then:
    assertThat(materialization.windowType(), is(Optional.of(WindowType.HOPPING)));
    final MaterializedWindowedTable table = materialization.windowed();
    final Set<Optional<Window>> expectedWindows = Stream.of(
        Window.of(WINDOW_START_INSTANTS.get(3), WINDOW_START_INSTANTS.get(3).plusSeconds(WINDOW_SEGMENT_DURATION.getSeconds()))
    ).map(Optional::of).collect(Collectors.toSet());
    verifyRetainedWindows(rows, table, expectedWindows);
  }

  @Test
  public void shouldQuerySessionWindowMaterializedTableWithRetention() {
    // Given:
    final PersistentQueryMetadata query = executeQuery(
        "CREATE TABLE " + output + " AS"
            + " SELECT USERID, COUNT(*) AS COUNT FROM " + PAGE_VIEWS_STREAM
            + " WINDOW SESSION (" + WINDOW_SEGMENT_DURATION.getSeconds()/2 + " SECONDS,"
            + " RETENTION " + WINDOW_SEGMENT_DURATION.getSeconds() + " SECONDS,"
            + " GRACE PERIOD 0 SECONDS"
            + ") GROUP BY USERID;"
    );

    final List<ConsumerRecord<Windowed<String>, GenericRow>> rows =
        waitForPageViewRows(SESSION_WINDOWED_DESERIALIZER, query.getPhysicalSchema());

    // When:
    final Materialization materialization = query.getMaterialization(queryId, contextStacker).get();

    // Then:
    assertThat(materialization.windowType(), is(Optional.of(WindowType.SESSION)));
    final MaterializedWindowedTable table = materialization.windowed();
    final Set<Optional<Window>> expectedWindows = Stream.of(
        Window.of(WINDOW_START_INSTANTS.get(3), WINDOW_START_INSTANTS.get(3))
    ).map(Optional::of).collect(Collectors.toSet());
    verifyRetainedWindows(rows, table, expectedWindows);
  }

  @Test
  public void shouldQueryMaterializedTableWithKeyFieldsInProjection() {
    // Given:
    final PersistentQueryMetadata query = executeQuery(
        "CREATE TABLE " + output + " AS"
            + " SELECT USERID, COUNT(*), AS_VALUE(USERID) AS USERID_2 FROM " + USER_TABLE
            + " GROUP BY USERID;"
    );

    final LogicalSchema schema = schema(
        "KSQL_COL_0", SqlTypes.BIGINT,
        "USERID_2", SqlTypes.STRING
    );

    final Map<String, GenericRow> rows = waitForUniqueUserRows(STRING_DESERIALIZER, schema);


    // When:
    final Materialization materialization = query.getMaterialization(queryId, contextStacker).get();

    // Then:
    assertThat(materialization.windowType(), is(Optional.empty()));

    final MaterializedTable table = materialization.nonWindowed();

    rows.forEach((rowKey, value) -> {
      final GenericKey key = genericKey(rowKey);

      final List<Row> rowList =
          withRetry(() -> Lists.newArrayList(table.get(key, PARTITION)));
      assertThat(rowList.size(), is(1));
      assertThat(rowList.get(0).schema(), is(schema));
      assertThat(rowList.get(0).key(), is(key));
      assertThat(rowList.get(0).value(), is(value));
    });
  }

  @Test
  public void shouldQueryMaterializedTableWithMultipleAggregationColumns() {
    // Given:
    final PersistentQueryMetadata query = executeQuery(
        "CREATE TABLE " + output + " AS"
            + " SELECT USERID, COUNT(1) AS COUNT, SUM(REGISTERTIME) AS SUM FROM " + USER_TABLE
            + " GROUP BY USERID;"
    );

    final LogicalSchema schema = schema(
        "COUNT", SqlTypes.BIGINT,
        "SUM", SqlTypes.BIGINT
    );

    final Map<String, GenericRow> rows = waitForUniqueUserRows(STRING_DESERIALIZER, schema);

    // When:
    final Materialization materialization = query.getMaterialization(queryId, contextStacker).get();

    // Then:
    assertThat(materialization.windowType(), is(Optional.empty()));

    final MaterializedTable table = materialization.nonWindowed();

    rows.forEach((rowKey, value) -> {
      final GenericKey key = genericKey(rowKey);

      final List<Row> rowList =
          withRetry(() -> Lists.newArrayList(table.get(key, PARTITION)));
      assertThat(rowList.size(), is(1));
      assertThat(rowList.get(0).schema(), is(schema));
      assertThat(rowList.get(0).key(), is(key));
      assertThat(rowList.get(0).value(), is(value));
    });
  }

  @Test
  public void shouldHandleHavingClause() {
    // Note: HAVING clause are handled centrally by KsqlMaterialization. This logic will have been
    // installed as part of building the below statement:

    // Given:
    final PersistentQueryMetadata query = executeQuery(
        "CREATE TABLE " + output + " AS"
            + " SELECT USERID, COUNT(*) AS COUNT FROM " + USER_TABLE
            + " GROUP BY USERID"
            + " HAVING SUM(REGISTERTIME) > 2;"
    );

    final LogicalSchema schema = schema("COUNT", SqlTypes.BIGINT);

    final int matches = (int) USER_DATA_PROVIDER.data().values().stream()
        .filter(row -> ((Long) row.get(0)) > 2)
        .count();

    final Map<String, GenericRow> rows = waitForUniqueUserRows(matches, STRING_DESERIALIZER, schema);

    // When:
    final Materialization materialization = query.getMaterialization(queryId, contextStacker).get();

    // Then:
    final MaterializedTable table = materialization.nonWindowed();

    rows.forEach((rowKey, value) -> {
      // Rows passing the HAVING clause:
      final GenericKey key = genericKey(rowKey);

      final List<Row> rowList =
          withRetry(() -> Lists.newArrayList(table.get(key, PARTITION)));
      assertThat(rowList.size(), is(1));
      assertThat(rowList.get(0).schema(), is(schema));
      assertThat(rowList.get(0).key(), is(key));
      assertThat(rowList.get(0).value(), is(value));
    });

    USER_DATA_PROVIDER.data().entries().stream()
        .filter(e -> !rows.containsKey(e.getKey().get(0)))
        .forEach(e -> {
          // Rows filtered by the HAVING clause:
          final List<Row> rowList =
              withRetry(() -> Lists.newArrayList(table.get(e.getKey(), PARTITION)));
          assertThat(rowList.isEmpty(), is(true));
        });
  }

  private static void verifyRetainedWindows(
      final List<ConsumerRecord<Windowed<String>, GenericRow>> rows,
      final MaterializedWindowedTable table,
      final Set<Optional<Window>> expectedWindows
  ) {
    rows.forEach(record -> {
      final GenericKey key = genericKey(record.key().key());
      final List<WindowedRow> resultAtWindowStart =
          withRetry(() -> Lists.newArrayList(table.get(key, PARTITION, Range.all(), Range.all())));

      assertThat("Should have fewer windows retained",
          resultAtWindowStart,
          hasSize(expectedWindows.size()));
      final Set<Optional<Window>> actualWindows = resultAtWindowStart.stream()
          .map(WindowedRow::window)
          .collect(Collectors.toSet());
      assertThat("Should retain the latest windows", actualWindows, equalTo(expectedWindows));
    });
  }

  private <T> Map<T, GenericRow> waitForUniqueUserRows(
      final Deserializer<T> keyDeserializer,
      final LogicalSchema aggregateSchema
  ) {
    return waitForUniqueUserRows(
        USER_DATA_PROVIDER.data().size(),
        keyDeserializer,
        aggregateSchema
    );
  }

  private <T> Map<T, GenericRow> waitForUniqueUserRows(
      final int count,
      final Deserializer<T> keyDeserializer,
      final LogicalSchema aggregateSchema
  ) {
    return TEST_HARNESS.verifyAvailableUniqueRows(
        output.toUpperCase(),
        count,
        VALUE_FORMAT,
        PhysicalSchema.from(aggregateSchema, SerdeFeatures.of(), SerdeFeatures.of()),
        keyDeserializer
    );
  }

  private <T> List<ConsumerRecord<T, GenericRow>> waitForPageViewRows(
      final Deserializer<T> keyDeserializer,
      final PhysicalSchema aggregateSchema) {
    return TEST_HARNESS.verifyAvailableRows(
        output.toUpperCase(),
        hasSize(PAGE_VIEW_DATA_PROVIDER.data().size() * NUM_WINDOWS),
        VALUE_FORMAT,
        aggregateSchema,
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

  @SuppressWarnings("SameParameterValue")
  private static LogicalSchema schema(
      final String columnName0,
      final SqlType columnType0
  ) {
    return LogicalSchema.builder()
        .keyColumn(ColumnName.of("USERID"), SqlTypes.STRING)
        .valueColumn(ColumnName.of(columnName0), columnType0)
        .build();
  }

  @SuppressWarnings("SameParameterValue")
  private static LogicalSchema schema(
      final String columnName0, final SqlType columnType0,
      final String columnName1, final SqlType columnType1
  ) {
    return LogicalSchema.builder()
        .keyColumn(ColumnName.of("USERID"), SqlTypes.STRING)
        .valueColumn(ColumnName.of(columnName0), columnType0)
        .valueColumn(ColumnName.of(columnName1), columnType1)
        .build();
  }

  private static void initializeKsql(final TestKsqlContext ksqlContext) {
    ksqlContext.ensureStarted();

    ksqlContext.sql("CREATE TABLE " + USER_TABLE
        + " (" + USER_DATA_PROVIDER.ksqlSchemaString(true) + ")"
        + " WITH ("
        + "    kafka_topic='" + USERS_TOPIC + "', "
        + "    value_format='" + VALUE_FORMAT.name() + "'"
        + ");"
    );

    ksqlContext.sql("CREATE STREAM " + USER_STREAM + " "
        + " (" + USER_DATA_PROVIDER.ksqlSchemaString(false) + ")"
        + " WITH ("
        + "    kafka_topic='" + USERS_TOPIC + "', "
        + "    value_format='" + VALUE_FORMAT.name() + "'"
        + ");"
    );

    ksqlContext.sql("CREATE STREAM " + PAGE_VIEWS_STREAM + " "
        + " (" + PAGE_VIEW_DATA_PROVIDER.ksqlSchemaString(false) + ")"
        + " WITH ("
        + "    kafka_topic='" + PAGE_VIEWS_TOPIC + "', "
        + "    value_format='" + VALUE_FORMAT.name() + "'"
        + ");"
    );
  }

  /*
   * table.get() calls can sometimes fail when partitions are being rebalanced.
   * When that happens: retry!
   */
  private static <T> T withRetry(final Supplier<T> supplier) {
    return assertThatEventually(supplier, is(notNullValue()), RetryOnException);
  }
}

