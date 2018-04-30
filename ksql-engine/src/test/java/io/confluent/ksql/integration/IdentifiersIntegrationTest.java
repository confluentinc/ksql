/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.integration;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.analyzer.AnalysisException;
import io.confluent.ksql.parser.exception.ParseFailedException;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.testutils.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KafkaTopicClientImpl;
import io.confluent.ksql.util.KeywordDataProvider;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.MixedCaseDataProvider;
import io.confluent.ksql.util.OrderDataProvider;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.QueuedQueryMetadata;
import io.confluent.ksql.util.TestDataProvider;
import io.confluent.ksql.util.TopicProducer;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests covering the use of column identifiers within SQL statements, including aliases and the
 * use of keywords, and the affect of identifier case.
 */
@Category({IntegrationTest.class})
public class IdentifiersIntegrationTest {

  private static final String INPUT_STREAM = "INPUT_STREAM";
  private static final AtomicInteger COUNTER = new AtomicInteger(0);

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster KAFKA =
      EmbeddedSingleNodeKafkaCluster.newBuilder().build();

  private KsqlEngine ksqlEngine;
  private final TopicProducer topicProducer = new TopicProducer(KAFKA);
  private KafkaTopicClient topicClient;
  private String inputTopic;
  private String outputStream;
  private List<QueryId> queryIds = new ArrayList<>();

  @Before
  public void before() throws Exception {
    queryIds.clear();

    final int count = COUNTER.incrementAndGet();
    inputTopic = "INPUT_" + count;
    outputStream = "OUTPUT_" + count;

    final KsqlConfig ksqlConfig = new KsqlConfig(getKsqlConfig());

    topicClient = new KafkaTopicClientImpl(AdminClient.create(
        ksqlConfig.getKsqlAdminClientConfigProps()));

    ksqlEngine = new KsqlEngine(ksqlConfig, topicClient);

    ensureInputTopic();
  }

  @After
  public void after() throws Exception {
    queryIds.forEach(queryId -> ksqlEngine.terminateQuery(queryId, true));

    tryDropStream(INPUT_STREAM);
    tryDropStream(outputStream);

    if (ksqlEngine != null) {
      ksqlEngine.close();
    }

    if (topicClient != null) {
      topicClient.close();
    }
  }

  @Test
  public void shouldImportQuotedColumnsWhereCaseMatches() throws Exception {
    // Given:
    executeImportStatement(
        "CREATE STREAM %s ("
        + "\"thing1\" varchar, `thing2` varchar,"
        + "\"Thing1\" varchar, `Thing2` varchar,"
        + "\"THING1\" varchar, `THING2` varchar"
        + ") WITH (value_format = 'json', kafka_topic='%s');",
        INPUT_STREAM, inputTopic);

    produceInputData(new MixedCaseDataProvider());

    // When:
    final SchemaAndRow result = getFirstRow(INPUT_STREAM);

    // Then:
    assertThat(fieldNames(result.schema), containsInAnyOrder(
        "thing1", "thing2", "Thing1", "Thing2", "THING1", "THING2", "ROWTIME", "ROWKEY"));

    assertThat(dropRowTimeAndKey(result.row), is(genericRow(
        "thing1_0", "thing2_0",
        "Thing1_0", "Thing1_0",
        "THING1_0", "THING2_0")));
  }

  @Test
  public void shouldNotImportQuotedColumnsWhereCaseDiffers() throws Exception {
    // Given:
    executeImportStatement(
        "CREATE STREAM %s ("
        + "\"thINg1\" varchar, `thINg2` varchar"
        + ") WITH (value_format = 'json', kafka_topic='%s');",
        INPUT_STREAM, inputTopic);

    produceInputData(new MixedCaseDataProvider());

    // When:
    final List<Object> columns = getFirstRow(INPUT_STREAM).row.getColumns();

    // Then:
    assertThat(columns, hasSize(4));
    assertThat("\"thINg1\" should not match any column", columns.get(2), is(nullValue()));
    assertThat("`thINg2` should not match any column", columns.get(3), is(nullValue()));
  }

  @Test
  public void shouldImportQuotedKeywordColumnsWhereCaseMatches() throws Exception {
    // Given:
    executeImportStatement(
        "CREATE STREAM %s (\"Group\" varchar, `From` varchar) "
        + "WITH (value_format = 'json', kafka_topic='%s');",
        INPUT_STREAM, inputTopic);

    produceInputData(new KeywordDataProvider());

    // When:
    final SchemaAndRow result = getFirstRow(INPUT_STREAM);

    // Then:
    assertThat(fieldNames(result.schema), containsInAnyOrder(
        "Group", "From", "ROWTIME", "ROWKEY"));

    assertThat(dropRowTimeAndKey(result.row), is(genericRow("group_0", "from_0")));
  }

  @Test
  public void shouldNotImportQuotedKeywordColumnsWhereCaseDiffers() throws Exception {
    // Given:
    executeImportStatement(
        "CREATE STREAM %s (\"group\" varchar, `WHERE` varchar) "
        + "WITH (value_format = 'json', kafka_topic='%s');",
        INPUT_STREAM, inputTopic);

    produceInputData(new KeywordDataProvider());

    // When:
    final List<Object> columns = getFirstRow(INPUT_STREAM).row.getColumns();

    // Then:
    assertThat(columns, hasSize(4));
    assertThat("\"group\" should not match schema's Group", columns.get(2), is(nullValue()));
    assertThat("`WHERE` should not match schema's Where", columns.get(3), is(nullValue()));
  }

  @Test
  public void shouldTransformQuotedColumnsWhereCaseMatches() throws Exception {
    // Given:
    executeImportStatement(
        "CREATE STREAM %s ("
        + "\"thing1\" varchar, `thing2` varchar,"
        + "\"Thing1\" varchar, `Thing2` varchar,"
        + "\"THING1\" varchar, `THING2` varchar"
        + ") WITH (value_format = 'json', kafka_topic='%s');",
        INPUT_STREAM, inputTopic);

    produceInputData(new MixedCaseDataProvider());

    executeQuery(
        "CREATE STREAM %s AS SELECT "
        + "\"thing1\" AS \"object1\", `thing2` AS `object2`,"
        + "\"Thing1\" AS \"Object1\", `Thing2` AS `Object2`,"
        + "\"THING1\" AS \"OBJECT1\", `THING2` AS `OBJECT2`"
        + " FROM %s;",
        outputStream, INPUT_STREAM);

    // When:
    final SchemaAndRow result = getFirstRow(outputStream);

    // Then:
    assertThat(fieldNames(result.schema), containsInAnyOrder(
        "object1", "object2", "Object1", "Object2", "OBJECT1", "OBJECT2", "ROWTIME", "ROWKEY"));

    assertThat(dropRowTimeAndKey(result.row), is(genericRow(
        "thing1_0", "thing2_0",
        "Thing1_0", "Thing1_0",
        "THING1_0", "THING2_0")));
  }

  @Test(expected = AnalysisException.class)
  public void shouldNotTransformQuotedColumnsWhereCaseDiffers() throws Exception {
    // Given:
    executeImportStatement(
        "CREATE STREAM %s ("
        + "\"thing1\" varchar, `thing2` varchar,"
        + "\"Thing1\" varchar, `Thing2` varchar,"
        + "\"THING1\" varchar, `THING2` varchar"
        + ") WITH (value_format = 'json', kafka_topic='%s');",
        INPUT_STREAM, inputTopic);

    produceInputData(new MixedCaseDataProvider());

    // When:
    executeQuery(
        "CREATE STREAM %s AS SELECT "
        + "\"thINg1\" AS \"obJEct1\", `thINg2` AS `obJEct2`"
        + " FROM %s;",
        outputStream, INPUT_STREAM);

    // Then:
    // Should throw because "thINg1" and "thINg2" are unknown columns.
  }

  @Test
  public void shouldTransformQuotedKeywordColumnsWhereCaseMatches() throws Exception {
    // Given:
    executeImportStatement(
        "CREATE STREAM %s (`Group` varchar, `From` varchar, `Where` varchar) "
        + "WITH (value_format = 'json', kafka_topic='%s');",
        INPUT_STREAM, inputTopic);

    produceInputData(new KeywordDataProvider());

    executeQuery(
        "CREATE STREAM %s AS SELECT "
        + "\"Group\" AS \"From\", `Where` AS `BY` FROM %s;",
        outputStream, INPUT_STREAM);

    // When:
    final SchemaAndRow result = getFirstRow(outputStream);

    // Then:
    assertThat(fieldNames(result.schema), containsInAnyOrder(
        "From", "BY", "ROWTIME", "ROWKEY"));

    assertThat(dropRowTimeAndKey(result.row), is(genericRow("group_0", "where_0")));
  }

  @Test(expected = AnalysisException.class)
  public void shouldNotTransformQuotedKeywordColumnsWhereCaseDiffer() throws Exception {
    // Given:
    executeImportStatement(
        "CREATE STREAM %s (`Group` varchar, `From` varchar, `Where` varchar) "
        + "WITH (value_format = 'json', kafka_topic='%s');",
        INPUT_STREAM, inputTopic);

    produceInputData(new KeywordDataProvider());

    // When:
    executeQuery(
        "CREATE STREAM %s AS SELECT "
        + "\"grOUp\", `whERe` FROM %s;",
        outputStream, INPUT_STREAM);

    // Then:
    // Should throw due to unknown columns
  }

  @Test
  public void shouldExportQuotedColumnsWhereCaseMatches() throws Exception {
    // Given:
    executeImportStatement(
        "CREATE STREAM %s ("
        + "\"thing1\" varchar, `thing2` varchar,"
        + "\"Thing1\" varchar, `Thing2` varchar,"
        + "\"THING1\" varchar, `THING2` varchar"
        + ") WITH (value_format = 'json', kafka_topic='%s');",
        INPUT_STREAM, inputTopic);

    produceInputData(new MixedCaseDataProvider());

    // When:
    final SchemaAndRow result = getFirstRow(
        "SELECT "
        + "\"thing1\" AS \"object1\", `thing2` AS `object2`,"
        + "\"Thing1\" AS \"Object1\", `Thing2` AS `Object2`,"
        + "\"THING1\" AS \"OBJECT1\", `THING2` AS `OBJECT2`"
        + " from %s;",
        INPUT_STREAM);

    // Then:
    assertThat(fieldNames(result.schema), containsInAnyOrder(
        "object1", "object2", "Object1", "Object2", "OBJECT1", "OBJECT2"));

    assertThat(dropRowTimeAndKey(result.row), is(genericRow(
        "thing1_0", "thing2_0",
        "Thing1_0", "Thing1_0",
        "THING1_0", "THING2_0")));
  }

  @Test(expected = AnalysisException.class)
  public void shouldNotExportQuotedColumnsWhereCaseDiffers() throws Exception {
    // Given:
    executeImportStatement(
        "CREATE STREAM %s ("
        + "\"thing1\" varchar, `thing2` varchar,"
        + "\"Thing1\" varchar, `Thing2` varchar,"
        + "\"THING1\" varchar, `THING2` varchar"
        + ") WITH (value_format = 'json', kafka_topic='%s');",
        INPUT_STREAM, inputTopic);

    produceInputData(new MixedCaseDataProvider());

    // When:
    getFirstRow(
        "SELECT "
        + "\"thINg1\" AS \"obJEct1\", `thINg2` AS `obJEct2`"
        + " from %s;",
        INPUT_STREAM);

    // Then:
    // Should throw because "thINg1" and "thINg2" are unknown columns.
  }

  @Test
  public void shouldExportQuotedKeywordColumnsWhereCaseMatches() throws Exception {
    // Given:
    executeImportStatement(
        "CREATE STREAM %s (`Group` varchar, `From` varchar, `Where` varchar) "
        + "WITH (value_format = 'json', kafka_topic='%s');",
        INPUT_STREAM, inputTopic);

    produceInputData(new KeywordDataProvider());

    // When:
    final SchemaAndRow result = getFirstRow(
        "SELECT "
        + "\"Group\" AS \"From\", `Where` AS `BY`"
        + " from %s;",
        INPUT_STREAM);

    // Then:
    assertThat(fieldNames(result.schema), containsInAnyOrder("From", "BY"));

    assertThat(dropRowTimeAndKey(result.row), is(genericRow("group_0", "where_0")));
  }

  @Test(expected = AnalysisException.class)
  public void shouldNotExportQuotedKeywordColumnsWhereCaseDiffer() throws Exception {
    // Given:
    executeImportStatement(
        "CREATE STREAM %s (`Group` varchar, `From` varchar, `Where` varchar) "
        + "WITH (value_format = 'json', kafka_topic='%s');",
        INPUT_STREAM, inputTopic);

    produceInputData(new KeywordDataProvider());

    // When:
    getFirstRow(
        "SELECT "
        + "\"grOUp\", `whERe`"
        + " from %s;",
        INPUT_STREAM);

    // Then:
    // Should throw due to unknown columns
  }

  @Test
  public void shouldImportQuotedStreamIdentifier() throws Exception {
    // Given:
    executeImportStatement(
        "CREATE STREAM `%s` (ORDERTIME bigint) "
        + "WITH (value_format = 'json', kafka_topic='%s');",
        INPUT_STREAM.toLowerCase(), inputTopic);

    produceInputData(new OrderDataProvider());

    // When:
    final SchemaAndRow result = getFirstRow("\"" + INPUT_STREAM.toLowerCase() + "\"");

    // Then:
    assertThat(fieldNames(result.schema), containsInAnyOrder(
        "ORDERTIME", "ROWTIME", "ROWKEY"));

    assertThat(dropRowTimeAndKey(result.row), is(genericRow(1L)));
  }

  @Test
  public void shouldUseQuotedIdenfitierToSetSinkTopicName() throws Exception {
    // Given:
    executeImportStatement(
        "CREATE STREAM %s (`Group` varchar, `From` varchar, `Where` varchar) "
        + "WITH (value_format = 'json', kafka_topic='%s');",
        INPUT_STREAM, inputTopic);

    produceInputData(new KeywordDataProvider());

    executeQuery(
        "CREATE STREAM `%s` AS SELECT "
        + "\"Group\" AS \"From\", `Where` AS `BY` FROM %s;",
        outputStream.toLowerCase(), INPUT_STREAM);

    // When:
    final SchemaAndRow result = getFirstRow("`" + outputStream.toLowerCase() + "`");

    // Then:
    assertThat(fieldNames(result.schema), containsInAnyOrder(
        "From", "BY", "ROWTIME", "ROWKEY"));

    assertThat(dropRowTimeAndKey(result.row), is(genericRow("group_0", "where_0")));

    assertThat(topicClient.isTopicExists(outputStream.toLowerCase()), is(true));
  }

  private void tryDropStream(final String inputStream) throws Exception {
    try {
      ksqlEngine.buildMultipleQueries("DROP STREAM " + inputStream + ";", Collections.emptyMap());
    } catch (final ParseFailedException e) {
      // stream does not exist.
    }
  }

  private SchemaAndRow getFirstRow(final String streamName) throws Exception {
    return executeTransientQuery(1, "SELECT * from %s;", streamName)
        .firstRow();
  }

  private SchemaAndRow getFirstRow(final String selectStatement, final Object... args)
      throws Exception {
    return executeTransientQuery(1, selectStatement, args)
        .firstRow();
  }

  private static GenericRow genericRow(final Object... columns) {
    return new GenericRow(Arrays.asList(columns));
  }

  private static GenericRow dropRowTime(final GenericRow row) {
    row.getColumns().remove(0);
    return row;
  }

  private static GenericRow dropRowTimeAndKey(final GenericRow row) {
    dropRowTime(row).getColumns().remove(0);
    return row;
  }

  private static List<String> fieldNames(final Schema schema) {
    return schema.fields().stream()
        .map(Field::name)
        .collect(Collectors.toList());
  }

  private static Map<String, Object> getKsqlConfig() {
    final Map<String, Object> configs = new HashMap<>();
    configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.bootstrapServers());
    configs.put("application.id", "KSQL");
    configs.put("commit.interval.ms", 0);
    configs.put("cache.max.bytes.buffering", 0);
    configs.put("auto.offset.reset", "earliest");
    return configs;
  }

  private void produceInputData(final TestDataProvider dataProvider) throws Exception {
    topicProducer.produceInputData(inputTopic, dataProvider.data(), dataProvider.schema());
  }

  private void ensureInputTopic() throws InterruptedException {
    topicClient.createTopic(inputTopic, 1, (short) 1);

    TestUtils.waitForCondition(() -> topicClient.isTopicExists(inputTopic),
                               "failed to create test topic");
  }

  private void executeImportStatement(final String statement,
                                      final Object... params) throws Exception {
    final String stmt = String.format(statement, params);
    final List<QueryMetadata> result =
        ksqlEngine.buildMultipleQueries(stmt, Collections.emptyMap());
    assertThat(result, is(empty()));
  }

  private QueryMetadata executeQuery(final String queryString,
                                     final Object... params) throws Exception {
    final String query = String.format(queryString, params);

    final List<QueryMetadata> queries =
        ksqlEngine.buildMultipleQueries(query, Collections.emptyMap());

    final QueryMetadata queryMetadata = queries.get(0);

    queryMetadata.getKafkaStreams().start();

    if (queryMetadata instanceof PersistentQueryMetadata) {
      queryIds.add(((PersistentQueryMetadata) queryMetadata).getQueryId());
    }
    return queryMetadata;
  }

  @SuppressWarnings("SameParameterValue")
  private SchemaAndRows executeTransientQuery(
      final int expectedRowCount,
      final String statement,
      final Object... args) throws Exception {

    final QueuedQueryMetadata queryMetadata = (QueuedQueryMetadata) executeQuery(statement, args);

    final Schema schema = queryMetadata.getOutputNode().getSchema();

    final List<KeyValue<String, GenericRow>> rows = new ArrayList<>();
    final BlockingQueue<KeyValue<String, GenericRow>> rowQueue =
        queryMetadata.getRowQueue();

    TestUtils.waitForCondition(() -> {
      try {
        final KeyValue<String, GenericRow> nextRow = rowQueue.poll(1, TimeUnit.SECONDS);
        if (nextRow != null) {
          rows.add(nextRow);
        }
      } catch (Exception e) {
        throw new AssertionError("Exception polling", e);
      }
      return rows.size() >= expectedRowCount;
    }, 30000, "Expected " + expectedRowCount + ", got " + rows.size() + ": " + rows);

    return new SchemaAndRows(schema, rows);
  }

  private static class SchemaAndRows {

    private final Schema schema;
    private final List<KeyValue<String, GenericRow>> rows;

    private SchemaAndRows(final Schema schema,
                          final List<KeyValue<String, GenericRow>> rows) {
      this.schema = schema;
      this.rows = rows;
    }

    private SchemaAndRow firstRow() {
      return new SchemaAndRow(schema, rows.get(0).value);
    }
  }

  private static class SchemaAndRow {

    private final Schema schema;
    private final GenericRow row;

    private SchemaAndRow(final Schema schema,
                         final GenericRow row) {
      this.schema = schema;
      this.row = row;
    }
  }
}

// Todo(ac): Avro!
// Todo(ac): Table aliases.