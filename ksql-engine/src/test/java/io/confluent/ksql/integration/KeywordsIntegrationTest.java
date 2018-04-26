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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.testutils.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KafkaTopicClientImpl;
import io.confluent.ksql.util.KeywordDataProvider;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.OrderDataProvider;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.QueuedQueryMetadata;
import io.confluent.ksql.util.TestDataProvider;
import io.confluent.ksql.util.TopicProducer;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;

/**
 * Tests covering use of reserved keywords within SQL statements.
 */
@Category({IntegrationTest.class})
public class KeywordsIntegrationTest {

  private static final String INPUT_STREAM = "INPUT_STREAM";
  private static final AtomicInteger COUNTER = new AtomicInteger(0);

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster KAFKA =
      EmbeddedSingleNodeKafkaCluster.newBuilder().build();

  private KsqlEngine ksqlEngine;
  private final TopicProducer topicProducer = new TopicProducer(KAFKA);
  private KafkaTopicClient topicClient;
  private String inputTopic;
  private List<QueryId> queryIds = new ArrayList<>();

  @Before
  public void before() throws Exception {
    queryIds.clear();

    final int count = COUNTER.incrementAndGet();
    inputTopic = "INPUT_" + count;

    final KsqlConfig ksqlConfig = new KsqlConfig(getKsqlConfig());

    topicClient = new KafkaTopicClientImpl(AdminClient.create(
        ksqlConfig.getKsqlAdminClientConfigProps()));

    ksqlEngine = new KsqlEngine(ksqlConfig, topicClient);

    ensureInputTopic();
  }

  @After
  public void after() throws Exception {
    queryIds.forEach(queryId -> ksqlEngine.terminateQuery(queryId, true));

    ksqlEngine.buildMultipleQueries("DROP STREAM " + INPUT_STREAM + ";", Collections.emptyMap());

    if (ksqlEngine != null) {
      ksqlEngine.close();
    }

    if (topicClient != null) {
      topicClient.close();
    }
  }

  @Test
  public void shouldHandleImportingTopicWhereSchemaHasKeywordFieldNames() throws Exception {
    executeImportStatement(
        "CREATE STREAM %s (`Group` bigint, `From` varchar, `Where` varchar) "
        + "WITH (value_format = 'json', kafka_topic='%s');",
        INPUT_STREAM, inputTopic);

    produceInputData(new KeywordDataProvider());

    final KeyValue<String, GenericRow> row =
        executeTransientQuery(1, "SELECT * from %s;", INPUT_STREAM)
            .get(0);

    assertThat(row.value.getColumns(), hasSize(5));
    assertThat(row.value.getColumns().get(2), is(0L));
    assertThat(row.value.getColumns().get(3), is("from_0"));
    assertThat(row.value.getColumns().get(4), is("where_0"));
  }

  @Test
  public void shouldHandleMixedCaseSelectAliasThatIsKeyword() throws Exception {
    executeImportStatement(
        "CREATE STREAM %s (ORDERID varchar) "
        + "WITH (value_format = 'json', kafka_topic='%s');",
        INPUT_STREAM, inputTopic);

    produceInputData(new OrderDataProvider());

    executePersistentQuery(
        "CREATE STREAM test AS SELECT orderId AS `Group` FROM %s;",
        INPUT_STREAM);

    final List<KeyValue<String, GenericRow>> rows =
        executeTransientQuery(3, "SELECT * from test;");

    rows.forEach(row -> {
      assertThat(row.value.getColumns().get(2), instanceOf(String.class));
      assertThat((String) row.value.getColumns().get(2), containsString("ORDER_"));
    });
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

  private QueryMetadata executePersistentQuery(final String queryString,
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

  private List<KeyValue<String, GenericRow>> executeTransientQuery(
      final int expectedRowCount,
      final String statement,
      final String... args) throws Exception {

    final QueryMetadata queryMetadata = executePersistentQuery(statement, args);

    final List<KeyValue<String, GenericRow>> rows = new ArrayList<>();
    final BlockingQueue<KeyValue<String, GenericRow>> rowQueue =
        ((QueuedQueryMetadata) queryMetadata).getRowQueue();

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

    return rows;
  }
}