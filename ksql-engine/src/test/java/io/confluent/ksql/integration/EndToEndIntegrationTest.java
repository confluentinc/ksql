/**
 * Copyright 2017 Confluent Inc.
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
 **/
package io.confluent.ksql.integration;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KafkaTopicClientImpl;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PageViewDataProvider;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.QueuedQueryMetadata;
import io.confluent.ksql.util.UserDataProvider;

import static java.lang.String.format;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * This test emulates the end to end flow in the quick start guide and ensures that the outputs at each stage
 * are what we expect. This tests a broad set of KSQL functionality and is a good catch-all.
 */
@Category({IntegrationTest.class})
public class EndToEndIntegrationTest {

  private static final Logger log = LoggerFactory.getLogger(EndToEndIntegrationTest.class);
  private IntegrationTestHarness testHarness;
  private KsqlEngine ksqlEngine;

  private PageViewDataProvider pageViewDataProvider;

  private final String pageViewTopic = "pageviews";
  private final String usersTopic = "users";

  private final String pageViewStream = "pageviews_original";
  private final String userTable = "users_original";

  @Rule
  public final Timeout timeout = Timeout.seconds(120);

  @Before
  public void before() throws Exception {
    testHarness = new IntegrationTestHarness();
    testHarness.start();
    Map<String, Object> streamsConfig = testHarness.ksqlConfig.getKsqlStreamConfigProps();
    streamsConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    KsqlConfig ksqlconfig = new KsqlConfig(streamsConfig);
    AdminClient adminClient = AdminClient.create(ksqlconfig.getKsqlAdminClientConfigProps());
    KafkaTopicClient topicClient = new KafkaTopicClientImpl(adminClient);

    ksqlEngine = new KsqlEngine(ksqlconfig, topicClient);

    testHarness.createTopic(pageViewTopic);
    testHarness.createTopic(usersTopic);

    pageViewDataProvider = new PageViewDataProvider();

    testHarness.publishTestData(
        usersTopic, new UserDataProvider(), System.currentTimeMillis() - 10000);
    testHarness.publishTestData(pageViewTopic, pageViewDataProvider, System.currentTimeMillis());

    ksqlEngine.buildMultipleQueries(
        format("CREATE TABLE %s (registertime bigint, gender varchar, regionid varchar, " +
               "userid varchar) WITH (kafka_topic='%s', value_format='JSON', key = 'userid');",
               userTable,
               usersTopic), Collections.emptyMap());
    ksqlEngine.buildMultipleQueries(
        format("CREATE STREAM %s (viewtime bigint, userid varchar, pageid varchar) " +
               "WITH (kafka_topic='%s', value_format='JSON');", pageViewStream,
               pageViewTopic), Collections.emptyMap());
  }

  @After
  public void after() {
    ksqlEngine.close();
    testHarness.stop();
  }

  @Test
  public void shouldSelectAllFromUsers() throws Exception {
    final QueuedQueryMetadata queryMetadata = executeQuery(
        "SELECT * from %s;", userTable);

    BlockingQueue<KeyValue<String, GenericRow>> rowQueue = queryMetadata.getRowQueue();

    Set<String> actualUsers = new HashSet<>();
    Set<String> expectedUsers = Utils.mkSet("USER_0", "USER_1", "USER_2", "USER_3", "USER_4");
    while (actualUsers.size() < expectedUsers.size()) {
      KeyValue<String, GenericRow> nextRow = rowQueue.poll();
      if (nextRow != null) {
        List<Object> columns = nextRow.value.getColumns();
        assertEquals(6, columns.size());
        actualUsers.add((String) columns.get(1));
      }
    }
    assertEquals(expectedUsers, actualUsers);
  }

  @Test
  public void shouldSelectFromPageViewsWithSpecificColumn() throws Exception {
    final QueuedQueryMetadata queryMetadata =
        executeQuery("SELECT pageid from %s;", pageViewStream);

    BlockingQueue<KeyValue<String, GenericRow>> rowQueue = queryMetadata.getRowQueue();

    List<String> actualPages = new ArrayList<>();
    List<String> expectedPages =
        Arrays.asList("PAGE_1", "PAGE_2", "PAGE_3", "PAGE_4", "PAGE_5", "PAGE_5", "PAGE_5");
    while (actualPages.size() < expectedPages.size()) {
      KeyValue<String, GenericRow> nextRow = rowQueue.poll();
      if (nextRow != null) {
        List<Object> columns = nextRow.value.getColumns();
        assertEquals(1, columns.size());
        String page = (String) columns.get(0);
        actualPages.add(page);
      }
    }

    assertEquals(expectedPages, actualPages);
    queryMetadata.getKafkaStreams().close();
  }

  @Test
  public void shouldSelectAllFromDerivedStream() throws Exception {

    executeStatement(
        "CREATE STREAM pageviews_female"
        + " AS SELECT %s.userid AS userid, pageid, regionid, gender "
        + " FROM %s "
        + " LEFT JOIN %s ON %s.userid = %s.userid"
        + " WHERE gender = 'FEMALE';",
        userTable, pageViewStream, userTable, pageViewStream,
        userTable);

    final QueuedQueryMetadata queryMetadata = executeQuery(
        "SELECT * from pageviews_female;");

    List<KeyValue<String, GenericRow>> results = new ArrayList<>();
    BlockingQueue<KeyValue<String, GenericRow>> rowQueue = queryMetadata.getRowQueue();

    // From the mock data, we expect exactly 3 page views from female users.
    List<String> expectedPages = Arrays.asList("PAGE_2", "PAGE_5", "PAGE_5");
    List<String> expectedUsers = Arrays.asList("USER_2", "USER_0", "USER_2");
    List<String> actualPages = new ArrayList<>();
    List<String> actualUsers = new ArrayList<>();

    TestUtils.waitForCondition(() -> {
      try {
        log.debug("polling from pageviews_female");
        KeyValue<String, GenericRow> nextRow = rowQueue.poll(8000, TimeUnit.MILLISECONDS);
        if (nextRow != null) {
          results.add(nextRow);
        } else {
          // If we didn't receive any records on the output topic for 8 seconds, it probably means that the join
          // failed because the table data wasn't populated when the stream data was consumed. We should just
          // re populate the stream data to try the join again.
          log.warn("repopulating data in {} because the join returned empty results.",
                   pageViewTopic);
          testHarness
              .publishTestData(pageViewTopic, pageViewDataProvider, System.currentTimeMillis());
        }
      } catch (Exception e) {
        log.error("Got exception when polling from pageviews_female", e);
      }
      return 3 <= results.size();
    }, 30000, "Could not consume any records from " + pageViewTopic + " for 30 seconds");

    for (KeyValue<String, GenericRow> result : results) {
      List<Object> columns = result.value.getColumns();
      log.debug("pageview join: {}", columns);

      assertEquals(6, columns.size());
      String user = (String) columns.get(2);
      actualUsers.add(user);

      String page = (String) columns.get(3);
      actualPages.add(page);
    }

    assertEquals(expectedPages, actualPages);
    assertEquals(expectedUsers, actualUsers);
  }

  @Test
  public void shouldCreateStreamUsingLikeClause() throws Exception {

    executeStatement(
        "CREATE STREAM pageviews_like_p5"
        + " WITH (kafka_topic='pageviews_enriched_r0', value_format='DELIMITED')"
        + " AS SELECT * FROM %s"
        + " WHERE pageId LIKE '%%_5';",
        pageViewStream);

    final QueuedQueryMetadata queryMetadata =
        executeQuery("SELECT userid, pageid from pageviews_like_p5;");

    final List<Object> columns = waitForFirstRow(queryMetadata);

    assertThat(columns.get(1), is("PAGE_5"));
  }

  @Test
  public void shouldRetainSelectedColumnsInPartitionBy() throws Exception {

    executeStatement(
        "CREATE STREAM pageviews_by_viewtime "
        + "AS SELECT viewtime, pageid, userid "
        + "from %s "
        + "partition by viewtime;",
        pageViewStream);

    final QueuedQueryMetadata queryMetadata = executeQuery(
        "SELECT * from pageviews_by_viewtime;");

    final List<Object> columns = waitForFirstRow(queryMetadata);

    assertThat(columns.get(3).toString(), startsWith("PAGE_"));
    assertThat(columns.get(4).toString(), startsWith("USER_"));
  }

  @Test
  public void shouldSupportDroppingAndRecreatingJoinQuery() throws Exception {
    final String createStreamStatement = format(
        "create stream cart_event_product as "
        + "select pv.pageid, u.gender "
        + "from %s pv left join %s u on pv.userid=u.userid;",
        pageViewStream, userTable);

    executeStatement(createStreamStatement);

    ksqlEngine.terminateQuery(new QueryId("CSAS_CART_EVENT_PRODUCT"), true);

    executeStatement("DROP STREAM CART_EVENT_PRODUCT;");

    executeStatement(createStreamStatement);

    final QueuedQueryMetadata queryMetadata = executeQuery(
        "SELECT * from cart_event_product;");

    final List<Object> columns = waitForFirstRow(queryMetadata);

    assertThat(columns.get(1).toString(), startsWith("USER_"));
    assertThat(columns.get(2).toString(), startsWith("PAGE_"));
    assertThat(columns.get(3).toString(), either(is("FEMALE")).or(is("MALE")));
  }

  private QueryMetadata executeStatement(final String statement,
                                         final String... args) throws Exception {
    final String formatted = String.format(statement, args);
    log.debug("Sending statement: {}", formatted);

    final List<QueryMetadata> queries =
        ksqlEngine.buildMultipleQueries(formatted, Collections.emptyMap());

    queries.forEach(QueryMetadata::start);

    return queries.isEmpty() ? null : queries.get(0);
  }

  private QueuedQueryMetadata executeQuery(final String statement,
                                           final String... args) throws Exception {
    final QueryMetadata queryMetadata = executeStatement(statement, args);
    assertThat(queryMetadata, instanceOf(QueuedQueryMetadata.class));
    return (QueuedQueryMetadata) queryMetadata;
  }

  private static List<Object> waitForFirstRow(
      final QueuedQueryMetadata queryMetadata) throws Exception {
    return verifyAvailableRows(queryMetadata, 1).get(0).getColumns();
  }

  private static List<GenericRow> verifyAvailableRows(final QueuedQueryMetadata queryMetadata,
                                                      final int expectedRows) throws Exception {
    final BlockingQueue<KeyValue<String, GenericRow>> rowQueue = queryMetadata.getRowQueue();

    TestUtils.waitForCondition(
        () -> rowQueue.size() >= expectedRows,
        30_000,
        expectedRows + " rows where not available after 30 seconds");

    final List<KeyValue<String, GenericRow>> rows = new ArrayList<>();
    rowQueue.drainTo(rows);

    return rows.stream()
        .map(kv -> kv.value)
        .collect(Collectors.toList());
  }
}
