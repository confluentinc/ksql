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

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KafkaTopicClientImpl;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PageViewDataProvider;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.QueuedQueryMetadata;
import io.confluent.ksql.util.UserDataProvider;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
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

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
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
  private UserDataProvider userDataProvider;

  private final String pageViewTopic = "pageviews";
  private final String usersTopic = "users";

  private final String pageViewStream = "pageviews_original";
  private final String userTable = "users_original";

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
    userDataProvider = new UserDataProvider();

    testHarness.publishTestData(usersTopic, userDataProvider, System.currentTimeMillis() - 10000);
    testHarness.publishTestData(pageViewTopic, pageViewDataProvider, System.currentTimeMillis());

    ksqlEngine.buildMultipleQueries(String.format("CREATE TABLE %s (registertime bigint, gender varchar, regionid varchar, " +
            "userid varchar) WITH (kafka_topic='%s', value_format='JSON', key = 'userid');",
                                                         userTable,
                                                         usersTopic), Collections.emptyMap());
    ksqlEngine.buildMultipleQueries(String.format("CREATE STREAM %s (viewtime bigint, userid varchar, pageid varchar) " +
            "WITH (kafka_topic='%s', value_format='JSON');", pageViewStream, pageViewTopic), Collections.emptyMap());
  }

  @After
  public void after() throws Exception {
    ksqlEngine.close();
    testHarness.stop();
  }

  @Test
  public void testKSQLFromEndToEnd() throws Exception {
    validateSelectFromPageViewsWithSpecificColumn();
    validateSelectAllFromUsers();
    String derivedStream = validateSelectAllFromDerivedStream();
    validateCreateStreamUsingLikeClause(derivedStream);
  }

  @Test
  public void shouldRetainSelectedColumnsInPartitionBy() throws Exception {
    String createStreamStatement = String.format("CREATE STREAM pageviews_by_viewtime as select "
                                                 + "viewtime, pageid, userid from "
                                                 + "pageviews_original "
                                                 + "partition by "
                                                 + "viewtime;");

    List<QueryMetadata> queries = ksqlEngine.buildMultipleQueries(createStreamStatement,
                                                                  Collections.emptyMap());

    assertEquals(1, queries.size());
    assertThat(queries.get(0), instanceOf(PersistentQueryMetadata.class));

    PersistentQueryMetadata persistentQueryMetadata = (PersistentQueryMetadata) queries.get(0);
    persistentQueryMetadata.getKafkaStreams().start();

    String query = String.format("SELECT * from pageviews_by_viewtime;");

    queries = ksqlEngine.buildMultipleQueries(query, Collections.emptyMap());

    assertEquals(1, queries.size());
    assertThat(queries.get(0), instanceOf(QueuedQueryMetadata.class));

    QueuedQueryMetadata queryMetadata = (QueuedQueryMetadata) queries.get(0);
    queryMetadata.getKafkaStreams().start();

    BlockingQueue<KeyValue<String, GenericRow>> rowQueue = queryMetadata.getRowQueue();
    TestUtils.waitForCondition(() -> {
      KeyValue<String, GenericRow> nextRow;
      try {
        nextRow = rowQueue.poll(1000, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
      if (nextRow != null) {
        List<Object> columns = nextRow.value.getColumns();
        assertEquals(5, columns.size());
        String pageid = columns.get(3).toString();
        assertEquals(6, pageid.length());
        assertEquals("PAGE_", pageid.substring(0, 5));

        String userid = columns.get(4).toString();
        assertEquals(6, userid.length());
        assertEquals("USER_", userid.substring(0, 5));
        return true;
      }
      return false;
    }, 10000, "Could not retrieve a record from the derived topic for 10 seconds");
  }

  private void validateSelectAllFromUsers() throws Exception {
    String query = String.format("SELECT * from %s;", userTable);
    log.debug("Sending query: {}", query);

    List<QueryMetadata> queries = ksqlEngine.buildMultipleQueries(query, Collections.emptyMap());

    assertEquals(1, queries.size());
    assertTrue(queries.get(0) instanceof QueuedQueryMetadata);

    QueuedQueryMetadata queryMetadata = (QueuedQueryMetadata) queries.get(0);
    queryMetadata.getKafkaStreams().start();

    BlockingQueue<KeyValue<String, GenericRow>> rowQueue = queryMetadata.getRowQueue();

    Set<String> actualUsers = new HashSet<>();
    Set<String> expectedUsers = Utils.mkSet("USER_0", "USER_1", "USER_2", "USER_3", "USER_4");
    while (actualUsers.size() < 5) {
      KeyValue<String, GenericRow> nextRow = rowQueue.poll();
      if (nextRow != null) {
        List<Object> columns = nextRow.value.getColumns();
        assertEquals(6, columns.size());
        actualUsers.add((String) columns.get(1));
      }
    }
    assertEquals(expectedUsers, actualUsers);
    queryMetadata.getKafkaStreams().close();
  }

  private void validateSelectFromPageViewsWithSpecificColumn() throws Exception {
    String query = String.format("SELECT pageid from %s;", pageViewStream);
    log.debug("Sending query: {}", query);

    List<QueryMetadata> queries = ksqlEngine.buildMultipleQueries(query, Collections.emptyMap());

    assertEquals(1, queries.size());
    assertTrue(queries.get(0) instanceof QueuedQueryMetadata);

    QueuedQueryMetadata queryMetadata = (QueuedQueryMetadata) queries.get(0);
    queryMetadata.getKafkaStreams().start();

    BlockingQueue<KeyValue<String, GenericRow>> rowQueue = queryMetadata.getRowQueue();

    List<String> actualPages = new ArrayList<>();
    List<String> expectedPages = Arrays.asList("PAGE_1", "PAGE_2", "PAGE_3", "PAGE_4", "PAGE_5", "PAGE_5", "PAGE_5");
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

  private String validateSelectAllFromDerivedStream() throws Exception {

    String derivedStream = "pageviews_female";
    createPageViewsFemaleStream(derivedStream);

    String selectQuery = String.format("SELECT * from %s;", derivedStream);

    List<QueryMetadata> queries = ksqlEngine.buildMultipleQueries(selectQuery, Collections.emptyMap());

    assertEquals(1, queries.size());
    assertTrue(queries.get(0) instanceof QueuedQueryMetadata);

    QueuedQueryMetadata queryMetadata = (QueuedQueryMetadata) queries.get(0);
    queryMetadata.getKafkaStreams().start();

    List<KeyValue<String, GenericRow>> results = new ArrayList<>();
    BlockingQueue<KeyValue<String, GenericRow>> rowQueue = queryMetadata.getRowQueue();

    // From the mock data, we expect exactly 3 page views from female users.

    List<String> expectedPages = Arrays.asList("PAGE_2", "PAGE_5", "PAGE_5");
    List<String> expectedUsers = Arrays.asList("USER_2", "USER_0", "USER_2");
    List<String> actualPages = new ArrayList<>();
    List<String> actualUsers = new ArrayList<>();

    TestUtils.waitForCondition(() -> {
      try {
        log.debug("polling from {}", derivedStream);
        KeyValue<String, GenericRow> nextRow = rowQueue.poll(8000, TimeUnit.MILLISECONDS);
        if (nextRow != null) {
          results.add(nextRow);
        } else {
          // If we didn't receive any records on the output topic for 8 seconds, it probably means that the join
          // failed because the table data wasn't populated when the stream data was consumed. We should just
          // re populate the stream data to try the join again.
          log.warn("repopulating data in {} because the join returned empty results.", pageViewTopic);
          testHarness.publishTestData(pageViewTopic, pageViewDataProvider, System.currentTimeMillis());
        }
      }
      catch (Exception e) {
        log.error("Got exception when polling from " + derivedStream, e);
      }
      return 3 <= results.size();
    }, 30000, "Could not consume any records from " + pageViewTopic + " for 30 seconds");

    for (KeyValue<String, GenericRow> result: results) {
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

    return derivedStream;
  }

  private void validateCreateStreamUsingLikeClause(String inputStream) throws Exception {
    String outputStream = createStreamUsingLikeClause(inputStream);
    String selectPageViewsFromRegion = String.format("SELECT userid, pageid from %s;", outputStream);
    List<QueryMetadata> queries = ksqlEngine.buildMultipleQueries(selectPageViewsFromRegion,
            Collections.emptyMap());

    assertEquals(1, queries.size());
    assertTrue(queries.get(0) instanceof QueuedQueryMetadata);

    QueuedQueryMetadata queryMetadata = (QueuedQueryMetadata) queries.get(0);
    queryMetadata.getKafkaStreams().start();

    BlockingQueue<KeyValue<String, GenericRow>> rowQueue = queryMetadata.getRowQueue();

    List<String> actualPages = new ArrayList<>();
    List<String> expectedPages = Arrays.asList("PAGE_5");

    TestUtils.waitForCondition(() -> {
      try {
        log.debug("polling from {}", outputStream);
        KeyValue<String, GenericRow> nextRow = rowQueue.poll(1000, TimeUnit.MILLISECONDS);
        if (nextRow != null) {
          List<Object> columns = nextRow.value.getColumns();
          assertEquals(2, columns.size());
          log.debug("pageview from region 0: {}", nextRow.value.getColumns());
          actualPages.add((String) columns.get(1));
        }
      } catch (Exception e) {
        log.warn("Failed to read data from " + outputStream, e);
      }
      return 1 <= actualPages.size();
    }, 30000, "Could not read data from " + outputStream + " for 30 seconds");

    assertEquals(expectedPages, actualPages);
    queryMetadata.getKafkaStreams().close();
  }

  private String createStreamUsingLikeClause(String inputStream) throws Exception {
    String outputStream = "pageviews_female_like_r0";
    String createStatement = String.format("CREATE STREAM %s WITH (kafka_topic='pageviews_enriched_r0', " +
            "value_format='DELIMITED') AS SELECT * FROM %s WHERE regionid LIKE '%%_0';", outputStream, inputStream);

    List<QueryMetadata> queryMetadata = ksqlEngine.buildMultipleQueries(createStatement, Collections.emptyMap());
    assertEquals(1, queryMetadata.size());
    queryMetadata.get(0).getKafkaStreams().start();
    return outputStream;
  }

  private PersistentQueryMetadata createPageViewsFemaleStream(String streamName) throws Exception {
    String createStreamStatement = String.format("CREATE STREAM %s AS SELECT %s.userid AS userid, " +
                    "pageid, regionid, gender FROM %s LEFT JOIN %s ON " +
                    "%s.userid = %s.userid WHERE gender = 'FEMALE';", streamName,
            userTable, pageViewStream, userTable, pageViewStream, userTable);

    log.debug("Creating {} using: {}", streamName, createStreamStatement);

    List<QueryMetadata> queries = ksqlEngine.buildMultipleQueries(createStreamStatement,
            Collections.emptyMap());

    assertEquals(1, queries.size());
    assertTrue(queries.get(0) instanceof PersistentQueryMetadata);

    PersistentQueryMetadata persistentQueryMetadata = (PersistentQueryMetadata) queries.get(0);
    persistentQueryMetadata.getKafkaStreams().start();
    return persistentQueryMetadata;

  }
}
