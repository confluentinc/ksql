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

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlContext;
import io.confluent.ksql.util.PageViewDataProvider;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.QueuedQueryMetadata;
import io.confluent.ksql.util.UserDataProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.test.IntegrationTest;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.testng.Assert.assertNotNull;

/**
 * This test emulates the end to end flow in the quick start guide and ensures that the outputs at each stage
 * are what we expect. This tests a broad set of KSQL functionality and is a good catch-all.
 */
@Category({IntegrationTest.class})
public class EndToEndIntegrationTest {
  private static final Logger log = LoggerFactory.getLogger(EndToEndIntegrationTest.class);
  private IntegrationTestHarness testHarness;
  private KsqlContext ksqlContext;

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
    Map<String, Object> ksqlConfig = testHarness.ksqlConfig.getKsqlStreamConfigProps();
    ksqlConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    ksqlContext = KsqlContext.create(ksqlConfig);

    testHarness.createTopic(pageViewTopic);
    testHarness.createTopic(usersTopic);

    pageViewDataProvider = new PageViewDataProvider();
    userDataProvider = new UserDataProvider();

    testHarness.publishTestData(usersTopic, userDataProvider, System.currentTimeMillis() - 10000);
    testHarness.publishTestData(pageViewTopic, pageViewDataProvider, System.currentTimeMillis());

    ksqlContext.sql(String.format("CREATE TABLE %s (registertime bigint, gender varchar, regionid varchar, " +
            "userid varchar) WITH (kafka_topic='%s', value_format='JSON');", userTable, usersTopic));

    ksqlContext.sql(String.format("CREATE STREAM %s (viewtime bigint, userid varchar, pageid varchar) " +
            "WITH (kafka_topic='%s', value_format='JSON');", pageViewStream, pageViewTopic));

  }

  @After
  public void after() throws Exception {
    ksqlContext.close();
    testHarness.stop();
  }

  @Test
  public void testKSQLFromEndToEnd() throws Exception {
    validateSelectFromPageViewsWithSpecificColumn();
    validateSelectAllFromUsers();
    String derivedStream = validateSelectAllFromDerivedStream();
    validateCreateStreamUsingLikeClause(derivedStream);
  }

  private void validateSelectAllFromUsers() throws Exception {
    String query = String.format("SELECT * from %s;", userTable);
    log.debug("Sending query: {}", query);

    List<QueryMetadata> queries = ksqlContext.getKsqlEngine().buildMultipleQueries(false, query, Collections.emptyMap());

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

    List<QueryMetadata> queries = ksqlContext.getKsqlEngine().buildMultipleQueries(false, query, Collections.emptyMap());

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

    List<QueryMetadata> queries = ksqlContext.getKsqlEngine().buildMultipleQueries(false, selectQuery, Collections.emptyMap());

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

    while (results.size() < 3) {
      KeyValue<String, GenericRow> nextRow = rowQueue.poll();
      if (nextRow != null) {
        results.add(nextRow);
      }
    }

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
    List<QueryMetadata> queries = ksqlContext.getKsqlEngine().buildMultipleQueries(false, selectPageViewsFromRegion,
            Collections.emptyMap());

    assertEquals(1, queries.size());
    assertTrue(queries.get(0) instanceof QueuedQueryMetadata);

    QueuedQueryMetadata queryMetadata = (QueuedQueryMetadata) queries.get(0);
    queryMetadata.getKafkaStreams().start();

    BlockingQueue<KeyValue<String, GenericRow>> rowQueue = queryMetadata.getRowQueue();

    List<String> actualPages = new ArrayList<>();
    List<String> expectedPages = Arrays.asList("PAGE_5");
    while (actualPages.size() < 1) {
      KeyValue<String, GenericRow> nextRow = rowQueue.poll();
      if (nextRow != null) {
        List<Object> columns = nextRow.value.getColumns();
        assertEquals(2, columns.size());
        log.debug("pageview from region 0: {}", nextRow.value.getColumns());
        actualPages.add((String) columns.get(1));
      }
    }

    assertEquals(expectedPages, actualPages);
    queryMetadata.getKafkaStreams().close();
  }

  private String createStreamUsingLikeClause(String inputStream) throws Exception {
    String outputStream = "pageviews_female_like_r0";
    String createStatement = String.format("CREATE STREAM %s WITH (kafka_topic='pageviews_enriched_r0', " +
            "value_format='DELIMITED') AS SELECT * FROM %s WHERE regionid LIKE '%%_0';", outputStream, inputStream);

    ksqlContext.sql(createStatement);
    return outputStream;
  }

  private PersistentQueryMetadata createPageViewsFemaleStream(String streamName) throws Exception {
    String createStreamStatement = String.format("CREATE STREAM %s AS SELECT %s.userid AS userid, " +
                    "pageid, regionid, gender FROM %s LEFT JOIN %s ON " +
                    "%s.userid = %s.userid WHERE gender = 'FEMALE';", streamName,
            userTable, pageViewStream, userTable, pageViewStream, userTable);

    log.debug("Creating {} using: {}", streamName, createStreamStatement);

    List<QueryMetadata> queries = ksqlContext.getKsqlEngine().buildMultipleQueries(false, createStreamStatement,
            Collections.emptyMap());

    assertEquals(1, queries.size());
    assertTrue(queries.get(0) instanceof PersistentQueryMetadata);

    PersistentQueryMetadata persistentQueryMetadata = (PersistentQueryMetadata) queries.get(0);
    persistentQueryMetadata.getKafkaStreams().start();
    return persistentQueryMetadata;

  }
}
