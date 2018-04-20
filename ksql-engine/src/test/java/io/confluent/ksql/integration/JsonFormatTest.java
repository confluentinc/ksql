/*
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

import com.google.common.collect.ImmutableList;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.testutils.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KafkaTopicClientImpl;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.OrderDataProvider;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.TopicConsumer;
import io.confluent.ksql.util.TopicProducer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

@Category({IntegrationTest.class})
public class JsonFormatTest {
  private static final String inputTopic = "orders_topic";
  private static final String inputStream = "ORDERS";
  private static final String usersTopic = "users_topic";
  private static final String usersTable = "USERS";
  private static final String messageLogTopic = "log_topic";
  private static final String messageLogStream = "message_log";

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

  private MetaStore metaStore;
  private KsqlEngine ksqlEngine;
  private final TopicProducer topicProducer = new TopicProducer(CLUSTER);
  private final TopicConsumer topicConsumer = new TopicConsumer(CLUSTER);

  private AdminClient adminClient;
  private QueryId queryId;
  private KafkaTopicClient topicClient;

  @Before
  public void before() throws Exception {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
    configMap.put("application.id", "KSQL");
    configMap.put("commit.interval.ms", 0);
    configMap.put("cache.max.bytes.buffering", 0);
    configMap.put("auto.offset.reset", "earliest");

    KsqlConfig ksqlConfig = new KsqlConfig(configMap);
    adminClient = AdminClient.create(ksqlConfig.getKsqlAdminClientConfigProps());
    topicClient = new KafkaTopicClientImpl(adminClient);
    ksqlEngine = new KsqlEngine(ksqlConfig, topicClient);
    metaStore = ksqlEngine.getMetaStore();

    createInitTopics();
    produceInitData();
    execInitCreateStreamQueries();
  }

  private void createInitTopics() {
    topicClient.createTopic(inputTopic, 1, (short) 1);
    topicClient.createTopic(usersTopic, 1, (short) 1);
    topicClient.createTopic(messageLogTopic, 1, (short) 1);
  }

  private void produceInitData() throws Exception {
    OrderDataProvider orderDataProvider = new OrderDataProvider();

    topicProducer
            .produceInputData(inputTopic, orderDataProvider.data(), orderDataProvider.schema());

    Schema messageSchema = SchemaBuilder.struct().field("MESSAGE", SchemaBuilder.STRING_SCHEMA).build();

    GenericRow messageRow = new GenericRow(Collections.singletonList(
        "{\"log\":{\"@timestamp\":\"2017-05-30T16:44:22.175Z\",\"@version\":\"1\","
        + "\"caasVersion\":\"0.0.2\",\"cloud\":\"aws\",\"logs\":[{\"entry\":\"first\"}],\"clusterId\":\"cp99\",\"clusterName\":\"kafka\",\"cpComponentId\":\"kafka\",\"host\":\"kafka-1-wwl0p\",\"k8sId\":\"k8s13\",\"k8sName\":\"perf\",\"level\":\"ERROR\",\"logger\":\"kafka.server.ReplicaFetcherThread\",\"message\":\"Found invalid messages during fetch for partition [foo512,172] offset 0 error Record is corrupt (stored crc = 1321230880, computed crc = 1139143803)\",\"networkId\":\"vpc-d8c7a9bf\",\"region\":\"us-west-2\",\"serverId\":\"1\",\"skuId\":\"sku5\",\"source\":\"kafka\",\"tenantId\":\"t47\",\"tenantName\":\"perf-test\",\"thread\":\"ReplicaFetcherThread-0-2\",\"zone\":\"us-west-2a\"},\"stream\":\"stdout\",\"time\":2017}"));

    Map<String, GenericRow> records = new HashMap<>();
    records.put("1", messageRow);

    topicProducer.produceInputData(messageLogTopic, records, messageSchema);
  }

  private void execInitCreateStreamQueries() throws Exception {
    String ordersStreamStr = String.format("CREATE STREAM %s (ORDERTIME bigint, ORDERID varchar, "
        + "ITEMID varchar, ORDERUNITS double, PRICEARRAY array<double>, KEYVALUEMAP "
        + "map<varchar, double>) WITH (value_format = 'json', "
        + "kafka_topic='%s' , "
        + "key='ordertime');", inputStream, inputTopic);

    String usersTableStr = String.format("CREATE TABLE %s (userid varchar, age integer) WITH "
                                         + "(value_format = 'json', kafka_topic='%s', "
                                         + "KEY='userid');",
                                         usersTable, usersTopic);

    String messageStreamStr = String.format("CREATE STREAM %s (message varchar) WITH (value_format = 'json', "
        + "kafka_topic='%s');", messageLogStream, messageLogTopic);

    ksqlEngine.buildMultipleQueries(ordersStreamStr, Collections.emptyMap());
    ksqlEngine.buildMultipleQueries(usersTableStr, Collections.emptyMap());
    ksqlEngine.buildMultipleQueries(messageStreamStr, Collections.emptyMap());
  }

  @After
  public void after() {
    terminateQuery();
    ksqlEngine.close();
    adminClient.close();
  }

  //@Test
  public void testSelectDateTimeUDFs() throws Exception {
    final String streamName = "SelectDateTimeUDFsStream".toUpperCase();

    final String selectColumns =
        "(ORDERTIME+1500962514806) , TIMESTAMPTOSTRING(ORDERTIME+1500962514806, "
        + "'yyyy-MM-dd HH:mm:ss.SSS'), "
        + "STRINGTOTIMESTAMP"
            + "(TIMESTAMPTOSTRING"
            + "(ORDERTIME+1500962514806, 'yyyy-MM-dd HH:mm:ss.SSS'), 'yyyy-MM-dd HH:mm:ss.SSS')";
    final String whereClause = "ORDERUNITS > 20 AND ITEMID LIKE '%_8'";

    final String queryString = String.format(
        "CREATE STREAM %s AS SELECT %s FROM %s WHERE %s;",
        streamName,
        selectColumns,
        inputStream,
        whereClause
    );

    executePersistentQuery(queryString);

    Schema resultSchema = SchemaUtil
        .removeImplicitRowTimeRowKeyFromSchema(metaStore.getSource(streamName).getSchema());

    Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("8", new GenericRow(Arrays.asList(1500962514814L,
        "2017-07-24 23:01:54.814",
        1500962514814L)));

    Map<String, GenericRow> results = readNormalResults(streamName, resultSchema, expectedResults.size());

    assertThat(results, equalTo(expectedResults));
  }

  @Test
  public void testSinkProperties() throws Exception {
    final String streamName = "SinkPropertiesStream".toUpperCase();
    final int resultPartitionCount = 3;
    final String queryString = String.format("CREATE STREAM %s WITH (PARTITIONS = %d) AS SELECT * "
            + "FROM %s;",
        streamName, resultPartitionCount, inputStream);

    executePersistentQuery(queryString);

    TestUtils.waitForCondition(
        () -> topicClient.isTopicExists(streamName),
        "Wait for async topic creation"
    );

    assertThat(
        topicClient.describeTopics(ImmutableList.of(streamName)).get(streamName).partitions(),
        hasSize(3));
    assertThat(topicClient.getTopicCleanupPolicy(streamName), equalTo(
        KafkaTopicClient.TopicCleanupPolicy.DELETE));
  }

  @Test
  public void testTableSinkCleanupProperty() throws Exception {
    final String tableName = "SinkCleanupTable".toUpperCase();
    final int resultPartitionCount = 3;
    final String queryString = String.format("CREATE TABLE %s AS SELECT * "
                                             + "FROM %s;",
                                             tableName, usersTable);
    executePersistentQuery(queryString);

    TestUtils.waitForCondition(
        () -> topicClient.isTopicExists(tableName),
        "Wait for async topic creation"
    );

    assertThat(topicClient.getTopicCleanupPolicy(tableName), equalTo(
        KafkaTopicClient.TopicCleanupPolicy.COMPACT));
  }

  @Test
  public void testJsonStreamExtractor() throws Exception {

    final String streamName = "JSONSTREAM";
    final String queryString = String.format("CREATE STREAM %s AS SELECT EXTRACTJSONFIELD"
            + "(message, '$.log.cloud') "
            + "FROM %s;",
        streamName, messageLogStream);

    executePersistentQuery(queryString);

    Schema resultSchema = SchemaUtil
            .removeImplicitRowTimeRowKeyFromSchema(metaStore.getSource(streamName).getSchema());

    Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("1", new GenericRow(Collections.singletonList("aws")));

    Map<String, GenericRow> results = readNormalResults(streamName, resultSchema, expectedResults.size());

    assertThat(results, equalTo(expectedResults));
  }

  @Test
  public void testJsonStreamExtractorNested() throws Exception {

    final String streamName = "JSONSTREAM";
    final String queryString = String.format("CREATE STREAM %s AS SELECT EXTRACTJSONFIELD"
                    + "(message, '$.log.logs[0].entry') "
                    + "FROM %s;",
            streamName, messageLogStream);

    executePersistentQuery(queryString);

    Schema resultSchema = SchemaUtil
            .removeImplicitRowTimeRowKeyFromSchema(metaStore.getSource(streamName).getSchema());

    Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("1", new GenericRow(Collections.singletonList("first")));

    Map<String, GenericRow> results = readNormalResults(streamName, resultSchema, expectedResults.size());

    assertThat(results, equalTo(expectedResults));
  }

  private void executePersistentQuery(String queryString) throws Exception {
    final QueryMetadata queryMetadata = ksqlEngine
        .buildMultipleQueries(queryString, Collections.emptyMap()).get(0);

    queryMetadata.getKafkaStreams().start();
    queryId = ((PersistentQueryMetadata)queryMetadata).getQueryId();
  }

  private Map<String, GenericRow> readNormalResults(String resultTopic, Schema resultSchema, int expectedNumMessages) {
    return topicConsumer.readResults(resultTopic, resultSchema, expectedNumMessages, new StringDeserializer());
  }

  private void terminateQuery() {
    ksqlEngine.terminateQuery(queryId, true);
  }

}
