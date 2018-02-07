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
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.testutils.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.util.*;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class JsonFormatTest {

  private MetaStore metaStore;
  private KsqlEngine ksqlEngine;
  private TopicProducer topicProducer;
  private TopicConsumer topicConsumer;

  private Map<String, GenericRow> inputData;
  private Map<String, RecordMetadata> inputRecordsMetadata;

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

  private static final String inputTopic = "orders_topic";
  private static final String inputStream = "ORDERS";
  private static final String messageLogTopic = "log_topic";
  private static final String messageLogStream = "message_log";

  private static final Logger log = LoggerFactory.getLogger(JsonFormatTest.class);
  private AdminClient adminClient;
  private QueryId queryId;


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
    ksqlEngine = new KsqlEngine(ksqlConfig, new KafkaTopicClientImpl(adminClient));
    metaStore = ksqlEngine.getMetaStore();
    topicProducer = new TopicProducer(CLUSTER);
    topicConsumer = new TopicConsumer(CLUSTER);

    createInitTopics();
    produceInitData();
    execInitCreateStreamQueries();

  }

  private void createInitTopics() {
    ksqlEngine.getTopicClient().createTopic(inputTopic, 1, (short)1);
    ksqlEngine.getTopicClient().createTopic(messageLogTopic, 1, (short)1);
  }

  private void produceInitData() throws Exception {
    OrderDataProvider orderDataProvider = new OrderDataProvider();
    inputData = orderDataProvider.data();
    inputRecordsMetadata = topicProducer.produceInputData(inputTopic, orderDataProvider.data(), orderDataProvider.schema());

    Schema messageSchema = SchemaBuilder.struct().field("MESSAGE", SchemaBuilder.STRING_SCHEMA).build();

    GenericRow messageRow = new GenericRow(Arrays.asList
            ("{\"log\":{\"@timestamp\":\"2017-05-30T16:44:22.175Z\",\"@version\":\"1\","
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

    String messageStreamStr = String.format("CREATE STREAM %s (message varchar) WITH (value_format = 'json', "
        + "kafka_topic='%s');", messageLogStream, messageLogTopic);

    ksqlEngine.buildMultipleQueries(ordersStreamStr, Collections.emptyMap());
    ksqlEngine.buildMultipleQueries(messageStreamStr, Collections.emptyMap());
  }

  @After
  public void after() throws Exception {
    adminClient.close();
    ksqlEngine.close();
    terminateQuery();
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

    PersistentQueryMetadata queryMetadata =
        (PersistentQueryMetadata) ksqlEngine.buildMultipleQueries(queryString, Collections.emptyMap()).get(0);
    queryMetadata.getKafkaStreams().start();
    queryId = queryMetadata.getId();

    Schema resultSchema = SchemaUtil
        .removeImplicitRowTimeRowKeyFromSchema(metaStore.getSource(streamName).getSchema());

    Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("8", new GenericRow(Arrays.asList(1500962514814l,
        "2017-07-24 23:01:54.814",
        1500962514814l)));

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
    PersistentQueryMetadata queryMetadata =
            (PersistentQueryMetadata) ksqlEngine.buildMultipleQueries(queryString, Collections.emptyMap()).get(0);
    queryMetadata.getKafkaStreams().start();
    queryId = queryMetadata.getId();

    KafkaTopicClient kafkaTopicClient = ksqlEngine.getTopicClient();

    /*
     * It may take several seconds after AdminClient#createTopics returns
     * success for all the brokers to become aware that the topics have been created.
     * During this time, AdminClient#listTopics may not return information about the new topics.
     */
    log.info("Wait for the created topic to appear in the topic list...");
    Thread.sleep(2000);

    Assert.assertTrue(kafkaTopicClient.isTopicExists(streamName));
  }

  @Test
  public void testJsonStreamExtractor() throws Exception {

    final String streamName = "JSONSTREAM";
    final String queryString = String.format("CREATE STREAM %s AS SELECT EXTRACTJSONFIELD"
            + "(message, '$.log.cloud') "
            + "FROM %s;",
        streamName, messageLogStream);

    PersistentQueryMetadata queryMetadata =
        (PersistentQueryMetadata) ksqlEngine.buildMultipleQueries(queryString, Collections.emptyMap()).get(0);
    queryMetadata.getKafkaStreams().start();
    queryId = queryMetadata.getId();

    Schema resultSchema = SchemaUtil
            .removeImplicitRowTimeRowKeyFromSchema(metaStore.getSource(streamName).getSchema());

    Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("1", new GenericRow(Arrays.asList("aws")));

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

    PersistentQueryMetadata queryMetadata =
            (PersistentQueryMetadata) ksqlEngine.buildMultipleQueries(queryString, Collections.emptyMap()).get(0);
    queryMetadata.getKafkaStreams().start();
    queryId = queryMetadata.getId();

    Schema resultSchema = SchemaUtil
            .removeImplicitRowTimeRowKeyFromSchema(metaStore.getSource(streamName).getSchema());

    Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("1", new GenericRow(Arrays.asList("first")));

    Map<String, GenericRow> results = readNormalResults(streamName, resultSchema, expectedResults.size());

    assertThat(results, equalTo(expectedResults));
  }

  //*********************************************************//

  private Map<String, GenericRow> readNormalResults(String resultTopic, Schema resultSchema, int expectedNumMessages) {
    return topicConsumer.readResults(resultTopic, resultSchema, expectedNumMessages, new StringDeserializer());
  }

  private Map<Windowed<String>, GenericRow> readWindowedResults(
      String resultTopic,
      Schema resultSchema,
      int expectedNumMessages
  ) {
    Deserializer<Windowed<String>> keyDeserializer = new WindowedDeserializer<>(new StringDeserializer());
    return topicConsumer.readResults(resultTopic, resultSchema, expectedNumMessages, keyDeserializer);
  }

  private void terminateQuery() {
    ksqlEngine.terminateQuery(queryId, true);
  }

}