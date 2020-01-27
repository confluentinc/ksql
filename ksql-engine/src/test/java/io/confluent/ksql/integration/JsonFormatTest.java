/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.integration;

import static io.confluent.ksql.serde.Format.JSON;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlConfigTestUtil;
import io.confluent.ksql.ServiceInfo;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.KsqlEngineTestUtil;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.query.id.SequentialQueryIdGenerator;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.services.DisabledKsqlClient;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.ServiceContextFactory;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.OrderDataProvider;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import kafka.zookeeper.ZooKeeperClientException;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

@Category({IntegrationTest.class})
public class JsonFormatTest {
  private static final String inputTopic = "orders_topic";
  private static final String inputStream = "ORDERS";
  private static final String usersTopic = "users_topic";
  private static final String usersTable = "USERS";
  private static final String messageLogTopic = "log_topic";
  private static final String messageLogStream = "message_log";
  private static final AtomicInteger COUNTER = new AtomicInteger();

  public static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  @ClassRule
  public static final RuleChain CLUSTER_WITH_RETRY = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS);

  private MetaStore metaStore;
  private KsqlConfig ksqlConfig;
  private KsqlEngine ksqlEngine;
  private ServiceContext serviceContext;

  private QueryId queryId;
  private KafkaTopicClient topicClient;
  private String streamName;

  @Before
  public void before() {
    streamName = "STREAM_" + COUNTER.getAndIncrement();

    ksqlConfig = KsqlConfigTestUtil.create(TEST_HARNESS.kafkaBootstrapServers());
    serviceContext = ServiceContextFactory.create(ksqlConfig, DisabledKsqlClient::instance);

    ksqlEngine = new KsqlEngine(
        serviceContext,
        ProcessingLogContext.create(),
        new InternalFunctionRegistry(),
        ServiceInfo.create(ksqlConfig),
        new SequentialQueryIdGenerator());

    topicClient = serviceContext.getTopicClient();
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

  private static void produceInitData() {
    final OrderDataProvider orderDataProvider = new OrderDataProvider();

    TEST_HARNESS.produceRows(inputTopic, orderDataProvider, JSON);

    final LogicalSchema messageSchema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("MESSAGE"), SqlTypes.STRING)
        .build();

    final GenericRow messageRow = new GenericRow(Collections.singletonList(
        "{\"log\":{\"@timestamp\":\"2017-05-30T16:44:22.175Z\",\"@version\":\"1\","
        + "\"caasVersion\":\"0.0.2\",\"cloud\":\"aws\",\"logs\":[{\"entry\":\"first\"}],\"clusterId\":\"cp99\",\"clusterName\":\"kafka\",\"cpComponentId\":\"kafka\",\"host\":\"kafka-1-wwl0p\",\"k8sId\":\"k8s13\",\"k8sName\":\"perf\",\"level\":\"ERROR\",\"logger\":\"kafka.server.ReplicaFetcherThread\",\"message\":\"Found invalid messages during fetch for partition [foo512,172] offset 0 error Record is corrupt (stored crc = 1321230880, computed crc = 1139143803)\",\"networkId\":\"vpc-d8c7a9bf\",\"region\":\"us-west-2\",\"serverId\":\"1\",\"skuId\":\"sku5\",\"source\":\"kafka\",\"tenantId\":\"t47\",\"tenantName\":\"perf-test\",\"thread\":\"ReplicaFetcherThread-0-2\",\"zone\":\"us-west-2a\"},\"stream\":\"stdout\",\"time\":2017}"));

    final Map<String, GenericRow> records = new HashMap<>();
    records.put("1", messageRow);

    final PhysicalSchema schema = PhysicalSchema.from(
        messageSchema,
        SerdeOption.none()
    );

    TEST_HARNESS.produceRows(messageLogTopic, records, schema, JSON);
  }

  private void execInitCreateStreamQueries() {
    final String ordersStreamStr = String.format("CREATE STREAM %s ("
        + "ROWKEY BIGINT KEY, ORDERTIME bigint, ORDERID varchar, "
        + "ITEMID varchar, ORDERUNITS double, PRICEARRAY array<double>, KEYVALUEMAP "
        + "map<varchar, double>) WITH (value_format = 'json', "
        + "kafka_topic='%s' , "
        + "key='ordertime');", inputStream, inputTopic);

    final String usersTableStr = String.format("CREATE TABLE %s (userid varchar, age integer) WITH "
                                         + "(value_format = 'json', kafka_topic='%s', "
                                         + "KEY='userid');",
                                         usersTable, usersTopic);

    final String messageStreamStr = String.format("CREATE STREAM %s (message varchar) WITH (value_format = 'json', "
        + "kafka_topic='%s');", messageLogStream, messageLogTopic);

    KsqlEngineTestUtil.execute(
        serviceContext, ksqlEngine, ordersStreamStr, ksqlConfig, Collections.emptyMap());
    KsqlEngineTestUtil.execute(
        serviceContext, ksqlEngine, usersTableStr, ksqlConfig, Collections.emptyMap());
    KsqlEngineTestUtil.execute(
        serviceContext, ksqlEngine, messageStreamStr, ksqlConfig, Collections.emptyMap());
  }

  @After
  public void after() {
    terminateQuery();
    ksqlEngine.close();
    serviceContext.close();
  }

  //@Test
  public void testSelectDateTimeUDFs() {
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

    final Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("8", new GenericRow(Arrays.asList(1500962514814L,
        "2017-07-24 23:01:54.814",
        1500962514814L)));

    final Map<String, GenericRow> results = readNormalResults(streamName, expectedResults.size());

    assertThat(results, equalTo(expectedResults));
  }

  @Test
  public void testJsonStreamExtractor() {
    final String queryString = String.format("CREATE STREAM %s AS SELECT EXTRACTJSONFIELD"
            + "(message, '$.log.cloud') "
            + "FROM %s;",
        streamName, messageLogStream);

    executePersistentQuery(queryString);

    final Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("1", new GenericRow(Collections.singletonList("aws")));

    final Map<String, GenericRow> results = readNormalResults(streamName, expectedResults.size());

    assertThat(results, equalTo(expectedResults));
  }

  @Test
  public void testJsonStreamExtractorNested() {
    final String queryString = String.format("CREATE STREAM %s AS SELECT EXTRACTJSONFIELD"
                    + "(message, '$.log.logs[0].entry') "
                    + "FROM %s;",
            streamName, messageLogStream);

    executePersistentQuery(queryString);

    final Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("1", new GenericRow(Collections.singletonList("first")));

    final Map<String, GenericRow> results = readNormalResults(streamName, expectedResults.size());

    assertThat(results, equalTo(expectedResults));
  }

  private void executePersistentQuery(final String queryString) {
    final QueryMetadata queryMetadata = KsqlEngineTestUtil
        .execute(serviceContext, ksqlEngine, queryString, ksqlConfig, Collections.emptyMap())
        .get(0);

    queryMetadata.start();
    queryId = ((PersistentQueryMetadata)queryMetadata).getQueryId();
  }

  private Map<String, GenericRow> readNormalResults(
      final String resultTopic,
      final int expectedNumMessages
  ) {
    final DataSource source = metaStore.getSource(SourceName.of(streamName));

    final PhysicalSchema resultSchema = PhysicalSchema.from(
        source.getSchema(),
        source.getSerdeOptions()
    );

    return TEST_HARNESS
        .verifyAvailableUniqueRows(resultTopic, expectedNumMessages, JSON, resultSchema);
  }

  private void terminateQuery() {
    ksqlEngine.getPersistentQuery(queryId)
        .ifPresent(QueryMetadata::close);
  }
}
