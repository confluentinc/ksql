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
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.testutils.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.testutils.secure.ClientTrustStore;
import io.confluent.ksql.testutils.secure.SecureKafkaHelper;
import io.confluent.ksql.util.KafkaTopicClientImpl;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.OrderDataProvider;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.TopicProducer;

import static io.confluent.ksql.testutils.EmbeddedSingleNodeKafkaCluster.VALID_USER1;

/**
 * Tests covering integration with secured components, e.g. secure Kafka cluster.
 */
public class SecureIntegrationTest {

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster SECURE_CLUSTER =
      EmbeddedSingleNodeKafkaCluster.newBuilder()
          .withSaslSslListenersOnly()
          .build();

  private static final String INPUT_TOPIC = "orders_topic";
  private static final String INPUT_STREAM = "ORDERS";

  private QueryId queryId;
  private KsqlEngine ksqlEngine;
  private TopicProducer topicProducer;
  private KafkaTopicClientImpl topicClient;

  @Before
  public void before() {
    topicProducer = new TopicProducer(SECURE_CLUSTER);
  }

  @After
  public void after() {
    if (queryId != null) {
      ksqlEngine.terminateQuery(queryId, true);
    }
    ksqlEngine.close();
    topicClient.close();
  }

  @Test
  public void shouldRunQueryOnSecureKafkaCluster() throws Exception {
    // Given:
    final Map<String, Object> configs = getBaseKsqlConfig();

    // Additional Properties required for KSQL to talk to secure cluster:
    configs.put("security.protocol", "SASL_SSL");
    configs.put("sasl.mechanism", "PLAIN");
    configs.put("sasl.jaas.config", SecureKafkaHelper.buildJaasConfig(VALID_USER1));

    givenTestSetupWithConfig(configs);

    // Then:
    assertCanRunKsqlQuery();
  }

  private void givenTestSetupWithConfig(final Map<String, Object> ksqlConfigs) throws Exception {
    // Additional Properties required for KSQL to talk to test secure cluster,
    // where SSL cert not properly signed. (Not required for proper cluster).
    ksqlConfigs.putAll(ClientTrustStore.trustStoreProps());

    final KsqlConfig ksqlConfig = new KsqlConfig(ksqlConfigs);

    topicClient = new KafkaTopicClientImpl(
            AdminClient.create(ksqlConfig.getKsqlAdminClientConfigProps()));

    ksqlEngine = new KsqlEngine(ksqlConfig, topicClient);

    produceInitData();
    execInitCreateStreamQueries();
  }

  private void assertCanRunKsqlQuery() throws Exception {
    final String queryString = String.format("CREATE STREAM TEST_1 AS SELECT * "
                                             + "FROM %s;", INPUT_STREAM);
    executePersistentQuery(queryString);

    TestUtils.waitForCondition(
        () -> topicClient.isTopicExists("TEST_1"),
        "Wait for async topic creation"
    );
  }

  private Map<String, Object> getBaseKsqlConfig() {
    final Map<String, Object> configs = new HashMap<>();
    configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SECURE_CLUSTER.bootstrapServers());
    configs.put("application.id", "KSQL");
    configs.put("commit.interval.ms", 0);
    configs.put("cache.max.bytes.buffering", 0);
    configs.put("auto.offset.reset", "earliest");
    return configs;
  }

  private void produceInitData() throws Exception {
    topicClient.createTopic(INPUT_TOPIC, 1, (short) 1);

    final OrderDataProvider orderDataProvider = new OrderDataProvider();

    topicProducer
        .produceInputData(INPUT_TOPIC, orderDataProvider.data(), orderDataProvider.schema());
  }

  private void execInitCreateStreamQueries() throws Exception {
    String ordersStreamStr = String.format("CREATE STREAM %s (ORDERTIME bigint, ORDERID varchar, "
                                           + "ITEMID varchar, ORDERUNITS double, PRICEARRAY array<double>, KEYVALUEMAP "
                                           + "map<varchar, double>) WITH (value_format = 'json', "
                                           + "kafka_topic='%s' , "
                                           + "key='ordertime');", INPUT_STREAM, INPUT_TOPIC);

    ksqlEngine.buildMultipleQueries(ordersStreamStr, Collections.emptyMap());
  }

  private void executePersistentQuery(final String queryString) throws Exception {
    final QueryMetadata queryMetadata = ksqlEngine
        .buildMultipleQueries(queryString, Collections.emptyMap()).get(0);

    queryMetadata.getKafkaStreams().start();
    queryId = ((PersistentQueryMetadata) queryMetadata).getId();
  }
}