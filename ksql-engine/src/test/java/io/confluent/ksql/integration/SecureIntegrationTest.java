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

import com.google.common.collect.ImmutableSet;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.testutils.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.testutils.secure.ClientTrustStore;
import io.confluent.ksql.testutils.secure.Credentials;
import io.confluent.ksql.testutils.secure.SecureKafkaHelper;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KafkaTopicClientImpl;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.OrderDataProvider;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.TopicConsumer;
import io.confluent.ksql.util.TopicProducer;

import static io.confluent.ksql.testutils.EmbeddedSingleNodeKafkaCluster.VALID_USER1;
import static io.confluent.ksql.testutils.EmbeddedSingleNodeKafkaCluster.VALID_USER2;

/**
 * Tests covering integration with secured components, e.g. secure Kafka cluster.
 */
public class SecureIntegrationTest {

  private static final String INPUT_TOPIC = "orders_topic";
  private static final String INPUT_STREAM = "ORDERS";
  private static final Credentials SUPER_USER = VALID_USER1;
  private static final Credentials NORMAL_USER = VALID_USER2;
  private static final AtomicInteger COUNTER = new AtomicInteger(0);

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster SECURE_CLUSTER =
      EmbeddedSingleNodeKafkaCluster.newBuilder()
          .withSaslSslListenersOnly()
          .withAcls(SUPER_USER.username)
          .build();

  private QueryId queryId;
  private KsqlEngine ksqlEngine;
  private TopicProducer topicProducer;
  private KafkaTopicClient topicClient;
  private String outputTopic;

  @Before
  public void before() {
    SECURE_CLUSTER.clearAcls();
    topicProducer = new TopicProducer(SECURE_CLUSTER);
    outputTopic = "TEST_" + COUNTER.incrementAndGet();

    topicClient = new KafkaTopicClientImpl(AdminClient.create(
        new KsqlConfig(getKsqlConfig(SUPER_USER)).getKsqlAdminClientConfigProps()));
  }

  @After
  public void after() {
    if (queryId != null) {
      ksqlEngine.terminateQuery(queryId, true);
    }
    if (ksqlEngine != null) {
      ksqlEngine.close();
    }
    if (topicClient != null) {
      try {
        topicClient.deleteTopics(Collections.singletonList(outputTopic));
      } catch (final Exception e){
        e.printStackTrace(System.err);
      }
      topicClient.close();
    }
  }

  @Test
  public void shouldRunQueryAgainstKafkaClusterOverSaslSsl() throws Exception {
    // Given:
    final Map<String, Object> configs = getBaseKsqlConfig();

    // Additional Properties required for KSQL to talk to secure cluster:
    configs.put("security.protocol", "SASL_SSL");
    configs.put("sasl.mechanism", "PLAIN");
    configs.put("sasl.jaas.config", SecureKafkaHelper.buildJaasConfig(SUPER_USER));

    givenTestSetupWithConfig(configs);

    // Then:
    assertCanRunSimpleKsqlQuery();
  }

  @Test
  public void shouldRunQuerySimpleQueryAgainstKafkaClusterWithAcls() throws Exception {
    // Given:
    outputTopic = "SIMPLE_ACLS_TEST";

    givenAllowAcl(NORMAL_USER, ResourceType.CLUSTER, "kafka-cluster",
                  ImmutableSet.of(AclOperation.DESCRIBE_CONFIGS, AclOperation.CREATE));

    givenAllowAcl(NORMAL_USER, ResourceType.TOPIC, INPUT_TOPIC,
                  ImmutableSet.of(AclOperation.DESCRIBE, AclOperation.READ));

    givenAllowAcl(NORMAL_USER, ResourceType.TOPIC, outputTopic,
                  ImmutableSet.of(AclOperation.DESCRIBE, AclOperation.WRITE));

    givenAllowAcl(NORMAL_USER, ResourceType.TOPIC, "__consumer_offsets",
                  ImmutableSet.of(AclOperation.DESCRIBE));

    givenAllowAcl(NORMAL_USER, ResourceType.GROUP, "ksql_query_CSAS_SIMPLE_ACLS_TEST",
                  ImmutableSet.of(AclOperation.DESCRIBE, AclOperation.READ));

    givenTestSetupWithConfig(getKsqlConfig(NORMAL_USER));

    // Then:
    assertCanRunSimpleKsqlQuery();
  }

  @Test
  public void shouldRunQueryWithChangeLogsAgainstKafkaClusterWithWildcardAcls() throws Exception {
    // Given:
    givenAllowAcl(NORMAL_USER, ResourceType.CLUSTER, "kafka-cluster",
                  ImmutableSet.of(AclOperation.DESCRIBE_CONFIGS, AclOperation.CREATE));

    givenAllowAcl(NORMAL_USER, ResourceType.TOPIC, "*",
                  ImmutableSet.of(AclOperation.DESCRIBE, AclOperation.READ,
                                  AclOperation.WRITE, AclOperation.DELETE));

    givenAllowAcl(NORMAL_USER, ResourceType.GROUP, "*",
                  ImmutableSet.of(AclOperation.DESCRIBE, AclOperation.READ));

    givenTestSetupWithConfig(getKsqlConfig(NORMAL_USER));

    // Then:
    assertCanRunRepartitioningKsqlQuery();
  }

  @Test
  public void shouldRunQueryWithChangeLogsAgainstKafkaClusterWithAcls() throws Exception {
    // Given:
    outputTopic = "ACLS_TEST_2";

    givenAllowAcl(NORMAL_USER, ResourceType.CLUSTER, "kafka-cluster",
                  ImmutableSet.of(AclOperation.DESCRIBE_CONFIGS, AclOperation.CREATE));

    givenAllowAcl(NORMAL_USER, ResourceType.TOPIC, INPUT_TOPIC,
                  ImmutableSet.of(AclOperation.DESCRIBE, AclOperation.READ));

    givenAllowAcl(NORMAL_USER, ResourceType.TOPIC, "__consumer_offsets",
                  ImmutableSet.of(AclOperation.DESCRIBE));

    givenAllowAcl(NORMAL_USER, ResourceType.TOPIC, outputTopic,
                  ImmutableSet.of(AclOperation.DESCRIBE, AclOperation.WRITE));

    givenAllowAcl(NORMAL_USER, ResourceType.TOPIC,
                  "ksql_query_CTAS_ACLS_TEST_2-KSTREAM-AGGREGATE-STATE-STORE-0000000005-repartition",
                  ImmutableSet.of(AclOperation.DESCRIBE, AclOperation.READ, AclOperation.WRITE,
                                  AclOperation.DELETE));

    givenAllowAcl(NORMAL_USER, ResourceType.TOPIC,
                  "ksql_query_CTAS_ACLS_TEST_2-KSTREAM-AGGREGATE-STATE-STORE-0000000005-changelog",
                  ImmutableSet
                      .of(AclOperation.DESCRIBE, /* READ for recovery, */ AclOperation.WRITE,
                          AclOperation.DELETE));

    givenAllowAcl(NORMAL_USER, ResourceType.GROUP, "ksql_query_CTAS_ACLS_TEST_2",
                  ImmutableSet.of(AclOperation.DESCRIBE, AclOperation.READ));

    givenTestSetupWithConfig(getKsqlConfig(NORMAL_USER));

    // Then:
    assertCanRunRepartitioningKsqlQuery();
  }

  @Test
  public void shouldRunQueryWithChangeLogsAgainstKafkaClusterWithAclsWhereTopicsPreexist()
      throws Exception {
    // Given:
    outputTopic = "ACLS_TEST_3";

    final String repartitionTopic =
        "ksql_query_CTAS_ACLS_TEST_3-KSTREAM-AGGREGATE-STATE-STORE-0000000005-repartition";

    final String changeLogTopic =
        "ksql_query_CTAS_ACLS_TEST_3-KSTREAM-AGGREGATE-STATE-STORE-0000000005-changelog";

    SECURE_CLUSTER.createTopic(outputTopic, 4, 1);
    SECURE_CLUSTER.createTopic(repartitionTopic, 1, 1);
    SECURE_CLUSTER.createTopic(changeLogTopic, 1, 1);

    givenAllowAcl(NORMAL_USER, ResourceType.CLUSTER, "kafka-cluster",
                  ImmutableSet.of(AclOperation.DESCRIBE_CONFIGS));

    givenAllowAcl(NORMAL_USER, ResourceType.TOPIC, INPUT_TOPIC,
                  ImmutableSet.of(AclOperation.DESCRIBE, AclOperation.READ));

    givenAllowAcl(NORMAL_USER, ResourceType.TOPIC, "__consumer_offsets",
                  ImmutableSet.of(AclOperation.DESCRIBE));

    givenAllowAcl(NORMAL_USER, ResourceType.TOPIC, outputTopic,
                  ImmutableSet.of(AclOperation.DESCRIBE, AclOperation.WRITE));

    givenAllowAcl(NORMAL_USER, ResourceType.TOPIC,
                  repartitionTopic,
                  ImmutableSet.of(AclOperation.DESCRIBE, AclOperation.READ, AclOperation.WRITE));

    givenAllowAcl(NORMAL_USER, ResourceType.TOPIC,
                  changeLogTopic,
                  ImmutableSet
                      .of(AclOperation.DESCRIBE, /* READ for recovery, */ AclOperation.WRITE));

    givenAllowAcl(NORMAL_USER, ResourceType.GROUP, "ksql_query_CTAS_ACLS_TEST_3",
                  ImmutableSet.of(AclOperation.DESCRIBE, AclOperation.READ));

    givenTestSetupWithConfig(getKsqlConfig(NORMAL_USER));

    // Then:
    assertCanRunRepartitioningKsqlQuery();
  }

  @Test
  public void shouldRunQueryWithChangeLogsAgainstKafkaClusterWithAclsAndCustomPrefixed() throws Exception {
    // Given:
    outputTopic = "ACLS_TEST_4";

    givenAllowAcl(NORMAL_USER, ResourceType.CLUSTER, "kafka-cluster",
                  ImmutableSet.of(AclOperation.DESCRIBE_CONFIGS, AclOperation.CREATE));

    givenAllowAcl(NORMAL_USER, ResourceType.TOPIC, INPUT_TOPIC,
                  ImmutableSet.of(AclOperation.DESCRIBE, AclOperation.READ));

    givenAllowAcl(NORMAL_USER, ResourceType.TOPIC, "__consumer_offsets",
                  ImmutableSet.of(AclOperation.DESCRIBE));

    givenAllowAcl(NORMAL_USER, ResourceType.TOPIC, outputTopic,
                  ImmutableSet.of(AclOperation.DESCRIBE, AclOperation.WRITE));

    givenAllowAcl(NORMAL_USER, ResourceType.TOPIC,
                  "t4_query_CTAS_ACLS_TEST_4-KSTREAM-AGGREGATE-STATE-STORE-0000000005-repartition",
                  ImmutableSet.of(AclOperation.DESCRIBE, AclOperation.READ, AclOperation.WRITE,
                                  AclOperation.DELETE));

    givenAllowAcl(NORMAL_USER, ResourceType.TOPIC,
                  "t4_query_CTAS_ACLS_TEST_4-KSTREAM-AGGREGATE-STATE-STORE-0000000005-changelog",
                  ImmutableSet
                      .of(AclOperation.DESCRIBE, /* READ for recovery, */ AclOperation.WRITE,
                          AclOperation.DELETE));

    givenAllowAcl(NORMAL_USER, ResourceType.GROUP, "t4_query_CTAS_ACLS_TEST_4",
                  ImmutableSet.of(AclOperation.DESCRIBE, AclOperation.READ));

    final Map<String, Object> ksqlConfig = getKsqlConfig(NORMAL_USER);
    ksqlConfig.put(KsqlConfig.KSQL_SERVICE_ID_CONFIG, "t4_");
    givenTestSetupWithConfig(ksqlConfig);

    // Then:
    assertCanRunRepartitioningKsqlQuery();
  }

  private void givenAllowAcl(final Credentials user,
                             final ResourceType resourceType,
                             final String resourceName,
                             final Set<AclOperation> ops) {
    SECURE_CLUSTER.addUserAcl(user, AclPermissionType.ALLOW,
                              new Resource(resourceType, resourceName), ops);
  }

  private void givenTestSetupWithConfig(final Map<String, Object> ksqlConfigs) throws Exception {
    final KsqlConfig ksqlConfig = new KsqlConfig(ksqlConfigs);

    ksqlEngine = new KsqlEngine(ksqlConfig, new KafkaTopicClientImpl(
        AdminClient.create(ksqlConfig.getKsqlAdminClientConfigProps())));

    produceInitData();
    execInitCreateStreamQueries();
  }

  private void assertCanRunSimpleKsqlQuery() throws Exception {
    assertCanRunKsqlQuery("CREATE STREAM %s AS SELECT * FROM %s;",
                          outputTopic, INPUT_STREAM);
  }

  private void assertCanRunRepartitioningKsqlQuery() throws Exception {
    assertCanRunKsqlQuery("CREATE STREAM %s AS SELECT itemid, count(*) "
                          + "FROM %s WINDOW TUMBLING (size 5 second) GROUP BY itemid;",
                          outputTopic, INPUT_STREAM);
  }

  private void assertCanRunKsqlQuery(final String queryString,
                                     final Object... args) throws Exception {
    executePersistentQuery(queryString, args);

    TestUtils.waitForCondition(
        () -> topicClient.isTopicExists(this.outputTopic),
        "Wait for async topic creation"
    );

    final Schema schema = SchemaUtil.removeImplicitRowTimeRowKeyFromSchema(
        ksqlEngine.getMetaStore().getSource(outputTopic).getSchema());

    final TopicConsumer consumer = new TopicConsumer(SECURE_CLUSTER);
    consumer.readResults(outputTopic, schema, 1, new ByteArrayDeserializer());
  }

  private Map<String, Object> getBaseKsqlConfig() {
    final Map<String, Object> configs = new HashMap<>();
    configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SECURE_CLUSTER.bootstrapServers());
    configs.put("application.id", "KSQL");
    configs.put("commit.interval.ms", 0);
    configs.put("cache.max.bytes.buffering", 0);
    configs.put("auto.offset.reset", "earliest");

    // Additional Properties required for KSQL to talk to test secure cluster,
    // where SSL cert not properly signed. (Not required for proper cluster).
    configs.putAll(ClientTrustStore.trustStoreProps());
    return configs;
  }

  private Map<String, Object> getKsqlConfig(final Credentials user) {
    final Map<String, Object> configs = getBaseKsqlConfig();
    configs.putAll(SecureKafkaHelper.getSecureCredentialsConfig(user));
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

  private void executePersistentQuery(final String queryString,
                                      final Object... params) throws Exception {
    final String query = String.format(queryString, params);

    final QueryMetadata queryMetadata = ksqlEngine
        .buildMultipleQueries(query, Collections.emptyMap()).get(0);

    queryMetadata.getKafkaStreams().start();
    queryId = ((PersistentQueryMetadata) queryMetadata).getId();
  }
}