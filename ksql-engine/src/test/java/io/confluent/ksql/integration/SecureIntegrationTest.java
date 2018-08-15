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

import static io.confluent.ksql.testutils.AssertEventually.assertThatEventually;
import static io.confluent.ksql.testutils.EmbeddedSingleNodeKafkaCluster.VALID_USER1;
import static io.confluent.ksql.testutils.EmbeddedSingleNodeKafkaCluster.VALID_USER2;
import static org.apache.kafka.common.acl.AclOperation.ALL;
import static org.apache.kafka.common.acl.AclOperation.CREATE;
import static org.apache.kafka.common.acl.AclOperation.DELETE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE_CONFIGS;
import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclOperation.WRITE;
import static org.apache.kafka.common.resource.ResourceType.CLUSTER;
import static org.apache.kafka.common.resource.ResourceType.GROUP;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

import io.confluent.common.utils.IntegrationTest;
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
import io.confluent.ksql.util.TopicConsumer;
import io.confluent.ksql.util.TopicProducer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import kafka.security.auth.Acl;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests covering integration with secured components, e.g. secure Kafka cluster.
 */
@Category({IntegrationTest.class})
public class SecureIntegrationTest {

  private static final String INPUT_TOPIC = "orders_topic";
  private static final String INPUT_STREAM = "ORDERS";
  private static final Credentials ALL_USERS = new Credentials("*", "ignored");
  private static final Credentials SUPER_USER = VALID_USER1;
  private static final Credentials NORMAL_USER = VALID_USER2;
  private static final AtomicInteger COUNTER = new AtomicInteger(0);

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster SECURE_CLUSTER =
      EmbeddedSingleNodeKafkaCluster.newBuilder()
          .withoutPlainListeners()
          .withSaslSslListeners()
          .withSslListeners()
          .withAcls(SUPER_USER.username)
          .build();

  private QueryId queryId;
  private KsqlConfig ksqlConfig;
  private KsqlEngine ksqlEngine;
  private final TopicProducer topicProducer = new TopicProducer(SECURE_CLUSTER);
  private KafkaTopicClient topicClient;
  private String outputTopic;

  @Before
  public void before() throws Exception {
    SECURE_CLUSTER.clearAcls();
    outputTopic = "TEST_" + COUNTER.incrementAndGet();

    topicClient = new KafkaTopicClientImpl(
        new KsqlConfig(getKsqlConfig(SUPER_USER)).getKsqlAdminClientConfigProps());

    produceInitData();
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
      } catch (final Exception e) {
        e.printStackTrace(System.err);
      }
      topicClient.close();
    }
  }

  @Test
  public void shouldRunQueryAgainstKafkaClusterOverSsl() throws Exception {
    // Given:
    givenAllowAcl(ALL_USERS,
                  resource(CLUSTER, "kafka-cluster"),
                  ops(DESCRIBE_CONFIGS, CREATE));

    givenAllowAcl(ALL_USERS,
                  resource(TOPIC, Acl.WildCardResource()),
                  ops(DESCRIBE, READ, WRITE, DELETE));

    givenAllowAcl(ALL_USERS,
                  resource(GROUP, Acl.WildCardResource()),
                  ops(DESCRIBE, READ));

    final Map<String, Object> configs = getBaseKsqlConfig();
    configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                SECURE_CLUSTER.bootstrapServers(SecurityProtocol.SSL));

    // Additional Properties required for KSQL to talk to cluster over SSL:
    configs.put("security.protocol", "SSL");
    configs.put("ssl.truststore.location", ClientTrustStore.trustStorePath());
    configs.put("ssl.truststore.password", ClientTrustStore.trustStorePassword());

    givenTestSetupWithConfig(configs);

    // Then:
    assertCanRunSimpleKsqlQuery();
  }

  @Test
  public void shouldRunQueryAgainstKafkaClusterOverSaslSsl() throws Exception {
    // Given:
    final Map<String, Object> configs = getBaseKsqlConfig();

    // Additional Properties required for KSQL to talk to secure cluster using SSL and SASL:
    configs.put("security.protocol", "SASL_SSL");
    configs.put("sasl.mechanism", "PLAIN");
    configs.put("sasl.jaas.config", SecureKafkaHelper.buildJaasConfig(SUPER_USER));

    givenTestSetupWithConfig(configs);

    // Then:
    assertCanRunSimpleKsqlQuery();
  }

  @Test
  public void shouldRunQueriesRequiringChangeLogsAndRepartitionTopicsWithMinimalPrefixedAcls()
      throws Exception {

    final String serviceId = "my-service-id_";  // Defaults to "default_"

    final Map<String, Object> ksqlConfig = getKsqlConfig(NORMAL_USER);
    ksqlConfig.put(KsqlConfig.KSQL_SERVICE_ID_CONFIG, serviceId);

    givenAllowAcl(NORMAL_USER,
                  resource(CLUSTER, "kafka-cluster"),
                  ops(DESCRIBE_CONFIGS));

    givenAllowAcl(NORMAL_USER,
                  resource(TOPIC, INPUT_TOPIC),
                  ops(READ));

    givenAllowAcl(NORMAL_USER,
                  resource(TOPIC, outputTopic),
                  ops(CREATE /* as the topic doesn't exist yet*/, WRITE));

    givenAllowAcl(NORMAL_USER,
                  prefixedResource(TOPIC, "_confluent-ksql-my-service-id_"),
                  ops(ALL));

    givenAllowAcl(NORMAL_USER,
                  prefixedResource(GROUP, "_confluent-ksql-my-service-id_"),
                  ops(ALL));

    givenTestSetupWithConfig(ksqlConfig);

    // Then:
    assertCanRunRepartitioningKsqlQuery();
  }

  // Requires correctly configured schema-registry running
  //@Test
  @SuppressWarnings("unused")
  public void shouldRunQueryAgainstSecureSchemaRegistry() throws Exception {
    // Given:
    final HostnameVerifier existing = HttpsURLConnection.getDefaultHostnameVerifier();
    HttpsURLConnection.setDefaultHostnameVerifier(
        (hostname, sslSession) -> hostname.equals("localhost"));

    try {
      final Map<String, Object> ksqlConfig = getKsqlConfig(SUPER_USER);
      ksqlConfig.put("ksql.schema.registry.url", "https://localhost:8481");
      ksqlConfig.put("ssl.truststore.location", ClientTrustStore.trustStorePath());
      ksqlConfig.put("ssl.truststore.password", ClientTrustStore.trustStorePassword());
      givenTestSetupWithConfig(ksqlConfig);

      // Then:
      assertCanRunKsqlQuery("CREATE STREAM %s WITH (VALUE_FORMAT='AVRO') AS "
                            + "SELECT * FROM %s;",
                            outputTopic, INPUT_STREAM);
    } finally {
      HttpsURLConnection.setDefaultHostnameVerifier(existing);
    }
  }

  private static void givenAllowAcl(final Credentials credentials,
                                    final ResourcePattern resource,
                                    final Set<AclOperation> ops) {
    SECURE_CLUSTER.addUserAcl(credentials.username, AclPermissionType.ALLOW, resource, ops);
  }

  private static Set<AclOperation> ops(final AclOperation... ops) {
    return Arrays.stream(ops).collect(Collectors.toSet());
  }

  private static ResourcePattern resource(final ResourceType resourceType,
                                          final String resourceName) {
    return new ResourcePattern(resourceType, resourceName, PatternType.LITERAL);
  }

  private static ResourcePattern prefixedResource(final ResourceType resourceType,
                                                  final String resourceName) {
    return new ResourcePattern(resourceType, resourceName, PatternType.PREFIXED);
  }

  private void givenTestSetupWithConfig(final Map<String, Object> ksqlConfigs) {
    ksqlConfig = new KsqlConfig(ksqlConfigs);
    ksqlEngine = new KsqlEngine(ksqlConfig);

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

    final TopicConsumer consumer = new TopicConsumer(SECURE_CLUSTER);
    consumer.verifyRecordsReceived(outputTopic, greaterThan(0));
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
    if (topicClient.isTopicExists(INPUT_TOPIC)) {
      return;
    }

    topicClient.createTopic(INPUT_TOPIC, 1, (short) 1);

    awaitAsyncInputTopicCreation();

    final OrderDataProvider orderDataProvider = new OrderDataProvider();

    topicProducer
        .produceInputData(INPUT_TOPIC, orderDataProvider.data(), orderDataProvider.schema());
  }

  private void awaitAsyncInputTopicCreation() {
    assertThatEventually(() -> topicClient.isTopicExists(INPUT_TOPIC), is(true));
  }

  private void execInitCreateStreamQueries() {
    final String ordersStreamStr = String.format("CREATE STREAM %s (ORDERTIME bigint, ORDERID varchar, "
                                           + "ITEMID varchar, ORDERUNITS double, PRICEARRAY array<double>, KEYVALUEMAP "
                                           + "map<varchar, double>) WITH (value_format = 'json', "
                                           + "kafka_topic='%s' , "
                                           + "key='ordertime');", INPUT_STREAM, INPUT_TOPIC);

    ksqlEngine.buildMultipleQueries(
        ordersStreamStr, ksqlConfig, Collections.emptyMap());
  }

  private void executePersistentQuery(final String queryString,
                                      final Object... params) {
    final String query = String.format(queryString, params);

    final QueryMetadata queryMetadata = ksqlEngine
        .buildMultipleQueries(query, ksqlConfig, Collections.emptyMap()).get(0);

    queryMetadata.getKafkaStreams().start();
    queryId = ((PersistentQueryMetadata) queryMetadata).getQueryId();
  }
}
