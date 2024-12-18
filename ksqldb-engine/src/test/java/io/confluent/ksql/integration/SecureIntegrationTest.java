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

import static io.confluent.ksql.function.UserFunctionLoaderTestUtil.loadAllUserFunctions;
import static io.confluent.ksql.serde.FormatFactory.JSON;
import static io.confluent.ksql.serde.FormatFactory.KAFKA;
import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster.VALID_USER1;
import static io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster.VALID_USER2;
import static io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster.ops;
import static io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster.prefixedResource;
import static io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster.resource;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_RETRY_BACKOFF_INITIAL_MS;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_RETRY_BACKOFF_MAX_MS;
import static io.confluent.ksql.util.KsqlConfig.KSQL_SERVICE_ID_CONFIG;
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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.KsqlConfigTestUtil;
import io.confluent.ksql.ServiceInfo;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.KsqlEngineTestUtil;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.function.MutableFunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.query.QueryError;
import io.confluent.ksql.query.QueryError.Type;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.query.id.SequentialQueryIdGenerator;
import io.confluent.ksql.services.DisabledKsqlClient;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.KafkaTopicClientImpl;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.ServiceContextFactory;
import io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.test.util.secure.ClientTrustStore;
import io.confluent.ksql.test.util.secure.Credentials;
import io.confluent.ksql.test.util.secure.SecureKafkaHelper;
import io.confluent.ksql.topic.TopicProperties;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.OrderDataProvider;
import io.confluent.ksql.util.QueryMetadata;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.MissingSourceTopicException;
import org.apache.kafka.streams.errors.StreamsException;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;

/**
 * Tests covering integration with secured components, e.g. secure Kafka cluster.
 */
@SuppressWarnings("SameParameterValue")
@Category({IntegrationTest.class})
public class SecureIntegrationTest {

  private static final String INPUT_TOPIC = "orders_topic";
  private static final String INPUT_STREAM = "ORDERS";
  private static final Credentials ALL_USERS = new Credentials("*", "ignored");
  private static final Credentials SUPER_USER = VALID_USER1;
  private static final Credentials NORMAL_USER = VALID_USER2;
  private static final AtomicInteger COUNTER = new AtomicInteger(0);
  private static final String SERVICE_ID = "my-service-id_";
  private static final String QUERY_ID_PREFIX = "_confluent-ksql-";

  public static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness
      .builder()
      .withKafkaCluster(
          EmbeddedSingleNodeKafkaCluster.newBuilder()
              .withoutPlainListeners()
              .withSaslSslListeners()
              .withSslListeners()
              .withAclsEnabled(SUPER_USER.username)
      )
      .build();

  @ClassRule
  public static final RuleChain CLUSTER_WITH_RETRY = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS);


  @Rule
  public final Timeout timeout = Timeout.seconds(90);

  private QueryId queryId;
  private KsqlConfig ksqlConfig;
  private KsqlEngine ksqlEngine;
  private KafkaTopicClient topicClient;
  private String outputTopic;
  private Admin adminClient;
  private ServiceContext serviceContext;

  @Before
  public void before() {
    TEST_HARNESS.getKafkaCluster().clearAcls();
    outputTopic = "TEST_" + COUNTER.incrementAndGet();

    adminClient = AdminClient.create(new KsqlConfig(getKsqlConfig(SUPER_USER))
        .getKsqlAdminClientConfigProps());
    topicClient = new KafkaTopicClientImpl(() -> adminClient);

    produceInitData();
  }

  @After
  public void after() {
    if (queryId != null) {
      ksqlEngine.getPersistentQuery(queryId)
          .ifPresent(QueryMetadata::close);
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
      adminClient.close();
    }
    if (serviceContext != null) {
      serviceContext.close();
    }
  }

  @Test
  public void shouldRunQueryAgainstKafkaClusterOverSsl() {
    // Given:
    givenAllowAcl(ALL_USERS,
                  resource(CLUSTER, "kafka-cluster"),
                  ops(DESCRIBE_CONFIGS, CREATE));

    givenAllowAcl(ALL_USERS,
                  resource(TOPIC, ResourcePattern.WILDCARD_RESOURCE),
                  ops(DESCRIBE, READ, WRITE, DELETE));

    givenAllowAcl(ALL_USERS,
                  resource(GROUP, ResourcePattern.WILDCARD_RESOURCE),
                  ops(DESCRIBE, READ));

    final Map<String, Object> configs = getBaseKsqlConfig();
    configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        TEST_HARNESS.getKafkaCluster().bootstrapServers(SecurityProtocol.SSL));

    // Additional Properties required for KSQL to talk to cluster over SSL:
    configs.put("security.protocol", "SSL");
    configs.put("ssl.truststore.location", ClientTrustStore.trustStorePath());
    configs.put("ssl.truststore.password", ClientTrustStore.trustStorePassword());

    givenTestSetupWithConfig(configs);

    // Then:
    assertCanRunSimpleKsqlQuery();
  }

  @Test
  public void shouldRunQueryAgainstKafkaClusterOverSaslSsl() {
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
  public void shouldWorkWithMinimalPrefixedAcls() {
    // Given:
    final Map<String, Object> ksqlConfig = getKsqlConfig(NORMAL_USER);
    ksqlConfig.put(KSQL_SERVICE_ID_CONFIG, SERVICE_ID);

    givenTestSetupWithAclsForQuery();
    givenAllowAcl(NORMAL_USER,
        resource(CLUSTER, "kafka-cluster"),
        ops(DESCRIBE_CONFIGS));
    givenTestSetupWithConfig(ksqlConfig);

    // Then:
    assertCanRunRepartitioningKsqlQuery();
    assertCanAccessClusterConfig();
  }

  @Test
  public void shouldClassifyMissingSourceTopicExceptionAsUserError() {
    // Given:
    final Map<String, Object> ksqlConfig = getKsqlConfig(NORMAL_USER);
    ksqlConfig.put(KSQL_SERVICE_ID_CONFIG, SERVICE_ID);
    ksqlConfig.put(KSQL_QUERY_RETRY_BACKOFF_INITIAL_MS, 0L);
    ksqlConfig.put(KSQL_QUERY_RETRY_BACKOFF_MAX_MS, 0L);
    ksqlConfig.put(KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED, false);

    givenTestSetupWithAclsForQuery();
    givenTestSetupWithConfig(ksqlConfig);

    // When:
    topicClient.deleteTopics(Collections.singleton(INPUT_TOPIC));
    assertThatEventually(
        "Wait for async topic deleting",
        () -> topicClient.isTopicExists(outputTopic),
        is(false)
    );

    // Then:
    assertQueryFailsWithUserError(
        String.format(
            "CREATE STREAM %s AS SELECT * FROM %s;",
            outputTopic,
            INPUT_STREAM
        ),
        String.format(
            "%s: One or more source topics were missing during rebalance",
            MissingSourceTopicException.class.getName()
        )
    );
  }

  @Test
  public void shouldClassifyMissingSourceTopicExceptionAsUserErrorSharedRuntimes() {
    // Given:
    final Map<String, Object> ksqlConfig = getKsqlConfig(NORMAL_USER);
    ksqlConfig.put(KSQL_SERVICE_ID_CONFIG, SERVICE_ID);
    ksqlConfig.put(KSQL_QUERY_RETRY_BACKOFF_INITIAL_MS, 0L);
    ksqlConfig.put(KSQL_QUERY_RETRY_BACKOFF_MAX_MS, 0L);
    ksqlConfig.put(KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED, true);


    givenTestSetupWithAclsForQuery();
    givenTestSetupWithConfig(ksqlConfig);

    // When:
    topicClient.deleteTopics(Collections.singleton(INPUT_TOPIC));
    assertThatEventually(
        "Wait for async topic deleting",
        () -> topicClient.isTopicExists(outputTopic),
        is(false)
    );

    // Then:
    assertQueryFailsWithUserError(
        String.format(
            "CREATE STREAM %s AS SELECT * FROM %s;",
            outputTopic,
            INPUT_STREAM
        ),
        String.format(
            "%s: Missing source topics",
            MissingSourceTopicException.class.getName()
        )
    );
  }

  @Test
  public void shouldClassifyTopicAuthorizationExceptionAsUserError() {
    // Given:
    final Map<String, Object> ksqlConfig = getKsqlConfig(NORMAL_USER);
    ksqlConfig.put(KSQL_SERVICE_ID_CONFIG, SERVICE_ID);
    ksqlConfig.put(KSQL_QUERY_RETRY_BACKOFF_INITIAL_MS, 0L);
    ksqlConfig.put(KSQL_QUERY_RETRY_BACKOFF_MAX_MS, 0L);

    givenTestSetupWithAclsForQuery();
    givenTestSetupWithConfig(ksqlConfig);

    // When:
    TEST_HARNESS.getKafkaCluster().addUserAcl(
        NORMAL_USER.username,
        AclPermissionType.DENY,
        resource(TOPIC, INPUT_TOPIC),
        ops(READ)
    );

    // Then:
    assertQueryFailsWithUserError(
        String.format(
            "CREATE STREAM %s AS SELECT * FROM %s;",
            outputTopic,
            INPUT_STREAM
        ),
        String.format(
            "%s: Not authorized to access topics: [%s]",
            TopicAuthorizationException.class.getName(),
            INPUT_TOPIC
        )
    );
  }

  @Test
  public void shouldClassifyGroupAuthorizationExceptionAsUserError() {
    // Given:
    final Map<String, Object> ksqlConfig = getKsqlConfig(NORMAL_USER);
    ksqlConfig.put(KSQL_SERVICE_ID_CONFIG, SERVICE_ID);
    ksqlConfig.put(KSQL_QUERY_RETRY_BACKOFF_INITIAL_MS, 0L);
    ksqlConfig.put(KSQL_QUERY_RETRY_BACKOFF_MAX_MS, 0L);

    givenTestSetupWithAclsForQuery();
    givenTestSetupWithConfig(ksqlConfig);

    TEST_HARNESS.getKafkaCluster().addUserAcl(
        NORMAL_USER.username,
        AclPermissionType.DENY,
        prefixedResource(GROUP, QUERY_ID_PREFIX),
        ops(ALL)
    );

    // Then:
    assertQueryFailsWithUserError(
        String.format(
            "CREATE STREAM %s AS SELECT * FROM %s;",
            outputTopic,
            INPUT_STREAM
        ),
        String.format(
            "%s: Not authorized to access group:",
            GroupAuthorizationException.class.getName(),
            QUERY_ID_PREFIX
        )
    );
  }

  @Test
  public void shouldClassifyTransactionIdAuthorizationExceptionAsUserError() {
    // Given:
    final Map<String, Object> ksqlConfig = getKsqlConfig(NORMAL_USER);
    ksqlConfig.put(KSQL_SERVICE_ID_CONFIG, SERVICE_ID);
    ksqlConfig.put(KSQL_QUERY_RETRY_BACKOFF_INITIAL_MS, 0L);
    ksqlConfig.put(KSQL_QUERY_RETRY_BACKOFF_MAX_MS, 0L);
    ksqlConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

    givenTestSetupWithAclsForQuery(); // does not authorize TX, but we enabled EOS above
    givenTestSetupWithConfig(ksqlConfig);

    // Then:
    assertQueryFailsWithUserError(
        String.format(
            "CREATE STREAM %s AS SELECT * FROM %s;",
            outputTopic,
            INPUT_STREAM
        ),
        String.format(
            "%s: Error encountered trying to initialize transactions [stream-thread [Time-limited test]]",
            StreamsException.class.getName()
        )
    );
  }

  // Requires correctly configured schema-registry running
  //@Test
  @SuppressWarnings("unused")
  public void shouldRunQueryAgainstSecureSchemaRegistry() {
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

  private void givenTestSetupWithAclsForQuery() {
      givenAllowAcl(NORMAL_USER,
          resource(TOPIC, INPUT_TOPIC),
          ops(READ));

      givenAllowAcl(NORMAL_USER,
          resource(TOPIC, outputTopic),
          ops(CREATE /* as the topic doesn't exist yet*/, WRITE));

      givenAllowAcl(NORMAL_USER,
          prefixedResource(TOPIC, QUERY_ID_PREFIX),
          ops(ALL));

      givenAllowAcl(NORMAL_USER,
          prefixedResource(GROUP, QUERY_ID_PREFIX),
          ops(ALL));
  }

  private static void givenAllowAcl(final Credentials credentials,
                                    final ResourcePattern resource,
                                    final Set<AclOperation> ops) {
    TEST_HARNESS.getKafkaCluster()
        .addUserAcl(credentials.username, AclPermissionType.ALLOW, resource, ops);
  }

  private void givenTestSetupWithConfig(final Map<String, Object> ksqlConfigs) {
    ksqlConfig = new KsqlConfig(ksqlConfigs);
    serviceContext = ServiceContextFactory.create(ksqlConfig, DisabledKsqlClient::instance);

    MutableFunctionRegistry functionRegistry = new InternalFunctionRegistry();
    loadAllUserFunctions(functionRegistry);

    ksqlEngine = new KsqlEngine(
        serviceContext,
        ProcessingLogContext.create(),
        functionRegistry,
        ServiceInfo.create(ksqlConfig),
        new SequentialQueryIdGenerator(),
        ksqlConfig,
        Collections.emptyList(),
        new MetricCollectors()
    );

    execInitCreateStreamQueries();
  }

  private void assertCanRunSimpleKsqlQuery() {
    assertCanRunKsqlQuery(
        "CREATE STREAM %s AS SELECT * FROM %s;",
        outputTopic,
        INPUT_STREAM
    );
  }

  private void assertCanRunRepartitioningKsqlQuery() {
    assertCanRunKsqlQuery(
        "CREATE TABLE %s AS SELECT itemid, count(*) FROM %s GROUP BY itemid;",
        outputTopic,
        INPUT_STREAM
    );
  }

  private void assertCanAccessClusterConfig() {
    // Creating topic with default replicas causes topic client to query cluster config to get
    // default replica count:
    serviceContext.getTopicClient()
        .createTopic(QUERY_ID_PREFIX + "-foo", 1, TopicProperties.DEFAULT_REPLICAS);
  }

  private void assertCanRunKsqlQuery(
      final String queryString,
      final Object... args
  ) {
    executePersistentQuery(queryString, args);

    assertThatEventually(
        "Wait for async topic creation",
        () -> topicClient.isTopicExists(outputTopic),
        is(true)
    );

    TEST_HARNESS.verifyAvailableRecords(outputTopic, greaterThan(0));
  }

  private void assertQueryFailsWithUserError(
      final String query,
      final String errorMsg
  ) {
    final QueryMetadata queryMetadata = KsqlEngineTestUtil
        .execute(serviceContext, ksqlEngine, query, ksqlConfig, Collections.emptyMap()).get(0);

    queryMetadata.start();
    assertThatEventually(
        "Wait for query to fail",
        () -> queryMetadata.getQueryErrors().size() > 0,
        is(true)
    );

    for (final QueryError error : queryMetadata.getQueryErrors()) {
        assertThat(error.getType(), is(Type.USER));
        assertThat(
            error.getErrorMessage().split("\n")[0],
            containsString(String.format(errorMsg, queryMetadata.getQueryId()))
        );
    }
  }

  private static Map<String, Object> getBaseKsqlConfig() {
    final Map<String, Object> configs = new HashMap<>(KsqlConfigTestUtil.baseTestConfig());
    configs.put(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        TEST_HARNESS.getKafkaCluster().bootstrapServers()
    );

    // Additional Properties required for KSQL to talk to test secure cluster,
    // where SSL cert not properly signed. (Not required for proper cluster).
    configs.putAll(ClientTrustStore.trustStoreProps());
    return configs;
  }

  private static Map<String, Object> getKsqlConfig(final Credentials user) {
    final Map<String, Object> configs = getBaseKsqlConfig();
    configs.putAll(SecureKafkaHelper.getSecureCredentialsConfig(user));
    return configs;
  }

  private void produceInitData() {
    if (topicClient.isTopicExists(INPUT_TOPIC)) {
      return;
    }

    topicClient.createTopic(INPUT_TOPIC, 1, (short) 1);

    awaitAsyncInputTopicCreation();

    final OrderDataProvider orderDataProvider = new OrderDataProvider();

    TEST_HARNESS.produceRows(INPUT_TOPIC, orderDataProvider, KAFKA, JSON);
  }

  private void awaitAsyncInputTopicCreation() {
    assertThatEventually(() -> topicClient.isTopicExists(INPUT_TOPIC), is(true));
  }

  private void execInitCreateStreamQueries() {
    final String ordersStreamStr =
        "CREATE STREAM " + INPUT_STREAM + " (ROWKEY STRING KEY, ORDERTIME bigint, ORDERID varchar, "
            + "ITEMID varchar, ORDERUNITS double, PRICEARRAY array<double>, KEYVALUEMAP "
            + "map<varchar, double>) WITH (value_format = 'json', "
            + "kafka_topic='" + INPUT_TOPIC + "');";

    KsqlEngineTestUtil.execute(
        serviceContext,
        ksqlEngine,
        ordersStreamStr,
        ksqlConfig,
        Collections.emptyMap()
    );
  }

  private void executePersistentQuery(final String queryString,
                                      final Object... params) {
    final String query = String.format(queryString, params);

    final QueryMetadata queryMetadata = KsqlEngineTestUtil
        .execute(serviceContext, ksqlEngine, query, ksqlConfig, Collections.emptyMap()).get(0);

    queryMetadata.start();
    queryId = queryMetadata.getQueryId();
  }
}
