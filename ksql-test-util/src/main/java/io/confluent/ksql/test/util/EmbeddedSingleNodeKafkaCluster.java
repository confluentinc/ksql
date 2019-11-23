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

package io.confluent.ksql.test.util;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.test.util.secure.ClientTrustStore;
import io.confluent.ksql.test.util.secure.Credentials;
import io.confluent.ksql.test.util.secure.SecureKafkaHelper;
import io.confluent.ksql.test.util.secure.ServerKeyStore;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.security.auth.login.Configuration;
import kafka.security.auth.Acl;
import kafka.security.auth.Operation$;
import kafka.security.auth.PermissionType;
import kafka.security.auth.PermissionType$;
import kafka.security.auth.ResourceType$;
import kafka.security.authorizer.AclAuthorizer;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.test.TestUtils;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

/**
 * Runs an in-memory, "embedded" Kafka cluster with 1 ZooKeeper instance and 1 Kafka broker.
 */
// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public final class EmbeddedSingleNodeKafkaCluster extends ExternalResource {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final Logger log = LoggerFactory.getLogger(EmbeddedSingleNodeKafkaCluster.class);

  public static final String JAAS_KAFKA_PROPS_NAME = "KafkaServer";

  public static final Credentials VALID_USER1 =
      new Credentials("valid_user_1", "some-password");
  public static final Credentials VALID_USER2 =
      new Credentials("valid_user_2", "some-password");
  private static final List<Credentials> ALL_VALID_USERS =
      ImmutableList.of(VALID_USER1, VALID_USER2);

  static final Duration ZK_SESSION_TIMEOUT = Duration.ofSeconds(30);
  // Jenkins builds can take ages to create the ZK log, so the initial connect can be slow, hence:
  static final Duration ZK_CONNECT_TIMEOUT = Duration.ofSeconds(60);

  private final String jassConfigFile;
  private final String previousJassConfig;
  private final Map<String, Object> customBrokerConfig;
  private final Map<String, Object> customClientConfig;
  private final TemporaryFolder tmpFolder = new TemporaryFolder();
  @SuppressWarnings("deprecation")
  private final kafka.security.auth.SimpleAclAuthorizer authorizer =
      new kafka.security.auth.SimpleAclAuthorizer();
  private final Set<kafka.security.auth.Resource> addedAcls = new HashSet<>();
  private final Map<AclKey, Set<AclOperation>> initialAcls;

  private ZooKeeperEmbedded zookeeper;
  private KafkaEmbedded broker;

  /**
   * Creates and starts a Kafka cluster.
   * @param customBrokerConfig Additional broker configuration settings.
   * @param customClientConfig Additional client configuration settings.
   * @param initialAcls a set of ACLs to set when the cluster starts.
   */
  private EmbeddedSingleNodeKafkaCluster(
      final Map<String, Object> customBrokerConfig,
      final Map<String, Object> customClientConfig,
      final String additionalJaasConfig,
      final Map<AclKey, Set<AclOperation>> initialAcls
  ) {
    this.customBrokerConfig = ImmutableMap
        .copyOf(requireNonNull(customBrokerConfig, "customBrokerConfig"));
    this.customClientConfig = ImmutableMap
        .copyOf(requireNonNull(customClientConfig, "customClientConfig"));
    this.initialAcls = ImmutableMap.copyOf(initialAcls);

    this.previousJassConfig = System.getProperty("java.security.auth.login.config");
    this.jassConfigFile = createServerJaasConfig(additionalJaasConfig);
  }

  /**
   * Creates and starts a Kafka cluster.
   */
  public void start() throws Exception {
    log.debug("Initiating embedded Kafka cluster startup");

    tmpFolder.create();

    installJaasConfig();
    zookeeper = new ZooKeeperEmbedded();
    broker = new KafkaEmbedded(buildBrokerConfig(tmpFolder.newFolder().getAbsolutePath()));
    final ImmutableMap<String, Object> props = ImmutableMap.of(
        KafkaConfig.ZkConnectProp(), zookeeperConnect(),
        AclAuthorizer.ZkConnectionTimeOutProp(), (int) ZK_CONNECT_TIMEOUT.toMillis(),
        AclAuthorizer.ZkSessionTimeOutProp(), (int) ZK_SESSION_TIMEOUT.toMillis()
    );
    authorizer.configure(props);

    initialAcls.forEach((key, ops) ->
        addUserAcl(key.userName, AclPermissionType.ALLOW, key.resourcePattern, ops));
  }

  @Override
  protected void before() throws Exception {
    start();
  }

  @Override
  protected void after() {
    stop();
  }

  /**
   * Stop the Kafka cluster.
   */
  public void stop() {
    if (broker != null) {
      broker.stop();
    }
    authorizer.close();
    try {
      if (zookeeper != null) {
        zookeeper.stop();
      }
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }

    resetJaasConfig();

    tmpFolder.delete();
  }

  /**
   * This cluster's `bootstrap.servers` value.  Example: `127.0.0.1:9092`.
   *
   * <p>You can use this to tell Kafka producers how to connect to this cluster.
   */
  public String bootstrapServers() {
    return broker.brokerList();
  }

  /**
   * This cluster's `bootstrap.servers` value.  Example: `127.0.0.1:9092`.
   *
   * <p>You can use this to tell Kafka producers how to connect to this cluster.
   * @param securityProtocol the security protocol to select.
   */
  public String bootstrapServers(final SecurityProtocol securityProtocol) {
    return broker.brokerList(securityProtocol);
  }

  /**
   * Common properties that clients will need to connect to the cluster.
   *
   * <p>This includes any SASL / SSL related settings.
   *
   * @return the properties that should be added to client props.
   */
  public Map<String, Object> getClientProperties() {
    final ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    builder.putAll(customClientConfig);
    builder.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    return builder.build();
  }

  /**
   * Common consumer properties that tests will need.
   *
   * @return base set of consumer properties.
   */
  public Map<String, Object> consumerConfig() {
    final Map<String, Object> config = new HashMap<>(getClientProperties());
    config.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Try to keep consumer groups stable:
    config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 7_000);
    config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 20_000);
    config.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, 3_000);
    return config;
  }

  /**
   * Common producer properties that tests will need.
   *
   * @return base set of producer properties.
   */
  public Map<String, Object> producerConfig() {
    final Map<String, Object> config = new HashMap<>(getClientProperties());
    config.put(ProducerConfig.ACKS_CONFIG, "all");
    config.put(ProducerConfig.RETRIES_CONFIG, 10);
    return config;
  }

  /**
   * This cluster's ZK connection string aka `zookeeper.connect` in `hostnameOrIp:port` format.
   * Example: `127.0.0.1:2181`.
   *
   * <p>You can use this to e.g. tell Kafka consumers how to connect to this cluster.
   */
  @SuppressWarnings("WeakerAccess") // Part of public API
  public String zookeeperConnect() {
    return zookeeper.connectString();
  }

  /**
   * Create a Kafka topic with 1 partition and a replication factor of 1.
   *
   * @param topic The name of the topic.
   */
  public void createTopic(final String topic) {
    broker.createTopic(topic, 1, 1);
  }

  /**
   * Create a Kafka topic with the given parameters.
   *
   * @param topic       The name of the topic.
   * @param partitions  The number of partitions for this topic.
   * @param replication The replication factor for (the partitions of) this topic.
   */
  public void createTopic(final String topic, final int partitions, final int replication) {
    broker.createTopic(topic, partitions, replication);
  }

  /**
   * Create a Kafka topic with the given parameters.
   *
   * @param topic       The name of the topic.
   * @param partitions  The number of partitions for this topic.
   * @param replication The replication factor for (partitions of) this topic.
   * @param topicConfig Additional topic-level configuration settings.
   */
  public void createTopic(
      final String topic,
                          final int partitions,
                          final int replication,
                          final Map<String, String> topicConfig) {
    broker.createTopic(topic, partitions, replication, topicConfig);
  }

  /**
   * Delete topics.
   * @param topics the topics to delete.
   */
  public void deleteTopics(final Collection<String> topics) {
    broker.deleteTopics(topics);
  }

  /**
   * Delete all topics in the cluster.
   * @param blacklist expect any in the blacklist
   */
  public void deleteAllTopics(final Collection<String> blacklist) {
    final Set<String> topics = broker.getTopics();
    topics.removeAll(blacklist);
    deleteTopics(topics);
  }

  public void deleteAllTopics(final String... blacklist) {
    deleteAllTopics(Arrays.asList(blacklist));
  }

  /**
   * Await the supplied {@code topicNames} to exist in the Cluster.
   *
   * @param topicNames the names of the topics
   * @throws AssertionError on timeout
   */
  public void waitForTopicsToBePresent(final String... topicNames) {
    broker.waitForTopicsToBePresent(topicNames);
  }

  /**
   * Await the supplied {@code topicNames} to not exist in the Cluster.
   *
   * @param topicNames the names of the topics
   * @throws AssertionError on timeout
   */
  public void waitForTopicsToBeAbsent(final String... topicNames) {
    broker.waitForTopicsToBeAbsent(topicNames);
  }

  /**
   * Verify there are {@code expectedCount} records available on the supplied {@code topic}.
   *
   * @param topic the name of the topic to check.
   * @param expectedCount the expected number of records.
   * @return the list of consumed records.
   */
  public List<ConsumerRecord<byte[], byte[]>> verifyAvailableRecords(
      final String topic,
      final int expectedCount
  ) {
    return verifyAvailableRecords(
        topic,
        expectedCount,
        new ByteArrayDeserializer(),
        new ByteArrayDeserializer()
    );
  }

  /**
   * Verify there are {@code expectedCount} records available on the supplied {@code topic}.
   *
   * @param topic the name of the topic to check.
   * @param expectedCount the expected number of records.
   * @return the list of consumed records.
   */
  public <K, V> List<ConsumerRecord<K, V>> verifyAvailableRecords(
      final String topic,
      final int expectedCount,
      final Deserializer<K> keyDeserializer,
      final Deserializer<V> valueDeserializer
  ) {
    try (KafkaConsumer<K, V> consumer = new KafkaConsumer<>(
        consumerConfig(),
        keyDeserializer,
        valueDeserializer)
    ) {
      consumer.subscribe(Collections.singleton(topic));

      return ConsumerTestUtil.verifyAvailableRecords(consumer, expectedCount);
    }
  }

  /**
   * Writes the supplied ACL information to ZK, where it will be picked up by the brokes authorizer.
   *
   * @param username    the who.
   * @param permission  the allow|deny.
   * @param resource    the thing
   * @param ops         the what.
   */
  public void addUserAcl(
      final String username,
      final AclPermissionType permission,
      final ResourcePattern resource,
      final Set<AclOperation> ops) {

    final KafkaPrincipal principal = new KafkaPrincipal("User", username);
    final PermissionType scalaPermission = PermissionType$.MODULE$.fromJava(permission);

    final Set<Acl> javaAcls = ops.stream()
        .map(Operation$.MODULE$::fromJava)
        .map(op -> new Acl(principal, scalaPermission, "*", op))
        .collect(Collectors.toSet());

    final scala.collection.immutable.Set<Acl> scalaAcls =
        JavaConverters.asScalaSet(javaAcls).toSet();

    final kafka.security.auth.ResourceType scalaResType =
        ResourceType$.MODULE$.fromJava(resource.resourceType());

    final kafka.security.auth.Resource scalaResource =
        new kafka.security.auth.Resource(scalaResType, resource.name(), resource.patternType());

    authorizer.addAcls(scalaAcls, scalaResource);

    addedAcls.add(scalaResource);
  }

  /**
   * Clear all ACLs from the cluster.
   */
  public void clearAcls() {
    addedAcls.forEach(authorizer::removeAcls);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static EmbeddedSingleNodeKafkaCluster build() {
    return newBuilder().build();
  }

  /**
   * Build config designed to keep the tests as stable as possible
   */
  @SuppressWarnings("deprecation")
  private Properties buildBrokerConfig(final String logDir) {
    final Properties config = new Properties();
    config.putAll(customBrokerConfig);
    // Only single node, so broker id always:
    config.put(KafkaConfig.BrokerIdProp(), 0);
    // Set the log dir for the node:
    config.put(KafkaConfig.LogDirProp(), logDir);
    // Need to know where ZK is:
    config.put(KafkaConfig.ZkConnectProp(), zookeeper.connectString());
    config.put(kafka.security.auth.SimpleAclAuthorizer.ZkUrlProp(), zookeeper.connectString());
    // Do not require tests to explicitly create tests:
    config.put(KafkaConfig.AutoCreateTopicsEnableProp(), true);
    // Default to small number of partitions for auto-created topics:
    config.put(KafkaConfig.NumPartitionsProp(), 1);
    // Allow tests to delete topics:
    config.put(KafkaConfig.DeleteTopicEnableProp(), true);
    // Do not clean logs from under the tests or waste resources doing so:
    config.put(KafkaConfig.LogCleanerEnableProp(), false);
    // Only single node, so only single RF on offset topic partitions:
    config.put(KafkaConfig.OffsetsTopicReplicationFactorProp(), (short) 1);
    // Tests do not need large numbers of offset topic partitions:
    config.put(KafkaConfig.OffsetsTopicPartitionsProp(), "1");
    // Shutdown quick:
    config.put(KafkaConfig.ControlledShutdownEnableProp(), false);
    // Set ZK connect timeout high enough to give ZK time to build log file on build server:
    config.put(KafkaConfig.ZkConnectionTimeoutMsProp(), (int) ZK_CONNECT_TIMEOUT.toMillis());
    // Set ZK session timeout high enough that slow build servers don't hit it:
    config.put(KafkaConfig.ZkSessionTimeoutMsProp(), (int) ZK_SESSION_TIMEOUT.toMillis());
    // Explicitly set to be less that the default 30 second timeout of KSQL functional tests
    config.put(KafkaConfig.ControllerSocketTimeoutMsProp(), 20_000);
    // Streams runs multiple consumers, so let's give them all a chance to join.
    // (Tests run quicker and with a more stable consumer group):
    config.put(KafkaConfig.GroupInitialRebalanceDelayMsProp(), 100);
    // Stop people writing silly data in tests:
    config.put(KafkaConfig.MessageMaxBytesProp(), 100_000);
    // Stop logs being deleted due to retention limits:
    config.put(KafkaConfig.LogRetentionTimeMillisProp(), -1);
    // Stop logs marked for deletion from being deleted
    config.put(KafkaConfig.LogDeleteDelayMsProp(), Long.MAX_VALUE);
    // Set to 1 because only 1 broker
    config.put(KafkaConfig.TransactionsTopicReplicationFactorProp(), (short) 1);
    // Set to 1 because only 1 broker
    config.put(KafkaConfig.TransactionsTopicMinISRProp(), 1);

    return config;
  }

  @SuppressWarnings("unused") // Part of Public API
  public String getJaasConfigPath() {
    return jassConfigFile;
  }

  private static String createServerJaasConfig(final String additionalJaasConfig) {
    try {
      final String jaasConfigContent = createJaasConfigContent() + additionalJaasConfig;
      final File jaasConfig = TestUtils.tempFile();
      Files.write(jaasConfig.toPath(), jaasConfigContent.getBytes(StandardCharsets.UTF_8));
      return jaasConfig.getAbsolutePath();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static String createJaasConfigContent() {
    final String prefix = JAAS_KAFKA_PROPS_NAME + " {\n  "
                          + PlainLoginModule.class.getName() + " required\n"
                          + "  username=\"broker\"\n"
                          + "  password=\"brokerPassword\"\n"
                          + "  user_broker=\"brokerPassword\"\n";

    return ALL_VALID_USERS.stream()
        .map(creds -> "  user_" + creds.username + "=\"" + creds.password + "\"")
        .collect(Collectors.joining("\n", prefix, ";\n};\n"));
  }

  private void installJaasConfig() {
    System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, jassConfigFile);
    System.setProperty(JaasUtils.ZK_SASL_CLIENT, "false");
    Configuration.setConfiguration(null);
  }

  private void resetJaasConfig() {
    if (previousJassConfig != null) {
      System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, previousJassConfig);
    } else {
      System.clearProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM);
    }
    Configuration.setConfiguration(null);
  }

  public static Set<AclOperation> ops(final AclOperation... ops) {
    return Arrays.stream(ops).collect(Collectors.toSet());
  }

  public static ResourcePattern resource(
      final ResourceType resourceType,
      final String resourceName
  ) {
    return new ResourcePattern(resourceType, resourceName, PatternType.LITERAL);
  }

  public static ResourcePattern prefixedResource(
      final ResourceType resourceType,
      final String resourceName
  ) {
    return new ResourcePattern(resourceType, resourceName, PatternType.PREFIXED);
  }

  public static final class Builder {

    private final Map<String, Object> brokerConfig = new HashMap<>();
    private final Map<String, Object> clientConfig = new HashMap<>();
    private final StringBuilder additionalJaasConfig = new StringBuilder();
    private final Map<AclKey, Set<AclOperation>> acls = new HashMap<>();

    @SuppressWarnings("deprecation")
    Builder() {
      brokerConfig.put(KafkaConfig.AuthorizerClassNameProp(),
          kafka.security.auth.SimpleAclAuthorizer.class.getName());
      brokerConfig.put(kafka.security.auth.SimpleAclAuthorizer.AllowEveryoneIfNoAclIsFoundProp(),
          true);
      brokerConfig.put(KafkaConfig.ListenersProp(), "PLAINTEXT://:0");
    }

    public Builder withoutPlainListeners() {
      removeListenersProp("PLAINTEXT");
      return this;
    }

    public Builder withSaslSslListeners() {
      addListenersProp("SASL_SSL");
      brokerConfig.put(KafkaConfig.SaslEnabledMechanismsProp(), "PLAIN");
      brokerConfig.put(KafkaConfig.InterBrokerSecurityProtocolProp(),
          SecurityProtocol.SASL_SSL.name());
      brokerConfig.put(KafkaConfig.SaslMechanismInterBrokerProtocolProp(), "PLAIN");
      brokerConfig.putAll(ServerKeyStore.keyStoreProps());
      brokerConfig.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");

      clientConfig.putAll(SecureKafkaHelper.getSecureCredentialsConfig(VALID_USER1));
      clientConfig.putAll(ClientTrustStore.trustStoreProps());
      clientConfig.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
      return this;
    }

    public Builder withSslListeners() {
      addListenersProp("SSL");
      return this;
    }

    @SuppressWarnings("deprecation")
    public Builder withAclsEnabled(final String... superUsers) {
      brokerConfig.remove(
          kafka.security.auth.SimpleAclAuthorizer.AllowEveryoneIfNoAclIsFoundProp());
      brokerConfig.put(kafka.security.auth.SimpleAclAuthorizer.SuperUsersProp(),
          Stream.concat(Arrays.stream(superUsers), Stream.of("broker"))
              .map(s -> "User:" + s)
              .collect(Collectors.joining(";")));
      return this;
    }

    public Builder withAcl(
        final Credentials credentials,
        final ResourcePattern resource,
        final Set<AclOperation> ops
    ) {
      acls.computeIfAbsent(AclKey.of(credentials.username, resource), k -> new HashSet<>())
          .addAll(ops);

      return this;
    }

    /**
     * Provide additional content to be included in the JVMs JAAS config file
     *
     * @param config the additional content
     * @return self.
     */
    @SuppressWarnings({"unused"}) // Part of Public API.
    public Builder withAdditionalJaasConfig(final String config) {
      additionalJaasConfig.append(config);
      return this;
    }

    public EmbeddedSingleNodeKafkaCluster build() {
      return new EmbeddedSingleNodeKafkaCluster(
          brokerConfig, clientConfig, additionalJaasConfig.toString(), acls);
    }

    private void addListenersProp(final String listenerType) {
      final Object current = brokerConfig.get(KafkaConfig.ListenersProp());
      brokerConfig.put(KafkaConfig.ListenersProp(), current + "," + listenerType + "://:0");
    }

    private void removeListenersProp(final String listenerType) {
      final String current = (String)brokerConfig.get(KafkaConfig.ListenersProp());
      final String replacement = Arrays.stream(current.split(","))
          .filter(part -> !part.startsWith(listenerType + "://"))
          .collect(Collectors.joining(","));
      brokerConfig.put(KafkaConfig.ListenersProp(), replacement);
    }
  }

  private static final class AclKey {

    private final String userName;
    private final ResourcePattern resourcePattern;

    AclKey(final String userName, final ResourcePattern resourcePattern) {
      this.userName = requireNonNull(userName, "userName");
      this.resourcePattern = requireNonNull(resourcePattern, "resourcePattern");
    }

    static AclKey of(final String userName, final ResourcePattern resourcePattern) {
      return new AclKey(userName, resourcePattern);
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final AclKey aclKey = (AclKey) o;
      return Objects.equals(userName, aclKey.userName)
          && Objects.equals(resourcePattern, aclKey.resourcePattern);
    }

    @Override
    public int hashCode() {
      return Objects.hash(userName, resourcePattern);
    }

    @Override
    public String toString() {
      return "AclKey{"
          + "userName='" + userName + '\''
          + ", resourcePattern=" + resourcePattern
          + '}';
    }
  }
}
