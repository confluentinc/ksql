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

package io.confluent.ksql.testutils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.test.TestUtils;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.confluent.ksql.testutils.secure.ClientTrustStore;
import io.confluent.ksql.testutils.secure.Credentials;
import io.confluent.ksql.testutils.secure.SecureKafkaHelper;
import io.confluent.ksql.testutils.secure.ServerKeyStore;
import kafka.security.auth.Acl;
import kafka.security.auth.Operation$;
import kafka.security.auth.PermissionType;
import kafka.security.auth.PermissionType$;
import kafka.security.auth.ResourceType$;
import kafka.security.auth.SimpleAclAuthorizer;
import kafka.server.KafkaConfig;
import kafka.utils.ZKConfig;
import scala.collection.JavaConversions;

import static org.apache.kafka.common.security.auth.SecurityProtocol.SASL_SSL;

/**
 * Runs an in-memory, "embedded" Kafka cluster with 1 ZooKeeper instance and 1 Kafka broker.
 */
public class EmbeddedSingleNodeKafkaCluster extends ExternalResource {

  private static final Logger log = LoggerFactory.getLogger(EmbeddedSingleNodeKafkaCluster.class);

  public static Credentials VALID_USER1 = new Credentials("valid_user_1", "some-password");
  public static Credentials VALID_USER2 = new Credentials("valid_user_2", "some-password");
  private static List<Credentials> ALL_VALID_USERS = ImmutableList.of(VALID_USER1, VALID_USER2);

  private ZooKeeperEmbedded zookeeper;
  private KafkaEmbedded broker;
  private final Map<String, Object> brokerConfig = new HashMap<>();
  private final Map<String, Object> clientConfig = new HashMap<>();
  private final TemporaryFolder tmpFolder = new TemporaryFolder();
  private final SimpleAclAuthorizer authorizer = new SimpleAclAuthorizer();
  private final Set<kafka.security.auth.Resource> addedAcls = new HashSet<>();

  static {
    createServerJaasConfig();
  }

  /**
   * Creates and starts a Kafka cluster.
   */
  public EmbeddedSingleNodeKafkaCluster() {
    this(Collections.emptyMap(), Collections.emptyMap());
  }

  /**
   * Creates and starts a Kafka cluster.
   *
   * @param brokerConfig Additional broker configuration settings.
   * @param clientConfig Additional client configuration settings.
   */
  public EmbeddedSingleNodeKafkaCluster(final Map<String, Object> brokerConfig,
                                        final Map<String, Object> clientConfig) {
    this.brokerConfig.putAll(brokerConfig);
    this.clientConfig.putAll(clientConfig);
  }

  /**
   * Creates and starts a Kafka cluster.
   */
  public void start() throws Exception {
    log.debug("Initiating embedded Kafka cluster startup");

    zookeeper = new ZooKeeperEmbedded();
    broker = new KafkaEmbedded(effectiveBrokerConfigFrom());
    clientConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    brokerConfig.put(SimpleAclAuthorizer.ZkUrlProp(), zookeeper.connectString());
    authorizer.configure(ImmutableMap.of(ZKConfig.ZkConnectProp(), zookeeperConnect()));
  }

  @Override
  protected void before() throws Exception {
    tmpFolder.create();
    start();
  }

  @Override
  protected void after() {
    stop();
    tmpFolder.delete();
  }

  /**
   * Stop the Kafka cluster.
   */
  public void stop() {
    if (broker != null) {
      broker.stop();
    }
    if (authorizer != null) {
      authorizer.close();
    }
    try {
      if (zookeeper != null) {
        zookeeper.stop();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * This cluster's `bootstrap.servers` value.  Example: `127.0.0.1:9092`.
   *
   * You can use this to tell Kafka producers how to connect to this cluster.
   */
  public String bootstrapServers() {
    return broker.brokerList();
  }

  /**
   * This cluster's `bootstrap.servers` value.  Example: `127.0.0.1:9092`.
   *
   * You can use this to tell Kafka producers how to connect to this cluster.
   * @param securityProtocol the security protocol to select.
   */
  public String bootstrapServers(final SecurityProtocol securityProtocol) {
    return broker.brokerList(securityProtocol);
  }

  /**
   * Common properties that clients will need to connect to the cluster.
   *
   * This includes any SASL / SSL related settings.
   *
   * @return the properties that should be added to client props.
   */
  public Map<String, Object> getClientProperties() {
    return Collections.unmodifiableMap(clientConfig);
  }

  /**
   * This cluster's ZK connection string aka `zookeeper.connect` in `hostnameOrIp:port` format.
   * Example: `127.0.0.1:2181`.
   *
   * You can use this to e.g. tell Kafka consumers how to connect to this cluster.
   */
  public String zookeeperConnect() {
    return zookeeper.connectString();
  }

  /**
   * Create a Kafka topic with 1 partition and a replication factor of 1.
   *
   * @param topic The name of the topic.
   */
  public void createTopic(String topic) {
    createTopic(topic, 1, 1, new Properties());
  }

  /**
   * Create a Kafka topic with the given parameters.
   *
   * @param topic       The name of the topic.
   * @param partitions  The number of partitions for this topic.
   * @param replication The replication factor for (the partitions of) this topic.
   */
  public void createTopic(String topic, int partitions, int replication) {
    createTopic(topic, partitions, replication, new Properties());
  }

  /**
   * Create a Kafka topic with the given parameters.
   *
   * @param topic       The name of the topic.
   * @param partitions  The number of partitions for this topic.
   * @param replication The replication factor for (partitions of) this topic.
   * @param topicConfig Additional topic-level configuration settings.
   */
  public void createTopic(String topic,
                          int partitions,
                          int replication,
                          Properties topicConfig) {
    broker.createTopic(topic, partitions, replication, topicConfig);
  }

  /**
   * Writes the supplied ACL information to ZK, where it will be picked up by the brokes authorizer.
   *
   * @param username    the who.
   * @param permission  the allow|deny.
   * @param resource    the thing
   * @param ops         the what.
   */
  public void addUserAcl(final String username,
                         final AclPermissionType permission,
                         final Resource resource,
                         final Set<AclOperation> ops) {

    final KafkaPrincipal principal = new KafkaPrincipal("User", username);
    final PermissionType scalaPermission = PermissionType$.MODULE$.fromJava(permission);

    final Set<Acl> javaAcls = ops.stream()
        .map(Operation$.MODULE$::fromJava)
        .map(op -> new Acl(principal, scalaPermission, "*", op))
        .collect(Collectors.toSet());

    final scala.collection.immutable.Set<Acl> scalaAcls =
        JavaConversions.asScalaSet(javaAcls).toSet();

    kafka.security.auth.ResourceType scalaResType =
        ResourceType$.MODULE$.fromJava(resource.resourceType());

    final kafka.security.auth.Resource scalaResource =
        new kafka.security.auth.Resource(scalaResType, resource.name());

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

  private Properties effectiveBrokerConfigFrom() {
    Properties effectiveConfig = new Properties();
    effectiveConfig.putAll(brokerConfig);
    effectiveConfig.put(KafkaConfig.ZkConnectProp(), zookeeper.connectString());
    effectiveConfig.put(KafkaConfig.DeleteTopicEnableProp(), true);
    effectiveConfig.put(KafkaConfig.LogCleanerDedupeBufferSizeProp(), 2 * 1024 * 1024L);
    effectiveConfig.put(KafkaConfig.OffsetsTopicReplicationFactorProp(), (short) 1);
    return effectiveConfig;
  }

  private static void createServerJaasConfig() {
    try {
      final String jaasConfigContent = createJaasConfigContent();
      final File jaasConfig = TestUtils.tempFile();
      Files.write(jaasConfigContent, jaasConfig, StandardCharsets.UTF_8);

      System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, jaasConfig.getAbsolutePath());
      System.setProperty(JaasUtils.ZK_SASL_CLIENT, "false");
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static String createJaasConfigContent() {
    final String prefix = "KafkaServer {\n  " + PlainLoginModule.class.getName() + " required\n"
                          + "  username=\"broker\"\n"
                          + "  password=\"brokerPassword\"\n"
                          + "  user_broker=\"brokerPassword\"\n";

    return ALL_VALID_USERS.stream()
        .map(creds -> "  user_" + creds.username + "=\"" + creds.password + "\"")
        .collect(Collectors.joining("\n", prefix, ";\n};\n"));
  }

  public static final class Builder {

    private final Map<String, Object> brokerConfig = new HashMap<>();
    private final Map<String, Object> clientConfig = new HashMap<>();

    public Builder() {
      brokerConfig.put(KafkaConfig.AuthorizerClassNameProp(), SimpleAclAuthorizer.class.getName());
      brokerConfig.put(SimpleAclAuthorizer.AllowEveryoneIfNoAclIsFoundProp(), true);
      brokerConfig.put(KafkaConfig.ListenersProp(), "PLAINTEXT://:0");
    }

    public Builder withoutPlainListeners() {
      removeListenersProp("PLAINTEXT");
      return this;
    }

    public Builder withSaslSslListeners() {
      addListenersProp("SASL_SSL");
      brokerConfig.put(KafkaConfig.SaslEnabledMechanismsProp(), "PLAIN");
      brokerConfig.put(KafkaConfig.InterBrokerSecurityProtocolProp(), SASL_SSL.name());
      brokerConfig.put(KafkaConfig.SaslMechanismInterBrokerProtocolProp(), "PLAIN");
      brokerConfig.putAll(ServerKeyStore.keyStoreProps());

      clientConfig.putAll(SecureKafkaHelper.getSecureCredentialsConfig(VALID_USER1));
      clientConfig.putAll(ClientTrustStore.trustStoreProps());
      return this;
    }

    public Builder withSslListeners() {
      addListenersProp("SSL");
      return this;
    }

    public Builder withAcls(final String... superUsers) {
      brokerConfig.remove(SimpleAclAuthorizer.AllowEveryoneIfNoAclIsFoundProp());
      brokerConfig.put(SimpleAclAuthorizer.SuperUsersProp(),
                       Stream.concat(Arrays.stream(superUsers), Stream.of("broker"))
                           .map(s -> "User:" + s)
                           .collect(Collectors.joining(";")));
      return this;
    }

    public EmbeddedSingleNodeKafkaCluster build() {
      return new EmbeddedSingleNodeKafkaCluster(brokerConfig, clientConfig);
    }

    private void addListenersProp(String listenerType) {
      final Object current = brokerConfig.get(KafkaConfig.ListenersProp());
      brokerConfig.put(KafkaConfig.ListenersProp(), current + "," + listenerType + "://:0");
    }

    private void removeListenersProp(String listenerType) {
      final String current = (String)brokerConfig.get(KafkaConfig.ListenersProp());
      final String replacement = Arrays.stream(current.split(","))
          .filter(part -> !part.startsWith(listenerType + "://"))
          .collect(Collectors.joining(","));
      brokerConfig.put(KafkaConfig.ListenersProp(), replacement);
    }
  }
}