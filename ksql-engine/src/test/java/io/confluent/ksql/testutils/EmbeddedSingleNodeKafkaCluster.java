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

import com.google.common.io.Files;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.confluent.ksql.testutils.secure.ClientTrustStore;
import io.confluent.ksql.testutils.secure.Credentials;
import io.confluent.ksql.testutils.secure.SecureKafkaHelper;
import io.confluent.ksql.testutils.secure.ServerKeyStore;
import kafka.security.auth.SimpleAclAuthorizer;
import kafka.server.KafkaConfig;

import static org.apache.kafka.common.security.auth.SecurityProtocol.SASL_SSL;

/**
 * Runs an in-memory, "embedded" Kafka cluster with 1 ZooKeeper instance and 1 Kafka broker.
 */
public class EmbeddedSingleNodeKafkaCluster extends ExternalResource {

  private static final Logger log = LoggerFactory.getLogger(EmbeddedSingleNodeKafkaCluster.class);

  public static Credentials VALID_USER1 = new Credentials("valid_user_1", "some-password");

  private ZooKeeperEmbedded zookeeper;
  private KafkaEmbedded broker;
  private final Map<String, Object> brokerConfig = new HashMap<>();
  private final Map<String, Object> clientConfig = new HashMap<>();
  private final TemporaryFolder tmpFolder = new TemporaryFolder();

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
   * Common properties that clients will need to connect to the cluster.
   *
   * This includes any SASL / SSL related settings.
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

  public static Builder newBuilder() {
    return new Builder();
  }

  private Properties effectiveBrokerConfigFrom() {
    Properties effectiveConfig = new Properties();
    effectiveConfig.put(KafkaConfig.ListenersProp(), "PLAINTEXT://:0");
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
      final File jaasConfig = File.createTempFile("jaas_conf", null);
      jaasConfig.deleteOnExit();
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

    return Stream.of(VALID_USER1)
        .map(creds -> "  user_" + creds.username + "=\"" + creds.password + "\"")
        .collect(Collectors.joining("\n", prefix, ";\n};\n"));
  }

  public static final class Builder {
    private final Map<String, Object> brokerConfig = new HashMap<>();
    private final Map<String, Object> clientConfig = new HashMap<>();

    public Builder withSaslSslListenersOnly() {
      brokerConfig.put(KafkaConfig.ListenersProp(), "SASL_SSL://:0");
      brokerConfig.put(KafkaConfig.SaslEnabledMechanismsProp(), "PLAIN");
      brokerConfig.put(KafkaConfig.InterBrokerSecurityProtocolProp(), SASL_SSL.name());
      brokerConfig.put(KafkaConfig.SaslMechanismInterBrokerProtocolProp(), "PLAIN");
      brokerConfig.put(KafkaConfig.AuthorizerClassNameProp(), SimpleAclAuthorizer.class.getName());
      brokerConfig.put(SimpleAclAuthorizer.AllowEveryoneIfNoAclIsFoundProp(), true);
      brokerConfig.putAll(ServerKeyStore.keyStoreProps());

      clientConfig.putAll(SecureKafkaHelper.getSecureCredentialsConfig(VALID_USER1));
      clientConfig.putAll(ClientTrustStore.trustStoreProps());
      return this;
    }

    public EmbeddedSingleNodeKafkaCluster build() {
      return new EmbeddedSingleNodeKafkaCluster(brokerConfig, clientConfig);
    }
  }
}