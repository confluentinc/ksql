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

package io.confluent.ksql.services;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.schema.registry.KsqlSchemaRegistryClientFactory;
import io.confluent.ksql.security.KsqlPrincipal;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.network.ProxyProtocol;
import org.apache.kafka.common.network.ProxyProtocolCommand;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;

public final class ServiceContextFactory {

  private ServiceContextFactory() {
    super();
  }

  public static ServiceContext create(
      final KsqlConfig ksqlConfig,
      final Supplier<SimpleKsqlClient> ksqlClientSupplier
  ) {
    return create(
        ksqlConfig,
        new DefaultKafkaClientSupplier(),
        new KsqlSchemaRegistryClientFactory(
            ksqlConfig,
            Collections.emptyMap())::get,
        () -> new DefaultConnectClientFactory(ksqlConfig).get(
            Optional.empty(),
            Collections.emptyList(),
            Optional.empty()),
        ksqlClientSupplier,
        Optional.empty()
    );
  }

  public static ServiceContext create(
      final KsqlConfig ksqlConfig,
      final KafkaClientSupplier kafkaClientSupplier,
      final Supplier<SchemaRegistryClient> srClientFactory,
      final Supplier<ConnectClient> connectClientSupplier,
      final Supplier<SimpleKsqlClient> ksqlClientSupplier,
      final Optional<KsqlPrincipal> userPrincipal
  ) {
    final Supplier<Admin> adminClientSupplier;
    final KafkaClientSupplier finalKafkaClientSupplier;

    if (ksqlConfig.getBoolean(KsqlConfig.KSQL_CLIENT_IP_PORT_CONFIGURATION_ENABLED)
        && userPrincipal.isPresent()) {
      final KsqlPrincipal principal = userPrincipal.get();

      // Create new map to make it modifiable
      final Map<String, Object> adminConfig =
          new HashMap<>(ksqlConfig.getKsqlAdminClientConfigProps());
      applyAdminProxyProtocolConfigs(principal, adminConfig);
      adminClientSupplier = () -> kafkaClientSupplier.getAdmin(adminConfig);

      finalKafkaClientSupplier =
          new KafkaClientSupplierWithProxyConfigs(principal, kafkaClientSupplier);
    } else {
      finalKafkaClientSupplier = kafkaClientSupplier;

      final Map<String, Object> adminConfig =
          new HashMap<>(ksqlConfig.getKsqlAdminClientConfigProps());
      applyAdminProxyProtocolLocalConfigs(adminConfig);
      adminClientSupplier = () -> kafkaClientSupplier.getAdmin(adminConfig);
    }

    return new DefaultServiceContext(
        finalKafkaClientSupplier,
        adminClientSupplier,
        adminClientSupplier,
        srClientFactory,
        connectClientSupplier,
        ksqlClientSupplier
    );
  }

  private static void applyAdminProxyProtocolLocalConfigs(
      final Map<String, Object> topicAdminConfig) {
    // Set proxy protocol client mode to local for topicAdminClientSupplier
    // if user principal doesn't exist
    topicAdminConfig.put(AdminClientConfig.PROXY_PROTOCOL_CLIENT_MODE,
        ProxyProtocolCommand.LOCAL.name());
    topicAdminConfig.put(AdminClientConfig.PROXY_PROTOCOL_CLIENT_VERSION, ProxyProtocol.V2.name);
  }

  private static void applyAdminProxyProtocolConfigs(
      final KsqlPrincipal userPrincipal,
      final Map<String, Object> topicAdminConfig) {
    // Set Client address config for topicAdminClientSupplier if user principal exists
    topicAdminConfig.put(AdminClientConfig.PROXY_PROTOCOL_CLIENT_MODE,
        ProxyProtocolCommand.PROXY.name());
    topicAdminConfig.put(AdminClientConfig.PROXY_PROTOCOL_CLIENT_ADDRESS,
        userPrincipal.getIpAddress());
    topicAdminConfig.put(AdminClientConfig.PROXY_PROTOCOL_CLIENT_PORT,
        userPrincipal.getPort());
    topicAdminConfig.put(AdminClientConfig.PROXY_PROTOCOL_CLIENT_VERSION, ProxyProtocol.V2.name);
  }

  private static class KafkaClientSupplierWithProxyConfigs implements KafkaClientSupplier {

    private final KsqlPrincipal userPrincipal;
    private final KafkaClientSupplier kafkaClientSupplier;

    KafkaClientSupplierWithProxyConfigs(final KsqlPrincipal userPrincipal,
        final KafkaClientSupplier kafkaClientSupplier) {
      this.userPrincipal = userPrincipal;
      this.kafkaClientSupplier = kafkaClientSupplier;
    }

    @Override
    public Producer<byte[], byte[]> getProducer(final Map<String, Object> config) {
      final Map<String, Object> configsWithProxyProtocol =
          applyProducerProxyProtocolConfigs(config);
      return kafkaClientSupplier.getProducer(configsWithProxyProtocol);
    }

    @Override
    public Consumer<byte[], byte[]> getConsumer(final Map<String, Object> config) {
      final Map<String, Object> configsWithProxyProtocol =
          applyConsumerProxyProtocolConfigs(config);
      return kafkaClientSupplier.getConsumer(configsWithProxyProtocol);
    }

    @Override
    public Consumer<byte[], byte[]> getRestoreConsumer(final Map<String, Object> config) {
      final Map<String, Object> configsWithProxyProtocol =
          applyConsumerProxyProtocolConfigs(config);
      return kafkaClientSupplier.getRestoreConsumer(configsWithProxyProtocol);
    }

    @Override
    public Consumer<byte[], byte[]> getGlobalConsumer(final Map<String, Object> config) {
      final Map<String, Object> configsWithProxyProtocol =
          applyConsumerProxyProtocolConfigs(config);
      return kafkaClientSupplier.getGlobalConsumer(configsWithProxyProtocol);
    }

    private Map<String, Object> applyProducerProxyProtocolConfigs(
        final Map<String, Object> config) {
      final Map<String, Object> configsWithProxyProtocol = new HashMap<>(config);
      configsWithProxyProtocol.put(ProducerConfig.PROXY_PROTOCOL_CLIENT_MODE,
          ProxyProtocolCommand.PROXY.name());
      configsWithProxyProtocol.put(ProducerConfig.PROXY_PROTOCOL_CLIENT_ADDRESS,
          userPrincipal.getIpAddress());
      configsWithProxyProtocol.put(ProducerConfig.PROXY_PROTOCOL_CLIENT_PORT,
          userPrincipal.getPort());
      configsWithProxyProtocol.put(ProducerConfig.PROXY_PROTOCOL_CLIENT_VERSION,
          ProxyProtocol.V2.name);
      return configsWithProxyProtocol;
    }

    private Map<String, Object> applyConsumerProxyProtocolConfigs(
        final Map<String, Object> config) {
      final Map<String, Object> configsWithProxyProtocol = new HashMap<>(config);
      configsWithProxyProtocol.put(ConsumerConfig.PROXY_PROTOCOL_CLIENT_MODE,
          ProxyProtocolCommand.PROXY.name());
      configsWithProxyProtocol.put(ConsumerConfig.PROXY_PROTOCOL_CLIENT_ADDRESS,
          userPrincipal.getIpAddress());
      configsWithProxyProtocol.put(ConsumerConfig.PROXY_PROTOCOL_CLIENT_PORT,
          userPrincipal.getPort());
      configsWithProxyProtocol.put(ConsumerConfig.PROXY_PROTOCOL_CLIENT_VERSION,
          ProxyProtocol.V2.name);
      return configsWithProxyProtocol;
    }
  }
}
