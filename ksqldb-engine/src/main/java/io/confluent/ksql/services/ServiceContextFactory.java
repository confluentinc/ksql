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
import org.apache.kafka.common.network.ProxyProtocol;
import org.apache.kafka.common.network.ProxyProtocolCommand;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;

public final class ServiceContextFactory {

  private ServiceContextFactory() {

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
    final Supplier<Admin> topicAdminClientSupplier;
    if (ksqlConfig.getBoolean(KsqlConfig.KSQL_CLIENT_IP_PORT_CONFIGURATION_ENABLED)
        && userPrincipal.isPresent()) {
      // Create new map to make it modifiable
      final Map<String, Object> topicAdminConfig = new HashMap<>(
          ksqlConfig.getKsqlAdminClientConfigProps());

      // Set Client address config for topicAdminClientSupplier if user principal exists
      topicAdminConfig.put(AdminClientConfig.PROXY_PROTOCOL_CLIENT_MODE,
          ProxyProtocolCommand.PROXY.name());
      topicAdminConfig.put(AdminClientConfig.PROXY_PROTOCOL_CLIENT_ADDRESS,
          userPrincipal.get().getIpAddress());
      topicAdminConfig.put(AdminClientConfig.PROXY_PROTOCOL_CLIENT_PORT,
          userPrincipal.get().getPort());
      topicAdminConfig.put(AdminClientConfig.PROXY_PROTOCOL_CLIENT_VERSION, ProxyProtocol.V2.name);
      topicAdminClientSupplier = () -> kafkaClientSupplier.getAdmin(topicAdminConfig);
    } else {
      topicAdminClientSupplier = () -> kafkaClientSupplier.getAdmin(
          ksqlConfig.getKsqlAdminClientConfigProps());
    }

    return new DefaultServiceContext(
        kafkaClientSupplier,
        () -> kafkaClientSupplier
            .getAdmin(ksqlConfig.getKsqlAdminClientConfigProps()),
        topicAdminClientSupplier,
        srClientFactory,
        connectClientSupplier,
        ksqlClientSupplier
    );
  }
}
