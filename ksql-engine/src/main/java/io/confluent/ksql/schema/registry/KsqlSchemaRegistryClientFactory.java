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

package io.confluent.ksql.schema.registry;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
import java.util.function.Supplier;
import javax.net.ssl.SSLContext;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.security.ssl.SslFactory;

/**
 * Configurable Schema Registry client factory, enabling SSL.
 */
public class KsqlSchemaRegistryClientFactory implements Supplier<SchemaRegistryClient> {

  private final SslFactory sslFactory;
  private final Supplier<RestService> serviceSupplier;
  private final Map<String, Object> schemaRegistryClientConfigs;
  private final SchemaRegistryClientFactory schemaRegistryClientFactory;

  interface SchemaRegistryClientFactory {
    CachedSchemaRegistryClient create(RestService service,
                                      int identityMapCapacity,
                                      Map<String, Object> clientConfigs);
  }


  public KsqlSchemaRegistryClientFactory(final KsqlConfig config) {
    this(config,
        () -> new RestService(config.getString(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY)),
        new SslFactory(Mode.CLIENT),
        CachedSchemaRegistryClient::new
    );

    // Force config exception now:
    config.getString(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY);
  }

  KsqlSchemaRegistryClientFactory(final KsqlConfig config,
                                  final Supplier<RestService> serviceSupplier,
                                  final SslFactory sslFactory,
                                  final SchemaRegistryClientFactory schemaRegistryClientFactory) {
    this.sslFactory = sslFactory;
    this.serviceSupplier = serviceSupplier;
    this.schemaRegistryClientConfigs = config.originalsWithPrefix(
        KsqlConfig.KSQL_SCHEMA_REGISTRY_PREFIX);

    this.sslFactory
        .configure(config.valuesWithPrefixOverride(KsqlConfig.KSQL_SCHEMA_REGISTRY_PREFIX));

    this.schemaRegistryClientFactory = schemaRegistryClientFactory;
  }

  public SchemaRegistryClient get() {
    final RestService restService = serviceSupplier.get();
    final SSLContext sslContext = sslFactory.sslContext();
    if (sslContext != null) {
      restService.setSslSocketFactory(sslContext.getSocketFactory());
    }

    return schemaRegistryClientFactory.create(restService, 1000, schemaRegistryClientConfigs);
  }
}
