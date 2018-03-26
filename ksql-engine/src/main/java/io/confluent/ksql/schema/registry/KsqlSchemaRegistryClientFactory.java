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

import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.security.ssl.SslFactory;

import java.util.function.Supplier;

import javax.net.ssl.SSLContext;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.ksql.util.KsqlConfig;

/**
 * Configurable Schema Registry client factory, enabling SSL.
 */
public class KsqlSchemaRegistryClientFactory {

  private final SslFactory sslFactory;
  private final Supplier<RestService> serviceSupplier;

  public KsqlSchemaRegistryClientFactory(final KsqlConfig config) {
    this(config, () -> new RestService(config.getString(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY)),
         new SslFactory(Mode.CLIENT));

    // Force config exception now:
    config.getString(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY);
  }

  KsqlSchemaRegistryClientFactory(final KsqlConfig config,
                                  final Supplier<RestService> serviceSupplier,
                                  final SslFactory sslFactory) {
    this.sslFactory = sslFactory;
    this.serviceSupplier = serviceSupplier;

    this.sslFactory
        .configure(config.valuesWithPrefixOverride(KsqlConfig.KSQL_SCHEMA_REGISTRY_PREFIX));
  }

  public SchemaRegistryClient create() {
    final RestService restService = serviceSupplier.get();
    final SSLContext sslContext = sslFactory.sslContext();
    if (sslContext != null) {
      restService.setSslSocketFactory(sslContext.getSocketFactory());
    }

    return new CachedSchemaRegistryClient(restService, 1000);
  }
}
