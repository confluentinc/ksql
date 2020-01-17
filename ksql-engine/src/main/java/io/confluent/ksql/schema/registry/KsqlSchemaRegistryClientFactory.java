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

package io.confluent.ksql.schema.registry;

import com.google.common.annotations.VisibleForTesting;
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
public class KsqlSchemaRegistryClientFactory {

  private final SslFactory sslFactory;
  private final Supplier<RestService> serviceSupplier;
  private final Map<String, Object> schemaRegistryClientConfigs;
  private final SchemaRegistryClientFactory schemaRegistryClientFactory;
  private final Map<String, String> httpHeaders;
  private final String schemaRegistryUrl;

  interface SchemaRegistryClientFactory {
    CachedSchemaRegistryClient create(RestService service,
                                      int identityMapCapacity,
                                      Map<String, Object> clientConfigs,
                                      Map<String, String> httpHeaders);
  }
  
  public KsqlSchemaRegistryClientFactory(
      final KsqlConfig config,
      final Map<String, String> schemaRegistryHttpHeaders
  ) {
    this(config, newSchemaRegistrySslFactory(config), schemaRegistryHttpHeaders);
  }

  public KsqlSchemaRegistryClientFactory(
      final KsqlConfig config,
      final SslFactory sslFactory,
      final Map<String, String> schemaRegistryHttpHeaders
  ) {
    this(config,
        () -> new RestService(config.getString(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY)),
        sslFactory,
        CachedSchemaRegistryClient::new,
        schemaRegistryHttpHeaders
    );

    // Force config exception now:
    config.getString(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY);
  }

  @VisibleForTesting
  KsqlSchemaRegistryClientFactory(final KsqlConfig config,
                                  final Supplier<RestService> serviceSupplier,
                                  final SslFactory sslFactory,
                                  final SchemaRegistryClientFactory schemaRegistryClientFactory,
                                  final Map<String, String> httpHeaders) {
    this.sslFactory = sslFactory;
    this.serviceSupplier = serviceSupplier;
    this.schemaRegistryClientConfigs = config.originalsWithPrefix(
        KsqlConfig.KSQL_SCHEMA_REGISTRY_PREFIX);

    this.schemaRegistryClientFactory = schemaRegistryClientFactory;
    this.httpHeaders = httpHeaders;
    this.schemaRegistryUrl = config.getString(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY).trim();
  }

  /**
   * Creates an SslFactory configured to be used with the KsqlSchemaRegistryClient.
   */
  public static SslFactory newSchemaRegistrySslFactory(final KsqlConfig config) {
    final SslFactory sslFactory = new SslFactory(Mode.CLIENT);
    configureSslFactory(config, sslFactory);
    return sslFactory;
  }

  @VisibleForTesting
  static void configureSslFactory(final KsqlConfig config, final SslFactory sslFactory) {
    sslFactory
        .configure(config.valuesWithPrefixOverride(KsqlConfig.KSQL_SCHEMA_REGISTRY_PREFIX));
  }

  public SchemaRegistryClient get() {
    if (schemaRegistryUrl.equals("")) {
      return new DefaultSchemaRegistryClient();
    }
  
    final RestService restService = serviceSupplier.get();
    final SSLContext sslContext = sslFactory.sslEngineBuilder().sslContext();
    if (sslContext != null) {
      restService.setSslSocketFactory(sslContext.getSocketFactory());
    }

    return schemaRegistryClientFactory.create(
        restService,
        1000,
        schemaRegistryClientConfigs,
        httpHeaders
    );
  }
}
