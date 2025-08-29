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
import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import javax.net.ssl.SSLContext;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.security.auth.SslEngineFactory;
import org.apache.kafka.common.security.ssl.DefaultSslEngineFactory;
import org.apache.kafka.common.utils.SecurityUtils;

/**
 * Configurable Schema Registry client factory, enabling SSL.
 */
public class KsqlSchemaRegistryClientFactory {

  private final SSLContext sslContext;
  private final Supplier<RestService> serviceSupplier;
  private final Map<String, Object> schemaRegistryClientConfigs;
  private final SchemaRegistryClientFactory schemaRegistryClientFactory;
  private final Map<String, String> httpHeaders;
  private final String schemaRegistryUrl;

  interface SchemaRegistryClientFactory {
    CachedSchemaRegistryClient create(RestService service,
                                      int identityMapCapacity,
                                      List<SchemaProvider> providers,
                                      Map<String, Object> clientConfigs,
                                      Map<String, String> httpHeaders);
  }
  
  public KsqlSchemaRegistryClientFactory(
      final KsqlConfig config,
      final Map<String, String> schemaRegistryHttpHeaders
  ) {
    this(config, newSslContext(config), schemaRegistryHttpHeaders);
  }

  public KsqlSchemaRegistryClientFactory(
      final KsqlConfig config,
      final SSLContext sslContext,
      final Map<String, String> schemaRegistryHttpHeaders
  ) {
    this(config,
        () -> new RestService(config.getString(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY)),
        sslContext,
        CachedSchemaRegistryClient::new,
        schemaRegistryHttpHeaders
    );

    // Force config exception now:
    config.getString(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY);
  }

  @VisibleForTesting
  KsqlSchemaRegistryClientFactory(final KsqlConfig config,
                                  final Supplier<RestService> serviceSupplier,
                                  final SSLContext sslContext,
                                  final SchemaRegistryClientFactory schemaRegistryClientFactory,
                                  final Map<String, String> httpHeaders) {
    this.sslContext = sslContext;
    this.serviceSupplier = serviceSupplier;
    this.schemaRegistryClientConfigs = config.originalsWithPrefix(
        KsqlConfig.KSQL_SCHEMA_REGISTRY_PREFIX);

    this.schemaRegistryClientFactory = schemaRegistryClientFactory;
    this.httpHeaders = httpHeaders;
    this.schemaRegistryUrl = config.getString(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY).trim();
  }

  /**
   * Creates an SslContext configured to be used with the KsqlSchemaRegistryClient.
   */
  public static SSLContext newSslContext(final KsqlConfig config) {
    if (config.getBoolean(ConfluentConfigs.ENABLE_FIPS_CONFIG)) {
      SecurityUtils.addConfiguredSecurityProviders(config.originals());
    }
    final DefaultSslEngineFactory sslFactory = new DefaultSslEngineFactory();
    configureSslEngineFactory(config, sslFactory);
    return sslFactory.sslContext();
  }

  @VisibleForTesting
  static void configureSslEngineFactory(
      final KsqlConfig config,
      final SslEngineFactory sslFactory
  ) {
    sslFactory
        .configure(config.valuesWithPrefixOverride(KsqlConfig.KSQL_SCHEMA_REGISTRY_PREFIX));
  }

  public SchemaRegistryClient get() {
    if (schemaRegistryUrl.equals("")) {
      return new DefaultSchemaRegistryClient();
    }
  
    final RestService restService = serviceSupplier.get();
    // This call sets a default sslSocketFactory.
    final SchemaRegistryClient client = schemaRegistryClientFactory.create(
        restService,
        1000,
        ImmutableList.of(
            new AvroSchemaProvider(), new ProtobufSchemaProvider(), new JsonSchemaProvider()),
        schemaRegistryClientConfigs,
        httpHeaders
    );

    // If we have an sslContext, we use it to set the sslSocketFactory on the restService client.
    //  We need to do it in this order so that the override here is not reset by the constructor
    //  above.
    if (sslContext != null) {
      restService.setSslSocketFactory(sslContext.getSocketFactory());
    }
    return client;
  }
}
